use bytes::{Buf, IntoBuf};
use futures::{Async, Future, Poll};
use h2;
use http;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_timer::clock;
use tower_h2::Body;

use super::{event, NextId, Taps};
use proxy::{self, http::client::Error};
use svc;

/// A stack module that wraps services to record taps.
#[derive(Clone, Debug)]
pub struct Layer<T, M> {
    next_id: NextId,
    taps: Arc<Mutex<Taps>>,
    _p: PhantomData<fn() -> (T, M)>,
}

/// Wraps services to record taps.
#[derive(Clone, Debug)]
pub struct Make<T, N>
where
    N: svc::Make<T>,
{
    next_id: NextId,
    taps: Arc<Mutex<Taps>>,
    inner: N,
    _p: PhantomData<fn() -> (T)>,
}

/// A middleware that records HTTP taps.
#[derive(Clone, Debug)]
pub struct Service<S>
where
    S: svc::Service,
{
    endpoint: event::Endpoint,
    next_id: NextId,
    taps: Arc<Mutex<Taps>>,
    inner: S,
}

#[derive(Debug, Clone)]
pub struct ResponseFuture<S>
where
    S: svc::Service,
{
    handle: Option<Handle<event::Request>>,
    inner: S::Future,
}

#[derive(Debug, Clone)]
struct Handle<M> {
    meta: M,
    taps: Option<Arc<Mutex<Taps>>>,
    request_open_at: Instant,
}

#[derive(Debug, Default)]
pub struct RequestBody<B> {
    inner: B,
    handle: Option<Handle<event::Request>>,
    byte_count: usize,
    frame_count: usize,
}

#[derive(Debug)]
pub struct ResponseBody<B> {
    inner: B,
    handle: Option<Handle<event::Response>>,
    response_open_at: Instant,
    response_first_frame_at: Option<Instant>,
    byte_count: usize,
    frame_count: usize,
}

// === Layer ===

impl<T, M, A, B> Layer<T, M>
where
    T: Clone + Into<event::Endpoint>,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A>>,
        Response = http::Response<B>,
        Error = Error,
    >,
    A: Body,
    B: Body,
{
    pub fn new(next_id: NextId, taps: Arc<Mutex<Taps>>) -> Self {
        Self {
            next_id,
            taps,
            _p: PhantomData,
        }
    }
}

impl<T, M, A, B> svc::Layer<T, T, M> for Layer<T, M>
where
    T: Clone + Into<event::Endpoint>,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A>>,
        Response = http::Response<B>,
        Error = Error,
    >,
    A: Body,
    B: Body,
{
    type Value = <Make<T, M> as svc::Make<T>>::Value;
    type Error = M::Error;
    type Make = Make<T, M>;

    fn bind(&self, inner: M) -> Self::Make {
        Make {
            next_id: self.next_id.clone(),
            taps: self.taps.clone(),
            inner,
            _p: PhantomData,
        }
    }
}

// === Make ===

impl<T, M, A, B> svc::Make<T> for Make<T, M>
where
    T: Clone + Into<event::Endpoint>,
    M: svc::Make<T>,
    M::Value: svc::Service<
        Request = http::Request<RequestBody<A>>,
        Response = http::Response<B>,
        Error = Error,
    >,
    A: Body,
    B: Body,
{
    type Value = Service<M::Value>;
    type Error = M::Error;

    fn make(&self, target: &T) -> Result<Self::Value, Self::Error> {
        let inner = self.inner.make(&target)?;
        Ok(Service {
            next_id: self.next_id.clone(),
            endpoint: target.clone().into(),
            taps: self.taps.clone(),
            inner,
        })
    }
}

// === Service ===

impl<S, A, B> svc::Service for Service<S>
where
    S: svc::Service<
        Request = http::Request<RequestBody<A>>,
        Response = http::Response<B>,
        Error = Error,
    >,
    A: Body,
    B: Body,
{
    type Request = http::Request<A>;
    type Response = http::Response<ResponseBody<B>>;
    type Error = S::Error;
    type Future = ResponseFuture<S>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.inner.poll_ready()
    }

    fn call(&mut self, req: Self::Request) -> Self::Future {
        let request_open_at = clock::now();

        let meta = req
            .extensions()
            .get::<proxy::Source>()
            .map(|source| event::Request {
                id: self.next_id.next_id(),
                endpoint: self.endpoint.clone(),
                source: source.clone(),
                method: req.method().clone(),
                uri: req.uri().clone(),
            });

        if let Some(meta) = meta.as_ref() {
            if let Ok(mut taps) = self.taps.lock() {
                taps.inspect(&event::Event::StreamRequestOpen(meta.clone()));
            }
        }

        let handle = meta.clone().map(|meta| Handle {
            meta,
            taps: Some(self.taps.clone()),
            request_open_at,
        });

        let req = {
            let handle = handle.clone();
            let (head, inner) = req.into_parts();
            let mut body = RequestBody {
                inner,
                handle,
                byte_count: 0,
                frame_count: 0,
            };

            if body.is_end_stream() {
                body.tap_eos(Some(&head.headers));
            }

            http::Request::from_parts(head, body)
        };

        ResponseFuture {
            inner: self.inner.call(req),
            handle,
        }
    }
}

impl<S, B> Future for ResponseFuture<S>
where
    B: Body,
    S: svc::Service<Response = http::Response<B>, Error = Error>,
{
    type Item = http::Response<ResponseBody<B>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let rsp = match self.inner.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(rsp)) => rsp,
            Err(e) => {
                if let Some(mut h) = self.handle.take() {
                    h.tap_err(e.reason());
                }
                return Err(e);
            }
        };
        let response_open_at = clock::now();

        let handle = self.handle.take().map(|h| Handle {
            meta: event::Response {
                request: h.meta.clone(),
                status: rsp.status(),
            },
            taps: h.taps.clone(),
            request_open_at: h.request_open_at,
        });

        if let Some(handle) = handle.as_ref() {
            if let Some(taps) = handle.taps.as_ref() {
                if let Ok(mut taps) = taps.lock() {
                    taps.inspect(&event::Event::StreamResponseOpen(
                        handle.meta.clone(),
                        event::StreamResponseOpen {
                            request_open_at: handle.request_open_at,
                            response_open_at,
                        },
                    ));
                }
            }
        }

        let rsp = {
            let (head, inner) = rsp.into_parts();
            let mut body = ResponseBody {
                inner,
                handle,
                response_open_at,
                response_first_frame_at: None,
                byte_count: 0,
                frame_count: 0,
            };

            if body.is_end_stream() {
                trace!("ResponseFuture::poll: eos");
                body.tap_eos(Some(&head.headers));
            }

            http::Response::from_parts(head, body)
        };

        Ok(rsp.into())
    }
}

// === RequestBody ===

impl<B: Body> Body for RequestBody<B> {
    type Data = <B::Data as IntoBuf>::Buf;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let poll_frame = self.inner.poll_data().map_err(|e| self.tap_err(e));
        let frame = try_ready!(poll_frame).map(|f| f.into_buf());

        if self.handle.is_some() {
            if let Some(ref f) = frame {
                self.frame_count += 1;
                self.byte_count += f.remaining();
            }
        }

        if self.inner.is_end_stream() {
            self.tap_eos(None);
        }

        Ok(Async::Ready(frame))
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, h2::Error> {
        let trailers = try_ready!(self.inner.poll_trailers().map_err(|e| self.tap_err(e)));
        self.tap_eos(trailers.as_ref());
        Ok(Async::Ready(trailers))
    }
}

impl<B> RequestBody<B> {
    fn tap_eos(&mut self, _: Option<&http::HeaderMap>) {
        if let Some(mut h) = self.handle.take() {
            if let Some(t) = h.taps.take() {
                if let Ok(mut taps) = t.lock() {
                    taps.inspect(&event::Event::StreamRequestEnd(
                        h.meta.clone(),
                        event::StreamRequestEnd {
                            request_open_at: h.request_open_at,
                            request_end_at: clock::now(),
                        },
                    ));
                }
            }
        }
    }

    fn tap_err(&mut self, e: h2::Error) -> h2::Error {
        if let Some(mut h) = self.handle.take() {
            h.tap_err(e.reason())
        }
        e
    }
}

impl Handle<event::Request> {
    fn tap_err(&mut self, reason: Option<h2::Reason>) {
        if let Some(t) = self.taps.take() {
            if let Ok(mut taps) = t.lock() {
                taps.inspect(&event::Event::StreamRequestFail(
                    self.meta.clone(),
                    event::StreamRequestFail {
                        request_open_at: self.request_open_at,
                        request_fail_at: clock::now(),
                        error: reason.unwrap_or(h2::Reason::INTERNAL_ERROR),
                    },
                ));
            }
        }
    }
}

impl<B> Drop for RequestBody<B> {
    fn drop(&mut self) {
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.tap_eos(None);
    }
}

// === ResponseBody ===

impl<B: Body + Default> Default for ResponseBody<B> {
    fn default() -> Self {
        Self {
            inner: B::default(),
            handle: None,
            response_open_at: clock::now(),
            response_first_frame_at: None,
            byte_count: 0,
            frame_count: 0,
        }
    }
}

impl<B: Body> Body for ResponseBody<B> {
    type Data = <B::Data as IntoBuf>::Buf;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        trace!("ResponseBody::poll_data");
        let poll_frame = self.inner.poll_data().map_err(|e| self.tap_err(e));
        let frame = try_ready!(poll_frame).map(|f| f.into_buf());

        if self.handle.is_some() {
            if self.response_first_frame_at.is_none() {
                self.response_first_frame_at = Some(clock::now());
            }
            if let Some(ref f) = frame {
                self.frame_count += 1;
                self.byte_count += f.remaining();
            }
        }

        if self.inner.is_end_stream() {
            self.tap_eos(None);
        }

        Ok(Async::Ready(frame))
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, h2::Error> {
        trace!("ResponseBody::poll_trailers");
        let trailers = try_ready!(self.inner.poll_trailers().map_err(|e| self.tap_err(e)));
        self.tap_eos(trailers.as_ref());
        Ok(Async::Ready(trailers))
    }
}

impl<B> ResponseBody<B> {
    fn tap_eos(&mut self, trailers: Option<&http::HeaderMap>) {
        trace!("ResponseBody::tap_eos: trailers={}", trailers.is_some());
        if let Some(mut h) = self.handle.take() {
            if let Some(t) = h.taps.take() {
                let response_end_at = clock::now();
                if let Ok(mut taps) = t.lock() {
                    taps.inspect(&event::Event::StreamResponseEnd(
                        h.meta.clone(),
                        event::StreamResponseEnd {
                            request_open_at: h.request_open_at,
                            response_open_at: self.response_open_at,
                            response_first_frame_at: self
                                .response_first_frame_at
                                .unwrap_or(response_end_at),
                            response_end_at,
                            grpc_status: trailers.and_then(Self::grpc_status),
                            bytes_sent: self.byte_count as u64,
                        },
                    ));
                }
            }
        }
    }

    fn grpc_status(t: &http::HeaderMap) -> Option<u32> {
        t.get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
    }

    fn tap_err(&mut self, e: h2::Error) -> h2::Error {
        trace!("ResponseBody::tap_err: {:?}", e);

        if let Some(mut h) = self.handle.take() {
            if let Some(t) = h.taps.take() {
                if let Ok(mut taps) = t.lock() {
                    taps.inspect(&event::Event::StreamResponseFail(
                        h.meta.clone(),
                        event::StreamResponseFail {
                            request_open_at: h.request_open_at,
                            response_open_at: self.response_open_at,
                            response_first_frame_at: self.response_first_frame_at,
                            response_fail_at: clock::now(),
                            error: e.reason().unwrap_or(h2::Reason::INTERNAL_ERROR),
                            bytes_sent: self.byte_count as u64,
                        },
                    ));
                }
            }
        }

        e
    }
}

impl<B> Drop for ResponseBody<B> {
    fn drop(&mut self) {
        trace!("ResponseHandle::drop");
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.tap_eos(None);
    }
}
