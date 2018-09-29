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

pub struct ResponseFuture<S>
where
    S: svc::Service,
{
    state: Option<RequestState>,
    inner: S::Future,
}

#[derive(Debug)]
pub struct RequestBody<B> {
    state: Option<RequestState>,
    inner: B,
}

#[derive(Clone, Debug)]
struct RequestState {
    meta: event::Request,
    taps: Option<Arc<Mutex<Taps>>>,
    request_open_at: Instant,
    byte_count: usize,
    frame_count: usize,
}

#[derive(Debug, Default)]
pub struct ResponseBody<B> {
    state: Option<ResponseState>,
    inner: B,
}

#[derive(Debug)]
struct ResponseState {
    meta: event::Response,
    taps: Option<Arc<Mutex<Taps>>>,
    request_open_at: Instant,
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

        let mut state = meta.as_ref().map(|meta| RequestState {
            meta: meta.clone(),
            taps: Some(self.taps.clone()),
            request_open_at,
            byte_count: 0,
            frame_count: 0,
        });

        if req.body().is_end_stream() {
            if let Some(mut state) = state.take() {
                state.tap_eos(Some(req.headers()));
            }
        }

        let req = {
            let (head, inner) = req.into_parts();
            let state = state.clone();
            http::Request::from_parts(head, RequestBody { state, inner })
        };

        ResponseFuture {
            state,
            inner: self.inner.call(req),
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
                if let Some(mut state) = self.state.take() {
                    state.tap_err(e.reason());
                }
                return Err(e);
            }
        };
        let response_open_at = clock::now();

        let state = if let Some(mut req) = self.state.take() {
            if let Some(taps) = req.taps.take() {
                let meta = event::Response {
                    request: req.meta.clone(),
                    status: rsp.status(),
                };

                if let Ok(mut taps) = taps.lock() {
                    taps.inspect(&event::Event::StreamResponseOpen(
                        meta.clone(),
                        event::StreamResponseOpen {
                            request_open_at: req.request_open_at,
                            response_open_at,
                        },
                    ));
                }

                let mut state = ResponseState {
                    meta,
                    taps: Some(taps),
                    request_open_at: req.request_open_at,
                    response_open_at,
                    response_first_frame_at: None,
                    byte_count: 0,
                    frame_count: 0,
                };

                if rsp.body().is_end_stream() {
                    state.tap_eos(Some(rsp.headers()));
                }

                Some(state)
            } else {
                None
            }
        } else {
            None
        };

        let rsp = {
            let (head, inner) = rsp.into_parts();
            http::Response::from_parts(head, ResponseBody { state, inner })
        };
        Ok(rsp.into())
    }
}

impl RequestState {
    fn tap_eos(&mut self, _: Option<&http::HeaderMap>) {
        if let Some(t) = self.taps.take() {
            if let Ok(mut taps) = t.lock() {
                taps.inspect(&event::Event::StreamRequestEnd(
                    self.meta.clone(),
                    event::StreamRequestEnd {
                        request_open_at: self.request_open_at,
                        request_end_at: clock::now(),
                    },
                ));
            }
        }
    }

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

impl Drop for RequestState {
    fn drop(&mut self) {
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.tap_eos(None);
    }
}

impl<B: Body> RequestBody<B> {
    fn tap_eos(&mut self, trailers: Option<&http::HeaderMap>) {
        if let Some(mut state) = self.state.take() {
            state.tap_eos(trailers);
        }
    }

    fn tap_err(&mut self, e: h2::Error) -> h2::Error {
        if let Some(mut state) = self.state.take() {
            state.tap_err(e.reason());
        }
        e
    }
}

impl<B: Body> Body for RequestBody<B> {
    type Data = <B::Data as IntoBuf>::Buf;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let poll_frame = self.inner.poll_data().map_err(|e| self.tap_err(e));
        let frame = try_ready!(poll_frame).map(|f| f.into_buf());

        if let Some(s) = self.state.as_mut() {
            if let Some(ref f) = frame {
                s.frame_count += 1;
                s.byte_count += f.remaining();
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

impl ResponseState {
    fn grpc_status(t: &http::HeaderMap) -> Option<u32> {
        t.get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
    }

    fn tap_eos(&mut self, trailers: Option<&http::HeaderMap>) {
        if let Some(t) = self.taps.take() {
            let response_end_at = clock::now();
            if let Ok(mut taps) = t.lock() {
                taps.inspect(&event::Event::StreamResponseEnd(
                    self.meta.clone(),
                    event::StreamResponseEnd {
                        request_open_at: self.request_open_at,
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

    fn tap_err(&mut self, reason: Option<h2::Reason>) {
        if let Some(t) = self.taps.take() {
            if let Ok(mut taps) = t.lock() {
                taps.inspect(&event::Event::StreamResponseFail(
                    self.meta.clone(),
                    event::StreamResponseFail {
                        request_open_at: self.request_open_at,
                        response_open_at: self.response_open_at,
                        response_first_frame_at: self.response_first_frame_at,
                        response_fail_at: clock::now(),
                        error: reason.unwrap_or(h2::Reason::INTERNAL_ERROR),
                        bytes_sent: self.byte_count as u64,
                    },
                ));
            }
        }
    }
}

impl Drop for ResponseState {
    fn drop(&mut self) {
        // TODO this should be recorded as a cancelation if the stream didn't end.
        self.tap_eos(None);
    }
}

impl<B: Body> ResponseBody<B> {
    fn tap_eos(&mut self, trailers: Option<&http::HeaderMap>) {
        if let Some(mut state) = self.state.take() {
            state.tap_eos(trailers);
        }
    }

    fn tap_err(&mut self, e: h2::Error) -> h2::Error {
        if let Some(mut state) = self.state.take() {
            state.tap_err(e.reason());
        }
        e
    }
}

impl<B> Body for ResponseBody<B>
where
    B: Body,
{
    type Data = <B::Data as IntoBuf>::Buf;

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, h2::Error> {
        let poll_frame = self.inner.poll_data().map_err(|e| self.tap_err(e));
        let frame = try_ready!(poll_frame).map(|f| f.into_buf());

        if let Some(ref mut s) = self.state.as_mut() {
            if s.response_first_frame_at.is_none() {
                s.response_first_frame_at = Some(clock::now());
            }
            if let Some(ref f) = frame {
                s.frame_count += 1;
                s.byte_count += f.remaining();
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
