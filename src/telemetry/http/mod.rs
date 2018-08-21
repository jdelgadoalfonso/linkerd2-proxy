use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::metrics::{
    latency,
    Counter,
    FmtMetrics,
    Histogram,
    Scopes,
};
use telemetry::tap::Taps;

pub mod event;
mod labels;
mod record;
pub mod service;
mod sensors;

use self::labels::{RequestLabels, ResponseLabels};
use self::record::Record;
pub use self::sensors::Sensors;

metrics! {
    request_total: Counter { "Total count of HTTP requests." },
    response_total: Counter { "Total count of HTTP responses" },
    response_latency_ms: Histogram<latency::Ms> {
        "Elapsed times between a request's headers being received \
        and its response stream completing"
    }
}

pub fn new(metrics_retain_idle: Duration, taps: &Arc<Mutex<Taps>>) -> (Sensors, Report) {
    let inner = Arc::new(Mutex::new(Inner {
        retain_idle: metrics_retain_idle,
        .. Inner::default()
    }));

    let sensors = Sensors::new(Record::new(Registry(inner.clone())), taps);
    (sensors, Report(inner))
}

#[derive(Clone, Debug, Default)]
struct Registry(Arc<Mutex<Inner>>);

#[derive(Clone, Debug, Default)]
pub struct Report(Arc<Mutex<Inner>>);

#[derive(Debug, Default)]
struct Inner {
    retain_idle: Duration,
    requests: RequestScopes,
    responses: ResponseScopes,
}

type RequestScopes = Scopes<RequestLabels, Stamped<RequestMetrics>>;

#[derive(Debug, Default)]
struct RequestMetrics {
    total: Counter,
}

type ResponseScopes = Scopes<ResponseLabels, Stamped<ResponseMetrics>>;

#[derive(Debug, Default)]
pub struct ResponseMetrics {
    total: Counter,
    latency: Histogram<latency::Ms>,
}

#[derive(Debug)]
struct Stamped<T> {
    stamp: Instant,
    inner: T,
}

impl Registry {

    fn end_request(&mut self, labels: RequestLabels) {
        let mut inner = match self.0.lock() {
            Err(_) => return,
            Ok(lock) => lock,
        };

        inner.requests.get_or_default(labels).stamped().end()
    }

    fn end_response(&mut self, labels: ResponseLabels, latency: Duration) {
        let mut inner = match self.0.lock() {
            Err(_) => return,
            Ok(lock) => lock,
        };

        inner.responses.get_or_default(labels).stamped().end(latency)
    }
}

// ===== impl Report =====

impl FmtMetrics for Report {
    fn fmt_metrics(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut inner = match self.0.lock() {
            Err(_) => return Ok(()),
            Ok(lock) => lock,
        };

        let epoch = Instant::now() - inner.retain_idle;
        inner.requests.retain(|_, v| v.stamp >= epoch);
        inner.responses.retain(|_, v| v.stamp >= epoch);

        let inner = inner;

        if !inner.requests.is_empty() {
            request_total.fmt_help(f)?;
            request_total.fmt_scopes(f, &inner.requests, |s| &s.total)?;
        }

        if !inner.responses.is_empty() {
            response_total.fmt_help(f)?;
            response_total.fmt_scopes(f, &inner.responses, |s| &s.total)?;

            response_latency_ms.fmt_help(f)?;
            response_latency_ms.fmt_scopes(f, &inner.responses, |s| &s.latency)?;
        }

        Ok(())
    }
}

// ===== impl RequestMetrics =====

impl RequestMetrics {
    pub fn end(&mut self) {
        self.total.incr();
    }

    #[cfg(test)]
    pub(super) fn total(&self) -> u64 {
        self.total.into()
    }
}

// ===== impl ResponseMetrics =====

impl ResponseMetrics {
    pub fn end(&mut self, duration: Duration) {
        self.total.incr();
        self.latency.add(duration);
    }

    #[cfg(test)]
    pub(super) fn total(&self) -> u64 {
        self.total.into()
    }

    #[cfg(test)]
    pub(super) fn latency(&self) -> &Histogram<latency::Ms> {
        &self.latency
    }
}

// ===== impl Stamped =====

impl<T> Stamped<T> {
    fn stamped(&mut self) -> &mut T {
        self.stamp = Instant::now();
        &mut self.inner
    }
}

impl<T: Default> Default for Stamped<T> {
    fn default() -> Self {
        T::default().into()
    }
}

impl<T> From<T> for Stamped<T> {
    fn from(inner: T) -> Self {
        Self {
            inner,
            stamp: Instant::now(),
        }
    }
}

impl<T> ::std::ops::Deref for Stamped<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use ctx;
    use ctx::test_util::*;
    use super::*;
    use conditional::Conditional;
    use tls;

    const TLS_DISABLED: Conditional<(), tls::ReasonForNoTls> =
        Conditional::None(tls::ReasonForNoTls::Disabled);

    fn mock_route(
        registry: &mut http::Registry,
        proxy: ctx::Proxy,
        server: &Arc<ctx::transport::Server>,
        team: &str
    ) {
        let client = client(proxy, indexmap!["team".into() => team.into(),], TLS_DISABLED);
        let (req, rsp) = request("http://nba.com", &server, &client);
        registry.request(http::RequestLabels::new(&req)).end();
        registry.response(http::ResponseLabels::new(&rsp, None)).end(Duration::from_millis(10));
   }

    #[test]
    fn expiry() {
        let proxy = ctx::Proxy::Outbound;

        let server = server(proxy, TLS_DISABLED);

        let mut report = Report::default();

        let t0 = Instant::now();

        mock_route(&mut report, proxy, &server, "warriors");
        let t1 = Instant::now();

        mock_route(&mut report, proxy, &server, "sixers");
        let t2 = Instant::now();

        assert_eq!(report.requests.len(), 2);
        assert_eq!(report.responses.len(), 2);

        report.retain_since(t0);
        assert_eq!(report.requests.len(), 2);
        assert_eq!(report.responses.len(), 2);

        report.retain_since(t1);
        assert_eq!(report.requests.len(), 1);
        assert_eq!(report.responses.len(), 1);

        report.retain_since(t2);
        assert_eq!(report.requests.len(), 0);
        assert_eq!(report.responses.len(), 0);
    }
}
