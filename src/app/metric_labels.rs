use std::{
    fmt::{self, Write},
    net,
};

use metrics::FmtLabels;
use transport::tls;
use Conditional;

use super::{classify, inbound, outbound};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointLabels {
    addr: net::SocketAddr,
    direction: Direction,
    tls_status: tls::Status,
    authority: Authority,
    labels: String,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Direction {
    In,
    Out,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Authority(String);

impl From<inbound::Endpoint> for EndpointLabels {
    fn from(ep: inbound::Endpoint) -> Self {
        Self {
            addr: ep.addr,
            authority: Authority(match ep.authority {
                Some(a) => format!("{}", a),
                None => format!("{}", ep.addr),
            }),
            direction: Direction::In,
            tls_status: ep.source_tls_status,
            labels: "".to_owned(),
        }
    }
}

impl From<outbound::Endpoint> for EndpointLabels {
    fn from(ep: outbound::Endpoint) -> Self {
        let mut label_iter = ep.metadata.labels().into_iter();
        let labels = if let Some((k0, v0)) = label_iter.next() {
            let mut s = format!("dst_{}=\"{}\"", k0, v0);
            for (k, v) in label_iter {
                write!(s, ",dst_{}=\"{}\"", k, v).expect("label concat must succeed");
            }
            s
        } else {
            "".to_owned()
        };

        Self {
            addr: ep.connect.addr,
            authority: Authority(match ep.dst.authority {
                Some(a) => format!("{}", a),
                None => format!("{}", ep.dst),
            }),
            direction: Direction::Out,
            tls_status: ep.connect.tls_status(),
            labels,
        }
    }
}

impl FmtLabels for EndpointLabels {
    fn fmt_labels(&self, f: &mut fmt::Formatter) -> fmt::Result {
        (&self.authority, &self.direction).fmt_labels(f)?;

        if !self.labels.is_empty() {
            write!(f, ",{}", self.labels)?;
        }

        write!(f, ",")?;
        self.tls_status.fmt_labels(f)?;

        Ok(())
    }
}

impl FmtLabels for Direction {
    fn fmt_labels(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Direction::In => write!(f, "direction=\"inbound\""),
            Direction::Out => write!(f, "direction=\"outbound\""),
        }
    }
}

impl FmtLabels for Authority {
    fn fmt_labels(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "authority=\"{}\"", self.0)
    }
}

impl FmtLabels for classify::Class {
    fn fmt_labels(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::classify::Class;
        match self {
            Class::Grpc(result, status) => write!(
                f,
                "classification=\"{}\",status_code=\"200\",grpc_status={}",
                result, status
            ),
            Class::Http(result, status) => write!(
                f,
                "classification=\"{}\",status_code=\"{}\"",
                result,
                status.as_str()
            ),
            Class::Stream(result, status) => {
                write!(f, "classification=\"{}\",h2_err=\"{}\"", result, status)
            }
        }
    }
}

impl fmt::Display for classify::Result {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::classify::Result::{Failure, Success};
        match self {
            Success => write!(f, "success"),
            Failure => write!(f, "failure"),
        }
    }
}

impl FmtLabels for tls::Status {
    fn fmt_labels(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Conditional::None(tls::ReasonForNoTls::NoIdentity(why)) => {
                write!(f, "tls=\"no_identity\",no_tls_reason=\"{}\"", why)
            }
            status => write!(f, "tls=\"{}\"", status),
        }
    }
}
