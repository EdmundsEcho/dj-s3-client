use std::error::Error as StdError;
use std::fmt;
use std::io;
use url::Url;

// use std::convert::From;
// use thiserror::Error;
//
// usage: Err(_elapsed) => Err(Box::new(crate::error::TimedOut) as BoxError),

// #[allow(dead_code)]

/// Modeled after reqwest Error
/// ⬜ How use Display fmt
/// A `Result` alias where the `Err` case is `tnc::Error`.
#[allow(unused)]
pub type Result<T> = std::result::Result<T, Error>;

/// sdk error -> this package error
pub fn into(err: impl Into<BoxError>, kind: Kind) -> Error {
    Error::new(kind, Some(err))
}
///
pub struct Error {
    inner: Box<Inner>,
}

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;

struct Inner {
    kind: Kind,
    source: Option<BoxError>,
    url: Option<Url>,
    key: Option<String>,
    msg: Option<String>,
}

impl Error {
    pub(crate) fn new<E>(kind: Kind, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(Inner {
                kind,
                source: source.map(Into::into),
                url: None,
                key: None,
                msg: None,
            }),
        }
    }

    /// Returns a possible URL related to this error.
    #[allow(dead_code)]
    pub fn url(&self) -> Option<&Url> {
        self.inner.url.as_ref()
    }

    /// Returns a mutable reference to the URL related to this error
    #[allow(dead_code)]
    pub fn url_mut(&mut self) -> Option<&mut Url> {
        self.inner.url.as_mut()
    }

    /// Add a url related to this error (overwriting any existing)
    #[allow(dead_code)]
    pub fn with_url(mut self, url: Url) -> Self {
        self.inner.url = Some(url);
        self
    }

    /// Strip the related url from this error (if, for example, it contains
    /// sensitive information)
    #[allow(dead_code)]
    pub fn without_url(mut self) -> Self {
        self.inner.url = None;
        self
    }
    /// Add a S3 key
    #[allow(dead_code)]
    pub fn with_key(mut self, key: impl AsRef<str>) -> Self {
        self.inner.key = Some(key.as_ref().to_string());
        self
    }
    /// Strip the related message
    #[allow(dead_code)]
    pub fn without_key(mut self) -> Self {
        self.inner.key = None;
        self
    }

    /// Add a message related to this error (overwriting any existing)
    #[allow(dead_code)]
    pub fn with_msg(mut self, msg: impl AsRef<str>) -> Self {
        self.inner.msg = Some(msg.as_ref().to_string());
        self
    }
    /// Strip the related message
    #[allow(dead_code)]
    pub fn without_msg(mut self) -> Self {
        self.inner.url = None;
        self
    }

    /// Returns true if the error is from a type Builder.
    /// External match based on internal value
    pub fn is_decode(&self) -> bool {
        matches!(self.inner.kind, Kind::Decode)
    }
    pub fn is_builder(&self) -> bool {
        matches!(self.inner.kind, Kind::Builder)
    }
    #[allow(dead_code)]
    pub fn is_missing_parameter(&self) -> bool {
        matches!(self.inner.kind, Kind::MissingParameter)
    }
    pub fn is_internal(&self) -> bool {
        matches!(self.inner.kind, Kind::Internal)
    }
    pub fn is_response(&self) -> bool {
        matches!(self.inner.kind, Kind::Response)
    }
    pub fn is_unauthorized(&self) -> bool {
        matches!(self.inner.kind, Kind::Unauthorized)
    }
    pub fn is_timedout(&self) -> bool {
        matches!(self.inner.kind, Kind::TimedOut)
    }

    // private
    #[allow(unused)]
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("tnc::Error");

        builder.field("kind", &self.inner.kind);

        if let Some(ref url) = self.inner.url {
            builder.field("url", url);
        }
        if let Some(ref key) = self.inner.key {
            builder.field("key", key);
        }
        if let Some(ref msg) = self.inner.msg {
            builder.field("msg", msg);
        }
        if let Some(ref source) = self.inner.source {
            builder.field("source", source);
        }

        builder.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.inner.kind {
            Kind::Decode => f.write_str("decode error")?,
            Kind::Builder => f.write_str("builder error")?,
            Kind::MissingParameter => f.write_str("missing parameter")?,
            Kind::Internal => f.write_str("internal error")?,
            Kind::Request => f.write_str("request error")?,
            Kind::Response => f.write_str("response error")?,
            Kind::Unauthorized => f.write_str("unauthorized")?,
            Kind::TimedOut => f.write_str("timed-out")?,
            Kind::MalformedData => f.write_str("malformed data")?,
        };

        if let Some(url) = &self.inner.url {
            write!(f, " for url ({})", url.as_str())?;
        }

        if let Some(msg) = &self.inner.msg {
            write!(f, ": {}", msg)?;
        }
        if let Some(e) = &self.inner.source {
            write!(f, ": {}", e)?;
        }

        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner.source.as_ref().map(|e| &**e as _)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Kind {
    Decode,
    Builder,
    Internal,
    Request,
    Response,
    Unauthorized,
    TimedOut,
    MissingParameter,
    MalformedData,
}

// constructors
#[allow(unused)]
pub(crate) fn decode<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::Decode, Some(e)).with_msg(msg)
}
#[allow(unused)]
pub(crate) fn builder<E: Into<BoxError>>(e: E) -> Error {
    Error::new(Kind::Builder, Some(e))
}
#[allow(unused)]
pub(crate) fn internal<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::Internal, Some(e)).with_msg(msg)
}
#[allow(unused)]
pub(crate) fn request<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::Request, Some(e)).with_msg(msg)
}
#[allow(unused)]
pub(crate) fn response<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::Response, Some(e)).with_msg(msg)
}
#[allow(unused)]
pub(crate) fn unauthorized<E: Into<BoxError>>(e: E, url: Url, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::Unauthorized, Some(e))
        .with_url(url)
        .with_msg(msg)
}
#[allow(unused)]
pub(crate) fn timedout<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::TimedOut, Some(e)).with_msg(msg)
}
#[allow(unused)]
pub(crate) fn missing_parameter<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::MissingParameter, Some(e)).with_msg(msg)
}

/// constructor for MalformedData error that includes a message
#[allow(unused)]
pub(crate) fn malformed_data<E: Into<BoxError>>(e: E, msg: impl AsRef<str>) -> Error {
    Error::new(Kind::MalformedData, Some(e)).with_msg(msg)
}

// io::Error helpers

#[allow(unused)]
pub(crate) fn into_io(e: Error) -> io::Error {
    e.into_io()
}

#[allow(unused)]
pub(crate) fn decode_io(e: io::Error) -> Error {
    if e.get_ref().map(|r| r.is::<Error>()).unwrap_or(false) {
        *e.into_inner()
            .expect("io::Error::get_ref was Some(_)")
            .downcast::<Error>()
            .expect("StdError::is() was true")
    } else {
        decode(e, "decode io")
    }
}
// internal Error "sources"

#[derive(Debug)]
pub(crate) struct TimedOut;

impl fmt::Display for TimedOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("operation timed out")
    }
}

impl StdError for TimedOut {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    #[test]
    fn test_source_chain() {
        let root = Error::new(Kind::Response, None::<Error>);
        assert!(root.source().is_none());
        assert_send::<Error>();
        assert_sync::<Error>();
    }

    #[test]
    fn mem_size_of() {
        use std::mem::size_of;
        assert_eq!(size_of::<Error>(), size_of::<usize>());
    }

    #[test]
    fn roundtrip_io_error() {
        let orig = super::request("orig", "test message");
        // Convert reqwest::Error into an io::Error...
        let io = orig.into_io();
        // Convert that io::Error back into a reqwest::Error...
        let err = super::decode_io(io);
        // It should have pulled out the original, not nested it...
        match err.inner.kind {
            Kind::Request => (),
            _ => panic!("{:?}", err),
        }
    }

    #[test]
    fn is_timedout() {
        let err = super::request(super::TimedOut, "test message");
        assert!(err.is_timedout());

        let io = io::Error::new(io::ErrorKind::Other, err);
        let nested = super::request(io, "test message");
        assert!(nested.is_timedout());
    }
}
