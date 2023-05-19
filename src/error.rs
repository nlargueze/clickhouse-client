//! Crate error

/// Generic error
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub(crate) String);

impl Error {
    /// Creates a new error
    pub fn new(msg: &str) -> Self {
        Self(msg.to_string())
    }
}

impl From<hyper::http::Error> for Error {
    fn from(value: hyper::http::Error) -> Self {
        Error(value.to_string())
    }
}

impl From<hyper::http::uri::InvalidUriParts> for Error {
    fn from(value: hyper::http::uri::InvalidUriParts) -> Self {
        Error(value.to_string())
    }
}

impl From<hyper::http::uri::InvalidUri> for Error {
    fn from(value: hyper::http::uri::InvalidUri) -> Self {
        Error(value.to_string())
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Error(value.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Error(value.to_string())
    }
}

impl From<leb128::read::Error> for Error {
    fn from(value: leb128::read::Error) -> Self {
        Error(value.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error(value.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(value: std::num::TryFromIntError) -> Self {
        Error(value.to_string())
    }
}
