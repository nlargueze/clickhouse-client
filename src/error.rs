//! Error

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

impl Error {
    /// Creates a new error
    pub fn new(msg: &str) -> Self {
        Self(msg.to_string())
    }
}

impl From<hyper::http::Error> for Error {
    fn from(value: hyper::http::Error) -> Self {
        Error::new(value.to_string().as_str())
    }
}

impl From<hyper::http::uri::InvalidUriParts> for Error {
    fn from(value: hyper::http::uri::InvalidUriParts) -> Self {
        Error::new(value.to_string().as_str())
    }
}

impl From<hyper::http::uri::InvalidUri> for Error {
    fn from(value: hyper::http::uri::InvalidUri) -> Self {
        Error::new(value.to_string().as_str())
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Error::new(value.to_string().as_str())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Error::new(value.to_string().as_str())
    }
}
