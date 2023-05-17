//! Error

/// Clickhouse client error
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

impl Error {
    /// Creates a new error
    pub fn new(msg: &str) -> Self {
        Self(msg.to_string())
    }

    /// Returns the error message
    pub fn message(&self) -> &str {
        &self.0
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

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Error(value.to_string())
    }
}

impl From<std::str::ParseBoolError> for Error {
    fn from(value: std::str::ParseBoolError) -> Self {
        Error(value.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(value: uuid::Error) -> Self {
        Error(value.to_string())
    }
}

impl From<time::error::Parse> for Error {
    fn from(value: time::error::Parse) -> Self {
        Error(value.to_string())
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(value: time::error::ComponentRange) -> Self {
        Error(value.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(value: std::array::TryFromSliceError) -> Self {
        Error(value.to_string())
    }
}
