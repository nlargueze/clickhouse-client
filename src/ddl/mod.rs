//! DDL

mod query;

pub use query::*;

use crate::{interface::Interface, Client};

/// DDL query
pub struct DdlQuery<'a, T>
where
    T: Interface,
{
    /// Client
    client: &'a Client<T>,
}

impl<T> Client<T>
where
    T: Interface,
{
    /// Prepares a DDL query
    pub fn ddl(&self) -> DdlQuery<T> {
        DdlQuery { client: self }
    }
}
