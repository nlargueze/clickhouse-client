//! Query statement

use crate::value::{ChValue, Value};

/// SQQL query placeholder
const QUERY_PARAM_PLACEHOLDER: &str = "[??]";

/// Extension trait for SQL statements
pub(crate) trait SqlStatement: Sized {
    /// Binds the statement to a string
    fn bind_str(self, value: &str) -> Self;

    /// Binds the statement to a value
    fn bind_val(self, value: impl ChValue) -> Self {
        self.bind_str(&value.into_ch_value().to_sql_string())
    }

    /// Binds the statement to a list of strings
    fn bind_str_list(self, values: Vec<&str>) -> Self {
        self.bind_str(values.join(", ").as_str())
    }

    /// Binds the statement to a list of [Value]s
    fn bind_val_list(self, values: Vec<Value>) -> Self {
        self.bind_str(
            values
                .into_iter()
                .map(|v| v.to_sql_string())
                .collect::<Vec<_>>()
                .join(", ")
                .as_str(),
        )
    }
}

impl SqlStatement for String {
    fn bind_str(self, value: &str) -> String {
        self.replacen(QUERY_PARAM_PLACEHOLDER, value, 1)
    }
}

/// SQL WHERE condition
#[derive(Debug, Clone, Default)]
pub struct Where {
    clause: String,
}

impl Where {
    /// Creates a new WHERE condition
    pub fn new(col: &str, operator: &str, value: &str) -> Self {
        let mut w = Self::default();
        w.add_condition("AND", col, operator, value);
        w
    }

    /// Creates a new WHERE condition with a [Value]
    pub fn with_val(col: &str, operator: &str, value: impl ChValue) -> Self {
        Self::new(
            col,
            operator,
            value.into_ch_value().to_sql_string().as_str(),
        )
    }

    /// Adds an AND condition
    pub fn and(mut self, col: &str, operator: &str, value: &str) -> Self {
        self.add_condition("AND", col, operator, value);
        self
    }

    /// Adds an OR condition
    pub fn or(mut self, col: &str, operator: &str, value: &str) -> Self {
        self.add_condition("OR", col, operator, value);
        self
    }

    /// Adds an AND condition
    pub fn and_val(mut self, col: &str, operator: &str, value: impl ChValue) -> Self {
        self.add_condition_val("AND", col, operator, value);
        self
    }

    /// Adds an OR condition
    pub fn or_val(mut self, col: &str, operator: &str, value: impl ChValue) -> Self {
        self.add_condition_val("OR", col, operator, value);
        self
    }

    /// Adds a condition
    fn add_condition(&mut self, prefix: &str, col: &str, operator: &str, value: &str) {
        let value = if self.clause.is_empty() {
            format!("{} {} {}", col, operator, value)
        } else {
            format!("{} {} {} {} {}", self.clause, prefix, col, operator, value)
        };
        self.clause = value;
    }

    /// Adds a condition with a [Value]
    fn add_condition_val(&mut self, prefix: &str, col: &str, operator: &str, value: impl ChValue) {
        self.add_condition(
            prefix,
            col,
            operator,
            value.into_ch_value().to_sql_string().as_str(),
        );
    }
}

impl std::fmt::Display for Where {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.clause.is_empty() {
            write!(f, "")
        } else {
            write!(f, " WHERE {}", self.clause)
        }
    }
}
