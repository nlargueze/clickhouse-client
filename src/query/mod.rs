//! Utilities for queries

use crate::orm::DbValue;

/// Query structure
#[derive(Debug, Clone)]
pub struct Query {
    /// Base query
    pub base_query: String,
    /// Where condition
    pub where_cond: Option<Where>,
}

impl Query {
    /// Instantiates a new query
    pub fn new(base_query_str: &str) -> Self {
        Self {
            base_query: base_query_str.to_string(),
            where_cond: None,
        }
    }

    /// Adds a where condition
    pub fn with_where(mut self, where_cond: Where) -> Self {
        self.where_cond = Some(where_cond);
        self
    }
}

impl From<String> for Query {
    fn from(value: String) -> Self {
        Query::new(&value)
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.base_query,
            self.where_cond.clone().unwrap_or_default()
        )
    }
}

/// Where condition
#[derive(Debug, Clone, Default)]
pub struct Where {
    /// Statement (prefix, column, condition, value)
    statements: Vec<(String, String, String, String)>,
}

impl Where {
    /// Instantiates with 1 condition
    pub fn new(col: &str, condition: &str, value: impl DbValue) -> Self {
        let mut where_cond = Self::default();
        where_cond.add_raw_statement("", col, condition, value.to_sql_str().as_str());
        where_cond
    }

    /// Instantiates an empty condition
    pub fn empty() -> Self {
        Self::default()
    }

    /// Adds an AND statement
    pub fn and(mut self, col: &str, condition: &str, value: impl DbValue) -> Self {
        self.add_raw_statement("AND", col, condition, value.to_sql_str().as_str());
        self
    }

    /// Adds an OR statement
    pub fn or(mut self, col: &str, condition: &str, value: impl DbValue) -> Self {
        self.add_raw_statement("OR", col, condition, value.to_sql_str().as_str());
        self
    }

    /// Adds a raw statement to a WHERE condition
    fn add_raw_statement(&mut self, prefix: &str, column: &str, condition: &str, value: &str) {
        self.statements.push((
            prefix.to_string(),
            column.to_string(),
            condition.to_string(),
            value.to_string(),
        ))
    }
}

impl std::fmt::Display for Where {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.statements.is_empty() {
            let s = self
                .statements
                .iter()
                .map(|(prefix, col, condition, value)| {
                    format!(
                        "{}({} {} {})",
                        if !prefix.is_empty() {
                            format!("{} ", prefix)
                        } else {
                            "".to_string()
                        },
                        col,
                        condition,
                        value
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            write!(f, " WHERE {s}")
        } else {
            write!(f, "")
        }
    }
}
