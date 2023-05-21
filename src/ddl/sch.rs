//! Schemas

/// Table schema
#[derive(Debug)]
pub struct TableSchema {
    /// Name
    pub name: String,
    /// Columns
    pub columns: Vec<ColSchema>,
}

impl TableSchema {
    /// Creates a new table schema with columns
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: vec![],
        }
    }

    /// Adds a column
    pub fn column(mut self, column: ColSchema) -> Self {
        self.columns.push(column);
        self
    }

    /// Adds a column
    pub fn new_column(mut self, id: &str, ty: &str, is_primary: bool) -> Self {
        self.columns.push(ColSchema::new(id, ty, is_primary));
        self
    }
}

/// Static column schema
#[derive(Debug, Clone)]
pub struct ColSchema {
    /// ID
    pub id: String,
    /// Type (Clickhouse data type)
    pub ty: String,
    /// Primary key
    pub is_primary: bool,
}

impl ColSchema {
    /// Creates a new column
    pub fn new(id: &str, ty: &str, is_primary: bool) -> Self {
        Self {
            id: id.to_string(),
            ty: ty.to_string(),
            is_primary,
        }
    }
}
