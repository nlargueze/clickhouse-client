//! Table

use crate::{
    schema::TableSchema,
    value::{Type, Value},
};

/// Query data
#[derive(Debug, Clone, PartialEq)]
pub enum QueryData {
    /// Without headers
    NoHeaders {
        /// Rows
        rows: Vec<Vec<Value>>,
    },
    /// With columnn names
    WithNames {
        /// Column names
        names: Vec<String>,
        /// Rows
        rows: Vec<Vec<Value>>,
    },
    /// With columnn names and types
    WithNamesAndTypes {
        /// Column names and types
        names_and_types: Vec<(String, Type)>,
        /// Rows
        rows: Vec<Vec<Value>>,
    },
}

/// Query data parts
#[derive(Debug, Clone, PartialEq)]
pub struct QueryDataParts {
    /// Names
    pub names: Option<Vec<String>>,
    /// Types
    pub types: Option<Vec<Type>>,
    /// Rows
    pub rows: Vec<Vec<Value>>,
}

impl Default for QueryData {
    fn default() -> Self {
        Self::NoHeaders { rows: vec![] }
    }
}

impl QueryData {
    /// Creates a new [QueryTable] witout headers
    pub fn no_headers() -> Self {
        Self::default()
    }

    /// Creates a new [QueryTable] with column names
    pub fn with_names(names: Vec<&str>) -> Self {
        Self::WithNames {
            names: names.iter().map(|n| n.to_string()).collect(),
            rows: vec![],
        }
    }

    /// Creates a new [QueryTable] with column names and types
    pub fn with_names_and_types(names_and_types: Vec<(&str, Type)>) -> Self {
        Self::WithNamesAndTypes {
            names_and_types: names_and_types
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
            rows: vec![],
        }
    }

    /// Creates a new [QueryTable] from a [TableSchema]
    pub fn from_schema(schema: &TableSchema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|c| (c.id.as_str(), c.ty.clone()))
            .collect::<Vec<_>>();
        Self::with_names_and_types(columns)
    }

    /// Adds a row
    pub fn row(mut self, row: Vec<Value>) -> Self {
        self.add_row(row);
        self
    }

    /// Adds several rows
    pub fn rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        self.add_rows(rows);
        self
    }

    /// Adds a row
    pub fn add_row(&mut self, row: Vec<Value>) -> &mut Self {
        self.add_row_checked(row);
        self
    }

    /// Adds rows
    pub fn add_rows(&mut self, rows: Vec<Vec<Value>>) -> &mut Self {
        for row in rows {
            self.add_row_checked(row);
        }
        self
    }

    /// Adds a row and checks the row length (internal method)
    ///
    /// # Panics
    ///
    /// Panics if the row length does not match the number of columns
    fn add_row_checked(&mut self, row: Vec<Value>) {
        // check that the row length matches the column length
        #[cfg(debug_assertions)]
        {
            let n_cols = self.n_cols();
            if n_cols > 0 && row.len() != n_cols {
                panic!(
                    "Row length ({}) does not match the number of columns ({})",
                    row.len(),
                    n_cols
                );
            }
        }

        // check the value type for the column
        #[cfg(debug_assertions)]
        {
            if let Some(columns) = self.get_columns() {
                for (i, value) in row.iter().enumerate() {
                    if let Some((n, Some(ty))) = columns.get(i) {
                        if !value.is_same_type_as(ty) {
                            panic!("Field '{n}' should have the type '{ty}'");
                        };
                    }
                }
            }
        }

        self.get_rows_mut().push(row);
    }

    /// Returns a reference to the rows
    pub fn get_rows(&self) -> &Vec<Vec<Value>> {
        match self {
            QueryData::NoHeaders { rows } => rows,
            QueryData::WithNames { names: _, rows } => rows,
            QueryData::WithNamesAndTypes {
                names_and_types: _,
                rows,
            } => rows,
        }
    }

    /// Returns a mut reference to the rows
    pub fn get_rows_mut(&mut self) -> &mut Vec<Vec<Value>> {
        match self {
            QueryData::NoHeaders { rows } => rows,
            QueryData::WithNames { names: _, rows } => rows,
            QueryData::WithNamesAndTypes {
                names_and_types: _,
                rows,
            } => rows,
        }
    }

    /// Returns the number of rows
    pub fn n_rows(&self) -> usize {
        self.get_rows().len()
    }

    /// Gets a reference to the columns
    pub fn get_columns(&self) -> Option<Vec<(&str, Option<Type>)>> {
        match &self {
            Self::NoHeaders { rows: _ } => None,
            Self::WithNames { rows: _, names } => {
                Some(names.iter().map(|n| (n.as_str(), None)).collect())
            }
            Self::WithNamesAndTypes {
                rows: _,
                names_and_types,
            } => Some(
                names_and_types
                    .iter()
                    .map(|(n, t)| (n.as_str(), Some(t.clone())))
                    .collect(),
            ),
        }
    }

    /// Returns the number of columns
    pub fn n_cols(&self) -> usize {
        let columns = self.get_columns();
        if let Some(columns) = &columns {
            // if headers are defined, thsi set the number of columns
            columns.len()
        } else {
            // if no headers are defined, we use the first row length
            let rows = self.get_rows();
            rows.first().map(|row| row.len()).unwrap_or(0)
        }
    }

    /// Gets a reference to the column names and types
    pub fn get_names_and_types(&self) -> Option<Vec<(&str, Type)>> {
        match self {
            QueryData::NoHeaders { .. } => None,
            QueryData::WithNames { .. } => None,
            QueryData::WithNamesAndTypes {
                names_and_types, ..
            } => Some(
                names_and_types
                    .iter()
                    .map(|(n, t)| (n.as_str(), t.clone()))
                    .collect::<Vec<_>>(),
            ),
        }
    }

    /// Gets a reference to the column types
    pub fn get_types(&self) -> Option<Vec<Type>> {
        match self {
            QueryData::NoHeaders { .. } => None,
            QueryData::WithNames { .. } => None,
            QueryData::WithNamesAndTypes {
                names_and_types, ..
            } => Some(
                names_and_types
                    .iter()
                    .map(|(_, t)| t.clone())
                    .collect::<Vec<_>>(),
            ),
        }
    }

    // /// Returns the table columns names
    // pub fn column_names(&self) -> Option<Vec<&str>> {
    //     match &self.headers {
    //         QueryTableHeaders::None => None,
    //         QueryTableHeaders::Names(names) => Some(names.iter().map(|n| n.as_str()).collect()),
    //         QueryTableHeaders::NamesAndTypes(names_and_types) => {
    //             Some(names_and_types.iter().map(|(n, _t)| n.as_str()).collect())
    //         }
    //     }
    // }

    // /// Returns the table columns names and types
    // pub fn column_names_and_types(&self) -> Option<Vec<(&str, Type)>> {
    //     match &self.headers {
    //         QueryTableHeaders::None => None,
    //         QueryTableHeaders::Names(_) => None,
    //         QueryTableHeaders::NamesAndTypes(names_and_types) => Some(
    //             names_and_types
    //                 .iter()
    //                 .map(|(n, t)| (n.as_str(), t.clone()))
    //                 .collect(),
    //         ),
    //     }
    // }

    /// Extracts the parts
    pub fn into_parts(self) -> QueryDataParts {
        match self {
            QueryData::NoHeaders { rows } => QueryDataParts {
                names: None,
                types: None,
                rows,
            },
            QueryData::WithNames { names, rows } => QueryDataParts {
                names: Some(names),
                types: None,
                rows,
            },
            QueryData::WithNamesAndTypes {
                names_and_types,
                rows,
            } => {
                let mut names = vec![];
                let mut types = vec![];
                for nt in names_and_types {
                    names.push(nt.0);
                    types.push(nt.1);
                }
                QueryDataParts {
                    names: Some(names),
                    types: Some(types),
                    rows,
                }
            }
        }
    }
}

impl std::fmt::Display for QueryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use prettytable::{Cell, Row, Table};

        let mut table = Table::new();

        if let Some(columns) = self.get_columns() {
            table.add_row(Row::new(
                columns
                    .into_iter()
                    .map(|(n, t)| {
                        Cell::new(
                            format!("{}{}", n, t.map(|t| format!(" ({t})")).unwrap_or_default())
                                .as_str(),
                        )
                    })
                    .collect(),
            ));
        }

        for row in self.get_rows() {
            table.add_row(Row::new(
                row.iter()
                    .map(|v| Cell::new(v.to_string().as_str()))
                    .collect(),
            ));
        }

        write!(f, "{table}")
    }
}

// custom cell iterator

// /// A cell of a [QueryTable]
// #[derive(Debug)]
// pub struct QueryTableCell<'a> {
//     /// Column name
//     pub name: Option<&'a str>,
//     /// Column type
//     pub ty: Option<&'a Type>,
//     /// Cell value
//     pub value: &'a Value,
// }

// impl QueryData {
//     /// Returns the table cells
//     pub fn cells(&self) -> Vec<Vec<QueryTableCell>> {
//         let columns = self.columns();
//         self.rows
//             .iter()
//             .map(|row| {
//                 row.iter()
//                     .enumerate()
//                     .map(|(i, value)| {
//                         let name = columns.as_ref().and_then(|c| c.get(i).map(|(n, _)| *n));
//                         let ty = columns
//                             .as_ref()
//                             .and_then(|c| c.get(i).map(|(_, t)| *t))
//                             .flatten();
//                         QueryTableCell { name, ty, value }
//                     })
//                     .collect::<Vec<_>>()
//             })
//             .collect()
//     }
// }
