// [File 28] In-memory data store
//
// Simple HashMap-based storage for tables. Each table is a Vec of rows.

use expr::schema::Schema;
use expr::types::Value;
use std::collections::HashMap;

/// A named table with a schema and row data.
pub struct InMemoryTable {
    pub schema: Schema,
    pub rows: Vec<Vec<Value>>,
}

/// A collection of named tables — the "database" for the in-memory engine.
pub struct InMemoryDataStore {
    pub tables: HashMap<String, InMemoryTable>,
}

impl InMemoryDataStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table with its schema and initial row data.
    pub fn register_table(
        &mut self,
        name: impl Into<String>,
        schema: Schema,
        rows: Vec<Vec<Value>>,
    ) {
        self.tables.insert(
            name.into(),
            InMemoryTable { schema, rows },
        );
    }

    /// Look up a table by name.
    pub fn get_table(&self, name: &str) -> Option<&InMemoryTable> {
        self.tables.get(name)
    }
}
