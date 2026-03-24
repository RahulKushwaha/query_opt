pub mod engine;

use bincode;
use expr::expr::Operator;
use expr::schema::{Field, Schema};
use expr::types::{DataType, Value};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

const META_CF: &str = "_meta";

/// Metadata for a single table, stored in the _meta column family.
#[derive(Serialize, Deserialize)]
struct TableMeta {
    schema: Vec<(String, DataType)>,
    next_row_id: u64,
    indexes: Vec<String>, // indexed column names
}

/// RocksDB-backed storage layer for the query engine.
pub struct RocksStorage {
    db: DB,
    /// Cache of table metadata to avoid repeated deserialization.
    meta_cache: Mutex<HashMap<String, TableMeta>>,
}

impl RocksStorage {
    /// Open or create a RocksDB database at the given path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();

        // Try to list existing column families, fall back to just default + _meta.
        let cf_names = DB::list_cf(&Options::default(), path).unwrap_or_default();

        let mut cf_descriptors = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Always include "default" and META_CF.
        for name in [String::from("default"), String::from(META_CF)]
            .into_iter()
            .chain(cf_names.into_iter())
        {
            if seen.insert(name.clone()) {
                cf_descriptors.push(ColumnFamilyDescriptor::new(&name, Options::default()));
            }
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
            .expect("failed to open RocksDB");

        let storage = Self {
            db,
            meta_cache: Mutex::new(HashMap::new()),
        };
        storage.load_meta_cache();
        storage
    }

    fn load_meta_cache(&self) {
        let cf = self.db.cf_handle(META_CF).unwrap();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut cache = self.meta_cache.lock().unwrap();
        for item in iter {
            let (key, val) = item.expect("meta iteration failed");
            let table_name = String::from_utf8(key.to_vec()).unwrap();
            if let Ok(meta) = bincode::deserialize::<TableMeta>(&val) {
                cache.insert(table_name, meta);
            }
        }
    }

    fn save_meta(&self, table_name: &str, meta: &TableMeta) {
        let cf = self.db.cf_handle(META_CF).unwrap();
        let bytes = bincode::serialize(meta).unwrap();
        self.db.put_cf(cf, table_name.as_bytes(), &bytes).unwrap();
    }

    /// Create a new table with the given schema.
    pub fn create_table(&self, name: &str, schema: &Schema) {
        // Create column family for the table data.
        self.db
            .create_cf(name, &Options::default())
            .unwrap_or_else(|_| {
                // CF may already exist if reopening.
            });

        let meta = TableMeta {
            schema: schema
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.data_type.clone()))
                .collect(),
            next_row_id: 0,
            indexes: Vec::new(),
        };
        self.save_meta(name, &meta);
        self.meta_cache
            .lock()
            .unwrap()
            .insert(name.to_string(), meta);
    }

    /// Insert a row into the named table.
    pub fn insert_row(&self, table: &str, row: Vec<Value>) {
        let row_id = {
            let mut cache = self.meta_cache.lock().unwrap();
            let meta = cache.get_mut(table).expect("table not found");
            let id = meta.next_row_id;
            meta.next_row_id += 1;
            self.save_meta(table, meta);
            id
        };

        let cf = self.db.cf_handle(table).expect("table CF not found");
        let key = row_id.to_be_bytes();
        let val = bincode::serialize(&row).unwrap();
        self.db.put_cf(cf, key, &val).unwrap();

        // Update secondary indexes.
        let indexes = {
            let cache = self.meta_cache.lock().unwrap();
            let meta = cache.get(table).unwrap();
            meta.indexes.clone()
        };
        let schema = self.get_schema(table).unwrap();
        for col_name in &indexes {
            if let Some((idx, _)) = schema.field_by_name(col_name) {
                self.write_index_entry(table, col_name, &row[idx], row_id);
            }
        }
    }

    /// Full table scan — returns all rows.
    pub fn scan_table(&self, table: &str) -> Vec<Vec<Value>> {
        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Vec::new(),
        };
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut rows = Vec::new();
        for item in iter {
            let (_key, val) = item.expect("scan iteration failed");
            if let Ok(row) = bincode::deserialize::<Vec<Value>>(&val) {
                rows.push(row);
            }
        }
        rows
    }

    /// Get the schema for a table.
    pub fn get_schema(&self, table: &str) -> Option<Schema> {
        let cache = self.meta_cache.lock().unwrap();
        let meta = cache.get(table)?;
        let fields: Vec<Field> = meta
            .schema
            .iter()
            .map(|(name, dt)| Field::new(name.clone(), dt.clone()))
            .collect();
        Some(Schema::new(fields))
    }

    /// List all registered table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.meta_cache.lock().unwrap().keys().cloned().collect()
    }

    // ── Secondary Index Support ────────────────────────

    fn index_cf_name(table: &str, column: &str) -> String {
        format!("idx_{}_{}", table, column)
    }

    fn encode_index_key(value: &Value, row_id: u64) -> Vec<u8> {
        // Prefix byte for sort order: 0=Null, 1=Bool, 2=Int, 3=Float, 4=Str
        let mut key = Vec::new();
        match value {
            Value::Null => key.push(0),
            Value::Bool(b) => {
                key.push(1);
                key.push(if *b { 1 } else { 0 });
            }
            Value::Int(i) => {
                key.push(2);
                // Flip sign bit for correct byte ordering of signed integers.
                key.extend_from_slice(&((*i as u64) ^ (1u64 << 63)).to_be_bytes());
            }
            Value::Float(f) => {
                key.push(3);
                let bits = f.to_bits();
                let ordered = if *f >= 0.0 {
                    bits ^ (1u64 << 63)
                } else {
                    !bits
                };
                key.extend_from_slice(&ordered.to_be_bytes());
            }
            Value::Str(s) => {
                key.push(4);
                key.extend_from_slice(s.as_bytes());
                key.push(0); // null terminator for prefix-free encoding
            }
        }
        key.extend_from_slice(&row_id.to_be_bytes());
        key
    }

    fn write_index_entry(&self, table: &str, column: &str, value: &Value, row_id: u64) {
        let cf_name = Self::index_cf_name(table, column);
        if let Some(cf) = self.db.cf_handle(&cf_name) {
            let key = Self::encode_index_key(value, row_id);
            self.db.put_cf(cf, key, &[]).unwrap();
        }
    }

    /// Create a secondary index on a column, backfilling existing rows.
    pub fn create_index(&self, table: &str, column: &str) {
        let cf_name = Self::index_cf_name(table, column);
        self.db
            .create_cf(&cf_name, &Options::default())
            .unwrap_or(());

        // Backfill from existing data.
        let schema = self.get_schema(table).expect("table not found");
        let col_idx = schema
            .field_by_name(column)
            .expect("column not found in schema")
            .0;

        let table_cf = self.db.cf_handle(table).unwrap();
        let iter = self.db.iterator_cf(table_cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key_bytes, val) = item.unwrap();
            let row_id = u64::from_be_bytes(key_bytes[..8].try_into().unwrap());
            let row: Vec<Value> = bincode::deserialize(&val).unwrap();
            self.write_index_entry(table, column, &row[col_idx], row_id);
        }

        // Record index in metadata.
        let mut cache = self.meta_cache.lock().unwrap();
        let meta = cache.get_mut(table).unwrap();
        if !meta.indexes.contains(&column.to_string()) {
            meta.indexes.push(column.to_string());
            self.save_meta(table, meta);
        }
    }

    /// Check if an index exists on a table column.
    pub fn has_index(&self, table: &str, column: &str) -> bool {
        let cache = self.meta_cache.lock().unwrap();
        cache
            .get(table)
            .map(|m| m.indexes.contains(&column.to_string()))
            .unwrap_or(false)
    }

    /// Scan using a secondary index with a comparison operator and value.
    /// Returns rows matching the condition `column <op> value`.
    pub fn index_scan(
        &self,
        table: &str,
        column: &str,
        op: &Operator,
        target: &Value,
    ) -> Vec<Vec<Value>> {
        let cf_name = Self::index_cf_name(table, column);
        let idx_cf = match self.db.cf_handle(&cf_name) {
            Some(cf) => cf,
            None => return Vec::new(),
        };
        let table_cf = self.db.cf_handle(table).unwrap();

        // Build the prefix for the target value (without row_id suffix).
        let target_prefix = {
            let full = Self::encode_index_key(target, 0);
            full[..full.len() - 8].to_vec() // strip the 8-byte row_id
        };

        let mut row_ids = Vec::new();
        let iter = self.db.iterator_cf(idx_cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key_bytes, _) = item.unwrap();
            let key = key_bytes.to_vec();
            let (value_part, rid_bytes) = key.split_at(key.len() - 8);
            let row_id = u64::from_be_bytes(rid_bytes.try_into().unwrap());

            let matches = match op {
                Operator::Eq => value_part == target_prefix.as_slice(),
                Operator::Lt => value_part < target_prefix.as_slice(),
                Operator::LtEq => value_part <= target_prefix.as_slice(),
                Operator::Gt => value_part > target_prefix.as_slice(),
                Operator::GtEq => value_part >= target_prefix.as_slice(),
                Operator::NotEq => value_part != target_prefix.as_slice(),
                _ => false,
            };

            if matches {
                row_ids.push(row_id);
            }
        }

        // Point-lookup each matching row.
        let mut rows = Vec::new();
        for rid in row_ids {
            if let Ok(Some(val)) = self.db.get_cf(table_cf, rid.to_be_bytes()) {
                if let Ok(row) = bincode::deserialize::<Vec<Value>>(&val) {
                    rows.push(row);
                }
            }
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("name", DataType::Str),
            Field::new("score", DataType::Int),
        ])
    }

    #[test]
    fn roundtrip_create_insert_scan() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        let schema = test_schema();
        storage.create_table("users", &schema);

        storage.insert_row(
            "users",
            vec![Value::Int(1), Value::Str("alice".into()), Value::Int(90)],
        );
        storage.insert_row(
            "users",
            vec![Value::Int(2), Value::Str("bob".into()), Value::Int(75)],
        );

        let rows = storage.scan_table("users");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::Str("alice".into()));
        assert_eq!(rows[1][2], Value::Int(75));
    }

    #[test]
    fn get_schema_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        let schema = test_schema();
        storage.create_table("t", &schema);

        let got = storage.get_schema("t").unwrap();
        assert_eq!(got.fields.len(), 3);
        assert_eq!(got.fields[0].name, "id");
        assert_eq!(got.fields[2].data_type, DataType::Int);
    }

    #[test]
    fn list_tables() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        storage.create_table("a", &Schema::new(vec![Field::new("x", DataType::Int)]));
        storage.create_table("b", &Schema::new(vec![Field::new("y", DataType::Str)]));

        let mut tables = storage.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["a", "b"]);
    }

    #[test]
    fn index_scan_eq() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        storage.create_table("t", &test_schema());

        storage.insert_row("t", vec![Value::Int(1), Value::Str("a".into()), Value::Int(10)]);
        storage.insert_row("t", vec![Value::Int(2), Value::Str("b".into()), Value::Int(20)]);
        storage.insert_row("t", vec![Value::Int(3), Value::Str("c".into()), Value::Int(10)]);

        storage.create_index("t", "score");

        let rows = storage.index_scan("t", "score", &Operator::Eq, &Value::Int(10));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn index_scan_gt() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        storage.create_table("t", &test_schema());

        for i in 0..5 {
            storage.insert_row(
                "t",
                vec![Value::Int(i), Value::Str(format!("r{}", i)), Value::Int(i * 10)],
            );
        }
        storage.create_index("t", "score");

        let rows = storage.index_scan("t", "score", &Operator::Gt, &Value::Int(20));
        assert_eq!(rows.len(), 2); // score=30, score=40
    }

    #[test]
    fn insert_after_index_creation() {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::new(dir.path());
        storage.create_table("t", &test_schema());
        storage.create_index("t", "score");

        storage.insert_row("t", vec![Value::Int(1), Value::Str("a".into()), Value::Int(50)]);
        storage.insert_row("t", vec![Value::Int(2), Value::Str("b".into()), Value::Int(60)]);

        let rows = storage.index_scan("t", "score", &Operator::Eq, &Value::Int(50));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Str("a".into()));
    }
}
