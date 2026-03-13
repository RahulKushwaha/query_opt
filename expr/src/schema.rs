// [File 06] Schema and Field definitions
//
// ┌─────────────────────────────────────────────────────┐
// │ IMPLEMENTATION ORDER: 2 of 15                       │
// │ Prerequisites: expr/src/types.rs (step 1)           │
// │ Next: expr/src/expr.rs (step 3)                     │
// └─────────────────────────────────────────────────────┘
//
// DataFusion ref: datafusion/common/src/dfschema.rs

use crate::types::DataType;
use std::collections::HashMap;

/// A single column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// Ordered list of fields describing the shape of a relation (table or intermediate result).
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub lookup: HashMap<String, usize>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        let mut lookup = HashMap::new();
        for field in fields.iter().enumerate() {
            lookup.insert(field.1.name.clone(), lookup.len());
        }

        Self { fields, lookup }
    }

    /// Look up a field by name. Returns the index and a reference to the Field.
    pub fn field_by_name(&self, name: &str) -> Option<(usize, &Field)> {
        // TODO: Iterate over self.fields, find the first field whose name matches,
        // return Some((index, &field)) or None if not found
        // todo!("implement field_by_name lookup")
        self.lookup.get(name).map(|idx| (*idx, &self.fields[*idx]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("name", DataType::Str),
            Field::new("active", DataType::Bool),
        ])
    }

    // ── Field ──────────────────────────────────────────

    #[test]
    fn field_new() {
        let f = Field::new("age", DataType::Int);
        assert_eq!(f.name, "age");
        assert_eq!(f.data_type, DataType::Int);
    }

    #[test]
    fn field_clone_eq() {
        let f = Field::new("x", DataType::Float);
        assert_eq!(f.clone(), f);
        assert_ne!(f, Field::new("y", DataType::Float));
    }

    // ── Schema::new ────────────────────────────────────

    #[test]
    fn schema_stores_fields_in_order() {
        let s = sample_schema();
        assert_eq!(s.fields.len(), 3);
        assert_eq!(s.fields[0].name, "id");
        assert_eq!(s.fields[2].name, "active");
    }

    #[test]
    fn schema_empty() {
        let s = Schema::new(vec![]);
        assert!(s.fields.is_empty());
        assert!(s.lookup.is_empty());
    }

    // ── field_by_name ──────────────────────────────────

    #[test]
    fn field_by_name_found() {
        let s = sample_schema();
        let (idx, field) = s.field_by_name("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(field.data_type, DataType::Str);
    }

    #[test]
    fn field_by_name_first_field() {
        let s = sample_schema();
        let (idx, field) = s.field_by_name("id").unwrap();
        assert_eq!(idx, 0);
        assert_eq!(field.data_type, DataType::Int);
    }

    #[test]
    fn field_by_name_last_field() {
        let s = sample_schema();
        let (idx, _) = s.field_by_name("active").unwrap();
        assert_eq!(idx, 2);
    }

    #[test]
    fn field_by_name_missing() {
        let s = sample_schema();
        assert!(s.field_by_name("nonexistent").is_none());
    }

    #[test]
    fn field_by_name_empty_schema() {
        let s = Schema::new(vec![]);
        assert!(s.field_by_name("anything").is_none());
    }

    // ── lookup map ─────────────────────────────────────

    #[test]
    fn lookup_has_all_fields() {
        let s = sample_schema();
        assert_eq!(s.lookup.len(), 3);
        assert_eq!(s.lookup["id"], 0);
        assert_eq!(s.lookup["name"], 1);
        assert_eq!(s.lookup["active"], 2);
    }

    // ── Schema Clone / Eq ──────────────────────────────

    #[test]
    fn schema_clone_eq() {
        let s = sample_schema();
        assert_eq!(s.clone(), s);
    }
}
