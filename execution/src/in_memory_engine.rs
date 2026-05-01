use std::collections::HashMap;

use expr::expr::{Expr, Operator};
use expr::logical_plan::JoinType;
use expr::schema::Schema;
use expr::types::Value;
use physical_plan::plan::PhysicalPlan;

use crate::engine::{ExecutionEngine, ExecutionError, ResultSet, Row};
use crate::evaluator::{cmp_values, eval};
use crate::helpers::{compute_aggregate, plan_schema};
use crate::memory_table::InMemoryDataStore;

/// Trait for providing rows to the execution engine.
/// Implement this to plug in any storage backend.
pub trait DataSource {
    fn scan(&self, table_name: &str, schema: &Schema) -> Result<ResultSet, ExecutionError>;
}

impl DataSource for InMemoryDataStore {
    fn scan(&self, table_name: &str, _schema: &Schema) -> Result<ResultSet, ExecutionError> {
        let table = self
            .get_table(table_name)
            .ok_or_else(|| ExecutionError::TableNotFound(table_name.into()))?;
        Ok(table.rows.clone())
    }
}

/// Generic execution engine that works with any DataSource.
pub struct GenericEngine<D: DataSource> {
    pub data_source: D,
}

impl<D: DataSource> GenericEngine<D> {
    pub fn new(data_source: D) -> Self {
        Self { data_source }
    }
}

impl<D: DataSource> ExecutionEngine for GenericEngine<D> {
    fn execute(&self, plan: &PhysicalPlan) -> Result<ResultSet, ExecutionError> {
        execute_plan(&self.data_source, plan)
    }
}

/// Execute a physical plan against a data source. Public so storage engines
/// can reuse the operator logic while providing their own TableScan.
pub fn execute_plan(source: &dyn DataSource, plan: &PhysicalPlan) -> Result<ResultSet, ExecutionError> {
    match plan {
        PhysicalPlan::TableScan {
            table_name, schema, ..
        } => source.scan(table_name, schema),

        PhysicalPlan::Filter {
            predicate, input, ..
        } => {
            let schema = plan_schema(input);
            let rows = execute_plan(source, input)?;
            Ok(rows
                .into_iter()
                .filter(|row| matches!(eval(predicate, row, &schema), Ok(Value::Bool(true))))
                .collect())
        }

        PhysicalPlan::Projection { exprs, input } => {
            let schema = plan_schema(input);
            let rows = execute_plan(source, input)?;
            rows.into_iter()
                .map(|row| {
                    exprs
                        .iter()
                        .map(|e| eval(e, &row, &schema).map_err(ExecutionError::Internal))
                        .collect()
                })
                .collect()
        }

        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            on,
            join_type,
        } => execute_join(source, left, right, on, join_type),

        PhysicalPlan::Sort { exprs, input } => {
            let schema = plan_schema(input);
            let mut rows = execute_plan(source, input)?;
            rows.sort_by(|a, b| {
                for e in exprs {
                    let va = eval(e, a, &schema).unwrap_or(Value::Null);
                    let vb = eval(e, b, &schema).unwrap_or(Value::Null);
                    let ord = cmp_values(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            Ok(rows)
        }

        PhysicalPlan::HashAggregate {
            group_by,
            aggr_exprs,
            input,
        } => execute_aggregate(source, group_by, aggr_exprs, input),
    }
}

fn execute_join(
    source: &dyn DataSource,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    on: &Expr,
    join_type: &JoinType,
) -> Result<ResultSet, ExecutionError> {
    let left_schema = plan_schema(left);
    let right_schema = plan_schema(right);
    let left_rows = execute_plan(source, left)?;
    let right_rows = execute_plan(source, right)?;

    let mut combined_fields = left_schema.fields.clone();
    combined_fields.extend(right_schema.fields.clone());
    let combined_schema = Schema::new(combined_fields);

    let null_right: Row = vec![Value::Null; right_schema.fields.len()];
    let null_left: Row = vec![Value::Null; left_schema.fields.len()];
    let mut result = Vec::new();

    match join_type {
        JoinType::Inner => {
            for lr in &left_rows {
                for rr in &right_rows {
                    let mut combined = lr.clone();
                    combined.extend(rr.clone());
                    if let Ok(Value::Bool(true)) = eval(on, &combined, &combined_schema) {
                        result.push(combined);
                    }
                }
            }
        }
        JoinType::Left => {
            for lr in &left_rows {
                let mut matched = false;
                for rr in &right_rows {
                    let mut combined = lr.clone();
                    combined.extend(rr.clone());
                    if let Ok(Value::Bool(true)) = eval(on, &combined, &combined_schema) {
                        result.push(combined);
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined = lr.clone();
                    combined.extend(null_right.clone());
                    result.push(combined);
                }
            }
        }
        JoinType::Right => {
            for rr in &right_rows {
                let mut matched = false;
                for lr in &left_rows {
                    let mut combined = lr.clone();
                    combined.extend(rr.clone());
                    if let Ok(Value::Bool(true)) = eval(on, &combined, &combined_schema) {
                        result.push(combined);
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined = null_left.clone();
                    combined.extend(rr.clone());
                    result.push(combined);
                }
            }
        }
        JoinType::Full => {
            let mut right_matched = vec![false; right_rows.len()];
            for lr in &left_rows {
                let mut left_matched = false;
                for (ri, rr) in right_rows.iter().enumerate() {
                    let mut combined = lr.clone();
                    combined.extend(rr.clone());
                    if let Ok(Value::Bool(true)) = eval(on, &combined, &combined_schema) {
                        result.push(combined);
                        left_matched = true;
                        right_matched[ri] = true;
                    }
                }
                if !left_matched {
                    let mut combined = lr.clone();
                    combined.extend(null_right.clone());
                    result.push(combined);
                }
            }
            for (ri, rr) in right_rows.iter().enumerate() {
                if !right_matched[ri] {
                    let mut combined = null_left.clone();
                    combined.extend(rr.clone());
                    result.push(combined);
                }
            }
        }
    }
    Ok(result)
}

fn execute_aggregate(
    source: &dyn DataSource,
    group_by: &[Expr],
    aggr_exprs: &[Expr],
    input: &PhysicalPlan,
) -> Result<ResultSet, ExecutionError> {
    let schema = plan_schema(input);
    let rows = execute_plan(source, input)?;

    let mut groups: HashMap<Vec<u8>, (Row, Vec<Vec<Value>>)> = HashMap::new();
    for row in &rows {
        let key_vals: Vec<Value> = group_by
            .iter()
            .map(|e| eval(e, row, &schema).unwrap_or(Value::Null))
            .collect();
        let key_bytes = format!("{:?}", key_vals).into_bytes();
        let entry = groups
            .entry(key_bytes)
            .or_insert_with(|| (key_vals.clone(), Vec::new()));
        let agg_inputs: Vec<Value> = aggr_exprs
            .iter()
            .map(|e| match e {
                Expr::AggregateFunction { args, .. } => args
                    .first()
                    .map(|a| eval(a, row, &schema).unwrap_or(Value::Null))
                    .unwrap_or(Value::Null),
                _ => eval(e, row, &schema).unwrap_or(Value::Null),
            })
            .collect();
        entry.1.push(agg_inputs);
    }

    let mut result = Vec::new();
    for (_key, (group_vals, agg_rows)) in groups {
        let mut out_row = group_vals;
        for (ai, agg_expr) in aggr_exprs.iter().enumerate() {
            let val = match agg_expr {
                Expr::AggregateFunction { fun, .. } => {
                    let vals: Vec<&Value> = agg_rows.iter().map(|r| &r[ai]).collect();
                    compute_aggregate(fun, &vals)
                }
                _ => Value::Null,
            };
            out_row.push(val);
        }
        result.push(out_row);
    }
    Ok(result)
}

/// Convenience type alias.
pub type InMemoryEngine = GenericEngine<InMemoryDataStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use expr::expr::AggFunc;
    use expr::schema::Field;
    use expr::types::DataType;

    fn make_engine() -> InMemoryEngine {
        let mut ds = InMemoryDataStore::new();
        ds.register_table(
            "users",
            Schema::new(vec![
                Field::new("id", DataType::Int),
                Field::new("name", DataType::Str),
                Field::new("score", DataType::Int),
            ]),
            vec![
                vec![Value::Int(1), Value::Str("alice".into()), Value::Int(90)],
                vec![Value::Int(2), Value::Str("bob".into()), Value::Int(75)],
                vec![Value::Int(3), Value::Str("carol".into()), Value::Int(90)],
            ],
        );
        GenericEngine::new(ds)
    }

    fn users_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("name", DataType::Str),
            Field::new("score", DataType::Int),
        ])
    }

    #[test]
    fn test_scan() {
        let engine = make_engine();
        let plan = PhysicalPlan::TableScan {
            table_name: "users".into(),
            schema: users_schema(),
        };
        let rows = engine.execute(&plan).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_filter() {
        let engine = make_engine();
        let plan = PhysicalPlan::Filter {
            predicate: Expr::BinaryExpr {
                left: Box::new(Expr::Column("score".into())),
                op: Operator::Gt,
                right: Box::new(Expr::Literal(Value::Int(80))),
            },
            input: Box::new(PhysicalPlan::TableScan {
                table_name: "users".into(),
                schema: users_schema(),
            }),
        };
        let rows = engine.execute(&plan).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_projection() {
        let engine = make_engine();
        let plan = PhysicalPlan::Projection {
            exprs: vec![Expr::Column("name".into())],
            input: Box::new(PhysicalPlan::TableScan {
                table_name: "users".into(),
                schema: users_schema(),
            }),
        };
        let rows = engine.execute(&plan).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![Value::Str("alice".into())]);
    }

    #[test]
    fn test_sort() {
        let engine = make_engine();
        let plan = PhysicalPlan::Sort {
            exprs: vec![Expr::Column("name".into())],
            input: Box::new(PhysicalPlan::TableScan {
                table_name: "users".into(),
                schema: users_schema(),
            }),
        };
        let rows = engine.execute(&plan).unwrap();
        assert_eq!(rows[0][1], Value::Str("alice".into()));
        assert_eq!(rows[1][1], Value::Str("bob".into()));
        assert_eq!(rows[2][1], Value::Str("carol".into()));
    }

    #[test]
    fn test_aggregate() {
        let engine = make_engine();
        let plan = PhysicalPlan::HashAggregate {
            group_by: vec![Expr::Column("score".into())],
            aggr_exprs: vec![Expr::AggregateFunction {
                fun: AggFunc::Count,
                args: vec![Expr::Column("id".into())],
            }],
            input: Box::new(PhysicalPlan::TableScan {
                table_name: "users".into(),
                schema: users_schema(),
            }),
        };
        let rows = engine.execute(&plan).unwrap();
        assert_eq!(rows.len(), 2); // score 90 (2 rows) and 75 (1 row)
    }
}
