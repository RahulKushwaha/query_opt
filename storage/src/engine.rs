use crate::RocksStorage;
use execution::engine::{ExecutionEngine, ExecutionError, ResultSet, Row};
use expr::expr::{AggFunc, Expr, Operator};
use expr::logical_plan::JoinType;
use expr::schema::Schema;
use expr::types::Value;
use physical_plan::plan::PhysicalPlan;
use std::collections::HashMap;

/// Execution engine backed by RocksDB storage.
pub struct RocksEngine<'a> {
    pub storage: &'a RocksStorage,
}

impl<'a> RocksEngine<'a> {
    pub fn new(storage: &'a RocksStorage) -> Self {
        Self { storage }
    }
}

impl<'a> ExecutionEngine for RocksEngine<'a> {
    fn execute(&self, plan: &PhysicalPlan) -> Result<ResultSet, ExecutionError> {
        match plan {
            PhysicalPlan::TableScan { table_name, .. } => {
                Ok(self.storage.scan_table(table_name))
            }

            PhysicalPlan::Filter {
                predicate, input, ..
            } => {
                if let Some(rows) = self.try_index_scan(input, predicate) {
                    return Ok(rows);
                }

                let schema = plan_schema(input, self.storage);
                let rows = self.execute(input)?;
                let mut result = Vec::new();
                for row in rows {
                    match eval(predicate, &row, &schema) {
                        Ok(Value::Bool(true)) => result.push(row),
                        _ => {}
                    }
                }
                Ok(result)
            }

            PhysicalPlan::Projection { exprs, input } => {
                let schema = plan_schema(input, self.storage);
                let rows = self.execute(input)?;
                let mut result = Vec::new();
                for row in rows {
                    let mut projected = Vec::new();
                    for e in exprs {
                        projected.push(
                            eval(e, &row, &schema)
                                .map_err(|e| ExecutionError::Internal(e))?,
                        );
                    }
                    result.push(projected);
                }
                Ok(result)
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let left_schema = plan_schema(left, self.storage);
                let right_schema = plan_schema(right, self.storage);
                let left_rows = self.execute(left)?;
                let right_rows = self.execute(right)?;

                let mut combined_fields = left_schema.fields.clone();
                combined_fields.extend(right_schema.fields.clone());
                let combined_schema = Schema::new(combined_fields);

                let mut result = Vec::new();
                let null_right: Row = vec![Value::Null; right_schema.fields.len()];
                let null_left: Row = vec![Value::Null; left_schema.fields.len()];

                match join_type {
                    JoinType::Inner => {
                        for lr in &left_rows {
                            for rr in &right_rows {
                                let mut combined = lr.clone();
                                combined.extend(rr.clone());
                                if let Ok(Value::Bool(true)) =
                                    eval(on, &combined, &combined_schema)
                                {
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
                                if let Ok(Value::Bool(true)) =
                                    eval(on, &combined, &combined_schema)
                                {
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
                                if let Ok(Value::Bool(true)) =
                                    eval(on, &combined, &combined_schema)
                                {
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
                                if let Ok(Value::Bool(true)) =
                                    eval(on, &combined, &combined_schema)
                                {
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

            PhysicalPlan::Sort { exprs, input } => {
                let schema = plan_schema(input, self.storage);
                let mut rows = self.execute(input)?;
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
            } => {
                let schema = plan_schema(input, self.storage);
                let rows = self.execute(input)?;

                let mut groups: HashMap<Vec<u8>, (Row, Vec<Vec<Value>>)> = HashMap::new();
                for row in &rows {
                    let key_vals: Vec<Value> = group_by
                        .iter()
                        .map(|e| eval(e, row, &schema).unwrap_or(Value::Null))
                        .collect();
                    let key_bytes = bincode::serialize(&key_vals).unwrap();
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
                                let vals: Vec<&Value> =
                                    agg_rows.iter().map(|r| &r[ai]).collect();
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
        }
    }
}

impl<'a> RocksEngine<'a> {
    fn try_index_scan(&self, input: &PhysicalPlan, predicate: &Expr) -> Option<Vec<Row>> {
        let table_name = match input {
            PhysicalPlan::TableScan { table_name, .. } => table_name,
            _ => return None,
        };

        let (col, op, val) = match predicate {
            Expr::BinaryExpr { left, op, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v)) => (c.as_str(), op, v),
                _ => return None,
            },
            _ => return None,
        };

        if !self.storage.has_index(table_name, col) {
            return None;
        }

        Some(self.storage.index_scan(table_name, col, op, val))
    }
}

// ── Self-contained expression evaluator ────────────────

fn eval(expr: &Expr, row: &[Value], schema: &Schema) -> Result<Value, String> {
    match expr {
        Expr::Column(name) => {
            let (idx, _) = schema
                .field_by_name(name)
                .ok_or_else(|| format!("column not found: {}", name))?;
            Ok(row[idx].clone())
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryExpr { left, op, right } => {
            let l = eval(left, row, schema)?;
            let r = eval(right, row, schema)?;

            // Null propagation for arithmetic/comparison.
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return match op {
                    Operator::And => match (&l, &r) {
                        (Value::Bool(false), _) | (_, Value::Bool(false)) => {
                            Ok(Value::Bool(false))
                        }
                        _ => Ok(Value::Null),
                    },
                    Operator::Or => match (&l, &r) {
                        (Value::Bool(true), _) | (_, Value::Bool(true)) => {
                            Ok(Value::Bool(true))
                        }
                        _ => Ok(Value::Null),
                    },
                    _ => Ok(Value::Null),
                };
            }

            match op {
                Operator::Plus => arith(&l, &r, |a, b| a + b, |a, b| a + b),
                Operator::Minus => arith(&l, &r, |a, b| a - b, |a, b| a - b),
                Operator::Multiply => arith(&l, &r, |a, b| a * b, |a, b| a * b),
                Operator::Divide => arith(&l, &r, |a, b| a / b, |a, b| a / b),
                Operator::Eq => Ok(Value::Bool(l == r)),
                Operator::NotEq => Ok(Value::Bool(l != r)),
                Operator::Lt => Ok(Value::Bool(cmp_values(&l, &r) == std::cmp::Ordering::Less)),
                Operator::LtEq => Ok(Value::Bool(cmp_values(&l, &r) != std::cmp::Ordering::Greater)),
                Operator::Gt => Ok(Value::Bool(cmp_values(&l, &r) == std::cmp::Ordering::Greater)),
                Operator::GtEq => Ok(Value::Bool(cmp_values(&l, &r) != std::cmp::Ordering::Less)),
                Operator::And => match (&l, &r) {
                    (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
                    _ => Err("AND requires booleans".into()),
                },
                Operator::Or => match (&l, &r) {
                    (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
                    _ => Err("OR requires booleans".into()),
                },
            }
        }
        Expr::AggregateFunction { .. } => {
            Err("aggregate functions cannot be evaluated per-row".into())
        }
    }
}

fn arith(
    l: &Value,
    r: &Value,
    int_op: fn(i64, i64) -> i64,
    float_op: fn(f64, f64) -> f64,
) -> Result<Value, String> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(int_op(*a, *b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_op(*a, *b))),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(float_op(*a as f64, *b))),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(float_op(*a, *b as f64))),
        _ => Err(format!("cannot do arithmetic on {:?} and {:?}", l, r)),
    }
}

fn plan_schema(plan: &PhysicalPlan, storage: &RocksStorage) -> Schema {
    match plan {
        PhysicalPlan::TableScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => plan_schema(input, storage),
        PhysicalPlan::Projection { exprs, input } => {
            let input_schema = plan_schema(input, storage);
            let fields = exprs
                .iter()
                .map(|e| match e {
                    Expr::Column(name) => input_schema
                        .field_by_name(name)
                        .map(|(_, f)| f.clone())
                        .unwrap_or_else(|| {
                            expr::schema::Field::new(name.clone(), expr::types::DataType::Str)
                        }),
                    _ => expr::schema::Field::new(
                        format!("{:?}", e),
                        expr::types::DataType::Str,
                    ),
                })
                .collect();
            Schema::new(fields)
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            let mut fields = plan_schema(left, storage).fields;
            fields.extend(plan_schema(right, storage).fields);
            Schema::new(fields)
        }
        PhysicalPlan::Sort { input, .. } => plan_schema(input, storage),
        PhysicalPlan::HashAggregate {
            group_by,
            aggr_exprs,
            input,
        } => {
            let input_schema = plan_schema(input, storage);
            let mut fields: Vec<expr::schema::Field> = group_by
                .iter()
                .map(|e| match e {
                    Expr::Column(name) => input_schema
                        .field_by_name(name)
                        .map(|(_, f)| f.clone())
                        .unwrap_or_else(|| {
                            expr::schema::Field::new(name.clone(), expr::types::DataType::Str)
                        }),
                    _ => expr::schema::Field::new(
                        format!("{:?}", e),
                        expr::types::DataType::Str,
                    ),
                })
                .collect();
            for e in aggr_exprs {
                fields.push(expr::schema::Field::new(
                    format!("{:?}", e),
                    expr::types::DataType::Int,
                ));
            }
            Schema::new(fields)
        }
    }
}

fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(a), Value::Str(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        // Cross-type: compare by type tag order.
        (Value::Int(a), Value::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Float(a), Value::Int(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn compute_aggregate(fun: &AggFunc, vals: &[&Value]) -> Value {
    match fun {
        AggFunc::Count => Value::Int(vals.iter().filter(|v| ***v != Value::Null).count() as i64),
        AggFunc::Sum => {
            let mut sum = 0i64;
            let mut has_float = false;
            let mut fsum = 0.0f64;
            for v in vals {
                match v {
                    Value::Int(i) => {
                        sum += i;
                        fsum += *i as f64;
                    }
                    Value::Float(f) => {
                        has_float = true;
                        fsum += f;
                    }
                    _ => {}
                }
            }
            if has_float {
                Value::Float(fsum)
            } else {
                Value::Int(sum)
            }
        }
        AggFunc::Min => vals
            .iter()
            .filter(|v| ***v != Value::Null)
            .min_by(|a, b| cmp_values(a, b))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null),
        AggFunc::Max => vals
            .iter()
            .filter(|v| ***v != Value::Null)
            .max_by(|a, b| cmp_values(a, b))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null),
        AggFunc::Avg => {
            let mut sum = 0.0f64;
            let mut count = 0;
            for v in vals {
                match v {
                    Value::Int(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Value::Float(sum / count as f64)
            } else {
                Value::Null
            }
        }
    }
}
