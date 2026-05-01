use expr::expr::{AggFunc, Expr};
use expr::schema::{Field, Schema};
use expr::types::{DataType, Value};
use physical_plan::plan::PhysicalPlan;

use crate::evaluator::cmp_values;

/// Derive the output schema of a physical plan node.
pub fn plan_schema(plan: &PhysicalPlan) -> Schema {
    match plan {
        PhysicalPlan::TableScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => plan_schema(input),
        PhysicalPlan::Projection { exprs, input } => {
            let input_schema = plan_schema(input);
            let fields = exprs
                .iter()
                .map(|e| match e {
                    Expr::Column(name) => input_schema
                        .field_by_name(name)
                        .map(|(_, f)| f.clone())
                        .unwrap_or_else(|| Field::new(name.clone(), DataType::Str)),
                    _ => Field::new(format!("{:?}", e), DataType::Str),
                })
                .collect();
            Schema::new(fields)
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            let mut fields = plan_schema(left).fields;
            fields.extend(plan_schema(right).fields);
            Schema::new(fields)
        }
        PhysicalPlan::Sort { input, .. } => plan_schema(input),
        PhysicalPlan::HashAggregate {
            group_by,
            aggr_exprs,
            input,
        } => {
            let input_schema = plan_schema(input);
            let mut fields: Vec<Field> = group_by
                .iter()
                .map(|e| match e {
                    Expr::Column(name) => input_schema
                        .field_by_name(name)
                        .map(|(_, f)| f.clone())
                        .unwrap_or_else(|| Field::new(name.clone(), DataType::Str)),
                    _ => Field::new(format!("{:?}", e), DataType::Str),
                })
                .collect();
            for e in aggr_exprs {
                fields.push(Field::new(format!("{:?}", e), DataType::Int));
            }
            Schema::new(fields)
        }
    }
}

/// Compute an aggregate function over a slice of values.
pub fn compute_aggregate(fun: &AggFunc, vals: &[&Value]) -> Value {
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
