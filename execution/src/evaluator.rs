use std::cmp::Ordering;

use expr::expr::{Expr, Operator};
use expr::schema::Schema;
use expr::types::Value;

/// Evaluate an expression against a single row.
pub fn eval(expr: &Expr, row: &[Value], schema: &Schema) -> Result<Value, String> {
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
                Operator::Lt => Ok(Value::Bool(cmp_values(&l, &r) == Ordering::Less)),
                Operator::LtEq => Ok(Value::Bool(cmp_values(&l, &r) != Ordering::Greater)),
                Operator::Gt => Ok(Value::Bool(cmp_values(&l, &r) == Ordering::Greater)),
                Operator::GtEq => Ok(Value::Bool(cmp_values(&l, &r) != Ordering::Less)),
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

pub fn arith(
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

pub fn cmp_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Str(a), Value::Str(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int(a), Value::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Value::Float(a), Value::Int(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expr::schema::Field;
    use expr::types::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("x", DataType::Int),
            Field::new("y", DataType::Int),
        ])
    }

    #[test]
    fn eval_column_ref() {
        let row = vec![Value::Int(10), Value::Int(20)];
        let result = eval(&Expr::Column("x".into()), &row, &test_schema()).unwrap();
        assert_eq!(result, Value::Int(10));
    }

    #[test]
    fn eval_binary_comparison() {
        let row = vec![Value::Int(10), Value::Int(5)];
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column("x".into())),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(Value::Int(5))),
        };
        assert_eq!(eval(&expr, &row, &test_schema()).unwrap(), Value::Bool(true));
    }

    #[test]
    fn eval_arithmetic() {
        let row = vec![Value::Int(3), Value::Int(7)];
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column("x".into())),
            op: Operator::Plus,
            right: Box::new(Expr::Column("y".into())),
        };
        assert_eq!(eval(&expr, &row, &test_schema()).unwrap(), Value::Int(10));
    }

    #[test]
    fn eval_null_propagation() {
        let row = vec![Value::Int(3), Value::Null];
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column("x".into())),
            op: Operator::Plus,
            right: Box::new(Expr::Column("y".into())),
        };
        assert_eq!(eval(&expr, &row, &test_schema()).unwrap(), Value::Null);
    }
}
