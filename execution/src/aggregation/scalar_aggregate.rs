use expr::expr::Expr;
use expr::schema::Schema;
use expr::types::FieldValue;

use crate::engine::{ExecutionError, ResultSet, Row};
use crate::evaluator::eval;
use crate::helpers::compute_aggregate;

/// Scalar aggregation: no GROUP BY, the entire input is one group.
/// Returns exactly one row. O(n) time, O(1) memory (no hash table).
pub fn execute_scalar_aggregate(
    rows: &[Row],
    schema: &Schema,
    aggr_exprs: &[Expr],
) -> Result<ResultSet, ExecutionError> {
    let mut out_row = Vec::new();
    for agg_expr in aggr_exprs {
        let val = match agg_expr {
            Expr::AggregateFunction { fun, args } => {
                let vals: Vec<FieldValue> = rows
                    .iter()
                    .map(|row| {
                        args.first()
                            .map(|a| eval(a, row, schema).unwrap_or(FieldValue::Null))
                            .unwrap_or(FieldValue::Null)
                    })
                    .collect();
                let refs: Vec<&FieldValue> = vals.iter().collect();
                compute_aggregate(fun, &refs)
            }
            _ => FieldValue::Null,
        };
        out_row.push(val);
    }

    Ok(vec![out_row])
}

#[cfg(test)]
mod tests {
    use super::*;
    use expr::expr::{AggFunc, Expr};
    use expr::schema::Field;
    use expr::types::DataType;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("score", DataType::Int),
        ])
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            vec![FieldValue::Int(1), FieldValue::Int(90)],
            vec![FieldValue::Int(2), FieldValue::Int(75)],
            vec![FieldValue::Int(3), FieldValue::Int(60)],
        ]
    }

    #[test]
    fn test_count() {
        let result = execute_scalar_aggregate(
            &sample_rows(),
            &sample_schema(),
            &[Expr::AggregateFunction {
                fun: AggFunc::Count,
                args: vec![Expr::Column("id".into())],
            }],
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![FieldValue::Int(3)]);
    }

    #[test]
    fn test_sum() {
        let result = execute_scalar_aggregate(
            &sample_rows(),
            &sample_schema(),
            &[Expr::AggregateFunction {
                fun: AggFunc::Sum,
                args: vec![Expr::Column("score".into())],
            }],
        )
        .unwrap();
        assert_eq!(result[0], vec![FieldValue::Int(225)]);
    }

    #[test]
    fn test_min_max() {
        let result = execute_scalar_aggregate(
            &sample_rows(),
            &sample_schema(),
            &[
                Expr::AggregateFunction {
                    fun: AggFunc::Min,
                    args: vec![Expr::Column("score".into())],
                },
                Expr::AggregateFunction {
                    fun: AggFunc::Max,
                    args: vec![Expr::Column("score".into())],
                },
            ],
        )
        .unwrap();
        assert_eq!(result[0], vec![FieldValue::Int(60), FieldValue::Int(90)]);
    }

    #[test]
    fn test_avg() {
        let result = execute_scalar_aggregate(
            &sample_rows(),
            &sample_schema(),
            &[Expr::AggregateFunction {
                fun: AggFunc::Avg,
                args: vec![Expr::Column("score".into())],
            }],
        )
        .unwrap();
        assert_eq!(result[0], vec![FieldValue::Float(75.0)]);
    }

    #[test]
    fn test_empty_input() {
        let result = execute_scalar_aggregate(
            &[],
            &sample_schema(),
            &[Expr::AggregateFunction {
                fun: AggFunc::Count,
                args: vec![Expr::Column("id".into())],
            }],
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![FieldValue::Int(0)]);
    }

    #[test]
    fn test_multiple_aggregates() {
        let result = execute_scalar_aggregate(
            &sample_rows(),
            &sample_schema(),
            &[
                Expr::AggregateFunction {
                    fun: AggFunc::Count,
                    args: vec![Expr::Column("id".into())],
                },
                Expr::AggregateFunction {
                    fun: AggFunc::Sum,
                    args: vec![Expr::Column("score".into())],
                },
                Expr::AggregateFunction {
                    fun: AggFunc::Avg,
                    args: vec![Expr::Column("score".into())],
                },
            ],
        )
        .unwrap();
        assert_eq!(
            result[0],
            vec![FieldValue::Int(3), FieldValue::Int(225), FieldValue::Float(75.0)]
        );
    }
}
