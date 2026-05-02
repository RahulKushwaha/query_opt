use std::collections::HashMap;

use expr::expr::Expr;
use expr::schema::Schema;
use expr::types::FieldValue;

use crate::engine::{ExecutionError, ResultSet, Row};
use crate::evaluator::eval;
use crate::helpers::compute_aggregate;

/// Hash-based aggregation: builds a hash table keyed by group_by columns.
/// Best for unsorted input with moderate group cardinality.
pub fn execute_hash_aggregate(
    rows: &[Row],
    schema: &Schema,
    group_by: &[Expr],
    aggr_exprs: &[Expr],
) -> Result<ResultSet, ExecutionError> {
    let mut groups: HashMap<Vec<FieldValue>, Vec<Vec<FieldValue>>> = HashMap::new();
    for row in rows {
        let key_vals: Vec<FieldValue> = group_by
            .iter()
            .map(|e| eval(e, row, schema).unwrap_or(FieldValue::Null))
            .collect();
        let entry = groups.entry(key_vals).or_insert_with(Vec::new);
        let agg_inputs: Vec<FieldValue> = aggr_exprs
            .iter()
            .map(|e| match e {
                Expr::AggregateFunction { args, .. } => args
                    .first()
                    .map(|a| eval(a, row, schema).unwrap_or(FieldValue::Null))
                    .unwrap_or(FieldValue::Null),
                _ => eval(e, row, schema).unwrap_or(FieldValue::Null),
            })
            .collect();
        entry.push(agg_inputs);
    }

    let mut result = Vec::new();
    for (group_vals, agg_rows) in groups {
        let mut out_row = group_vals;
        for (ai, agg_expr) in aggr_exprs.iter().enumerate() {
            let val = match agg_expr {
                Expr::AggregateFunction { fun, .. } => {
                    let vals: Vec<&FieldValue> = agg_rows.iter().map(|r| &r[ai]).collect();
                    compute_aggregate(fun, &vals)
                }
                _ => FieldValue::Null,
            };
            out_row.push(val);
        }
        result.push(out_row);
    }
    Ok(result)
}
