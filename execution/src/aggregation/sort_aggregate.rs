use expr::expr::Expr;
use expr::schema::Schema;

use crate::engine::{ExecutionError, ResultSet, Row};

/// Sort-based aggregation: assumes input is already sorted by group_by columns.
/// Scans sequentially, emitting a result each time the group key changes.
/// O(n) time, O(1) memory beyond the input.
pub fn execute_sort_aggregate(
    rows: &[Row],
    schema: &Schema,
    group_by: &[Expr],
    aggr_exprs: &[Expr],
) -> Result<ResultSet, ExecutionError> {
    todo!("implement sort aggregate")
}
