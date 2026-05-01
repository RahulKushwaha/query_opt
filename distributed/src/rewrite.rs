use expr::expr::{AggFunc, Expr};
use physical_plan::plan::PhysicalPlan;

/// Rewrite an aggregate into its shard-local partial phase.
///
/// For example:
///   - COUNT(x)  → COUNT(x)          (partial count per shard)
///   - SUM(x)    → SUM(x)            (partial sum per shard)
///   - AVG(x)    → SUM(x), COUNT(x)  (need both to compute final avg)
///   - MIN(x)    → MIN(x)
///   - MAX(x)    → MAX(x)
pub fn rewrite_partial_aggregate(
    group_by: &[Expr],
    aggr_exprs: &[Expr],
    input: PhysicalPlan,
) -> PhysicalPlan {
    // TODO: Walk aggr_exprs. For most aggregates, the partial is the same function.
    // Special case: AVG must be split into SUM + COUNT so the final phase can
    // compute SUM(partial_sums) / SUM(partial_counts).
    // Return a PhysicalPlan::HashAggregate with the rewritten expressions.
    todo!("rewrite aggregate into partial (shard-local) phase")
}

/// Rewrite an aggregate into its coordinator-side final merge phase.
///
/// The input to this node is the gathered partial results from all shards.
///
/// For example:
///   - COUNT → SUM(partial_counts)
///   - SUM   → SUM(partial_sums)
///   - AVG   → SUM(partial_sums) / SUM(partial_counts)
///   - MIN   → MIN(partial_mins)
///   - MAX   → MAX(partial_maxes)
pub fn rewrite_final_aggregate(
    group_by: &[Expr],
    aggr_exprs: &[Expr],
    input: PhysicalPlan,
) -> PhysicalPlan {
    // TODO: For each original aggregate, produce the merge expression that
    // combines partial results. COUNT partials are summed, AVG needs
    // SUM/COUNT, etc. Return a PhysicalPlan::HashAggregate over the
    // gathered partial results.
    todo!("rewrite aggregate into final (coordinator) merge phase")
}
