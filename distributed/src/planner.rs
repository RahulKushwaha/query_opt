use crate::fragment::*;
use crate::partition::PartitionMap;
use physical_plan::plan::PhysicalPlan;
use std::fmt;

#[derive(Debug)]
pub enum PlannerError {
    Unsupported(String),
    Internal(String),
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "Unsupported: {msg}"),
            Self::Internal(msg) => write!(f, "Internal: {msg}"),
        }
    }
}

/// Converts a single-node PhysicalPlan into a DistributedPlan
/// by inserting exchanges where data must move between nodes.
pub struct DistributedPlanner {
    pub partition_map: PartitionMap,
    next_fragment_id: FragmentId,
}

impl DistributedPlanner {
    pub fn new(partition_map: PartitionMap) -> Self {
        Self {
            partition_map,
            next_fragment_id: 0,
        }
    }

    fn next_id(&mut self) -> FragmentId {
        let id = self.next_fragment_id;
        self.next_fragment_id += 1;
        id
    }

    /// Entry point: take a PhysicalPlan and produce a DistributedPlan.
    pub fn plan(&mut self, physical_plan: &PhysicalPlan) -> Result<DistributedPlan, PlannerError> {
        let mut fragments = Vec::new();
        let mut exchanges = Vec::new();

        let root_id = self.plan_node(physical_plan, &mut fragments, &mut exchanges)?;

        Ok(DistributedPlan {
            fragments,
            exchanges,
            root_fragment: root_id,
        })
    }

    /// Recursively walk the physical plan and split into fragments.
    fn plan_node(
        &mut self,
        node: &PhysicalPlan,
        fragments: &mut Vec<PlanFragment>,
        exchanges: &mut Vec<Exchange>,
    ) -> Result<FragmentId, PlannerError> {
        match node {
            PhysicalPlan::TableScan { table_name, .. } => {
                self.plan_scan(table_name, node, fragments)
            }
            PhysicalPlan::Filter { predicate, input } => {
                // TODO: If the input is a TableScan and the predicate constrains the shard key,
                // use partition_map.resolve_shards() to target specific shards.
                // Otherwise, push the filter into the same fragment as its input.
                todo!("plan_filter: decide shard targeting based on predicate")
            }
            PhysicalPlan::Projection { .. } => {
                // TODO: Projections don't change partitioning — keep in the same fragment
                // as the input.
                todo!("plan_projection: inherit fragment from input")
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                on,
                join_type,
            } => self.plan_join(left, right, on, join_type, fragments, exchanges),
            PhysicalPlan::Sort { exprs, input } => {
                self.plan_sort(exprs, input, fragments, exchanges)
            }
            PhysicalPlan::HashAggregate {
                group_by,
                aggr_exprs,
                input,
            } => self.plan_aggregate(group_by, aggr_exprs, input, fragments, exchanges),
            PhysicalPlan::Limit { .. } => {
                // TODO: Push Limit into each shard's fragment (each only needs
                // skip+fetch rows), gather to coordinator, apply Limit again.
                todo!("plan_limit: push per-shard, gather, re-apply")
            }
            PhysicalPlan::SortAggregate { .. } => {
                // TODO: Per-shard partial SortAggregate, gather (preserving
                // sort order), final SortAggregate at coordinator.
                todo!("plan_sort_aggregate: partial per-shard, merge at coordinator")
            }
            PhysicalPlan::ScalarAggregate { .. } => {
                // TODO: Per-shard partial ScalarAggregate, gather, final
                // combine at coordinator (sum-of-sums for SUM/COUNT, etc.).
                todo!("plan_scalar_aggregate: partial per-shard, combine at coordinator")
            }
        }
    }

    /// Scan: create a fragment that runs on the relevant shard(s).
    fn plan_scan(
        &mut self,
        table_name: &str,
        node: &PhysicalPlan,
        fragments: &mut Vec<PlanFragment>,
    ) -> Result<FragmentId, PlannerError> {
        let target = if self.partition_map.tables.contains_key(table_name) {
            FragmentTarget::AllShards
        } else {
            FragmentTarget::Coordinator
        };

        let id = self.next_id();
        fragments.push(PlanFragment {
            id,
            plan: node.clone(),
            target,
        });
        Ok(id)
    }

    /// Join: decide between co-located, broadcast, or repartition.
    fn plan_join(
        &mut self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        _on: &expr::expr::Expr,
        _join_type: &expr::logical_plan::JoinType,
        fragments: &mut Vec<PlanFragment>,
        exchanges: &mut Vec<Exchange>,
    ) -> Result<FragmentId, PlannerError> {
        // TODO: Check if both sides are partitioned on the join key (co-located join).
        //   - If yes: no exchange needed, each shard joins its local data.
        //
        // TODO: If not co-located, estimate sizes to pick a strategy:
        //   - If one side is small: Broadcast it to all shards, join locally.
        //   - Otherwise: Repartition both sides by the join key, then join.
        //
        // TODO: Create the appropriate fragments and exchanges, return the
        //   fragment ID of the final join result.
        todo!("plan_join: co-located vs broadcast vs repartition")
    }

    /// Sort: local sort per shard, then merge-sort at coordinator.
    fn plan_sort(
        &mut self,
        _exprs: &[expr::expr::Expr],
        input: &PhysicalPlan,
        fragments: &mut Vec<PlanFragment>,
        exchanges: &mut Vec<Exchange>,
    ) -> Result<FragmentId, PlannerError> {
        // TODO: 1. Recurse into input to get the input fragment.
        // TODO: 2. Add a Sort node to each shard's fragment (local sort).
        // TODO: 3. Insert a Gather exchange to the coordinator.
        // TODO: 4. Create a coordinator fragment that does a merge-sort
        //          of the pre-sorted streams.
        todo!("plan_sort: local sort + gather + merge-sort")
    }

    /// Aggregate: two-phase — partial on shards, final on coordinator.
    fn plan_aggregate(
        &mut self,
        _group_by: &[expr::expr::Expr],
        _aggr_exprs: &[expr::expr::Expr],
        input: &PhysicalPlan,
        fragments: &mut Vec<PlanFragment>,
        exchanges: &mut Vec<Exchange>,
    ) -> Result<FragmentId, PlannerError> {
        // TODO: 1. Recurse into input to get the input fragment.
        // TODO: 2. Use rewrite::rewrite_partial_aggregate() to create the
        //          shard-local partial aggregate and add it to the shard fragment.
        // TODO: 3. Insert a Gather (or Repartition by group key) exchange.
        // TODO: 4. Use rewrite::rewrite_final_aggregate() to create the
        //          coordinator-side final aggregate fragment.
        todo!("plan_aggregate: partial + exchange + final")
    }
}
