use crate::fragment::*;
use crate::partition::PartitionMap;
use crate::planner::{DistributedPlanner, PlannerError};
use execution::engine::{ExecutionError, ResultSet};
use expr::types::Value;
use physical_plan::plan::PhysicalPlan;
use std::fmt;

#[derive(Debug)]
pub enum CoordinatorError {
    Planning(PlannerError),
    Execution(ExecutionError),
    Network(String),
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planning(e) => write!(f, "Planning: {e}"),
            Self::Execution(e) => write!(f, "Execution: {e}"),
            Self::Network(msg) => write!(f, "Network: {msg}"),
        }
    }
}

/// Orchestrates distributed query execution.
///
/// Takes a PhysicalPlan, splits it into fragments via DistributedPlanner,
/// dispatches fragments to shards, and merges results.
pub struct Coordinator {
    pub partition_map: PartitionMap,
}

impl Coordinator {
    pub fn new(partition_map: PartitionMap) -> Self {
        Self { partition_map }
    }

    /// Plan and execute a query across the cluster.
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<ResultSet, CoordinatorError> {
        let mut planner = DistributedPlanner::new(self.partition_map.clone());
        let distributed_plan = planner.plan(plan).map_err(CoordinatorError::Planning)?;

        self.execute_distributed(&distributed_plan)
    }

    /// Execute a DistributedPlan by dispatching fragments and collecting results.
    fn execute_distributed(&self, plan: &DistributedPlan) -> Result<ResultSet, CoordinatorError> {
        // TODO: Topologically sort fragments by their exchange dependencies.
        // TODO: For each fragment:
        //   - If target is Shard(id) or AllShards: dispatch to the remote shard(s)
        //     and collect results.
        //   - If target is Coordinator: execute locally.
        // TODO: For each exchange, move data between fragments:
        //   - Gather: concatenate results from all shards.
        //   - Broadcast: send one fragment's result to all shards.
        //   - Repartition: hash rows by key and route to the correct shard.
        // TODO: Execute the root fragment and return its result.
        todo!("execute distributed plan across shards")
    }

    /// Merge pre-sorted streams from multiple shards into a single sorted result.
    fn merge_sorted(&self, streams: Vec<ResultSet>, _sort_exprs: &[expr::expr::Expr]) -> ResultSet {
        // TODO: Use a k-way merge (e.g., min-heap) over the pre-sorted streams.
        // Each stream is already sorted by sort_exprs; produce a single sorted output.
        todo!("k-way merge sort of pre-sorted shard results")
    }

    /// Dispatch a fragment to a remote shard and return the result.
    fn dispatch_to_shard(
        &self,
        _shard_id: u64,
        _fragment: &PlanFragment,
    ) -> Result<ResultSet, CoordinatorError> {
        // TODO: Serialize the fragment's PhysicalPlan, send it to the shard's address
        // (from partition_map), wait for the result, deserialize and return.
        // For now this is a placeholder — real implementation needs a network layer.
        todo!("send fragment to remote shard and collect results")
    }
}
