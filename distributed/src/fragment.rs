use physical_plan::plan::PhysicalPlan;

/// Unique identifier for a plan fragment.
pub type FragmentId = u64;

/// How data moves between fragments across the network.
#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeType {
    /// All shards send their results to the coordinator (fan-in).
    Gather,
    /// Reshuffle rows across shards by hashing a key column so that
    /// rows with the same key land on the same shard.
    Repartition { key_column: String },
    /// Send the full result of one fragment to every shard.
    Broadcast,
}

/// A network boundary between two plan fragments.
#[derive(Debug, Clone)]
pub struct Exchange {
    pub exchange_type: ExchangeType,
    /// Fragment that produces the data.
    pub input_fragment: FragmentId,
    /// Fragment that consumes the data.
    pub output_fragment: FragmentId,
}

/// Where a fragment should execute.
#[derive(Debug, Clone, PartialEq)]
pub enum FragmentTarget {
    /// Run on a specific shard.
    Shard(u64),
    /// Run on every shard that owns data for the relevant table.
    AllShards,
    /// Run on the coordinator node.
    Coordinator,
}

/// A piece of the distributed plan that executes as a unit on one node.
#[derive(Debug, Clone)]
pub struct PlanFragment {
    pub id: FragmentId,
    pub plan: PhysicalPlan,
    pub target: FragmentTarget,
}

/// The complete distributed execution plan: a DAG of fragments connected by exchanges.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<Exchange>,
    /// The fragment that produces the final result (always runs on the coordinator).
    pub root_fragment: FragmentId,
}
