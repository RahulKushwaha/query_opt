use expr::types::Value;
use std::collections::HashMap;

/// How a table is partitioned across shards.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategy {
    /// Key ranges: shard owns [start, end)
    Range,
    /// Hash of the shard key mod number of shards.
    Hash { num_shards: usize },
}

/// Describes a single shard in the cluster.
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: u64,
    pub address: String,
    /// Inclusive lower bound of the key range (Range partitioning only).
    pub range_start: Option<Value>,
    /// Exclusive upper bound of the key range (Range partitioning only).
    pub range_end: Option<Value>,
}

/// Partitioning metadata for a single table.
#[derive(Debug, Clone)]
pub struct PartitionScheme {
    pub table_name: String,
    /// The column used as the shard key.
    pub shard_key: String,
    pub strategy: PartitionStrategy,
    pub shards: Vec<ShardInfo>,
}

/// Cluster-wide catalog: table name → partitioning info.
#[derive(Debug, Clone, Default)]
pub struct PartitionMap {
    pub tables: HashMap<String, PartitionScheme>,
}

impl PartitionMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, scheme: PartitionScheme) {
        self.tables.insert(scheme.table_name.clone(), scheme);
    }

    /// Return the shards that could contain rows matching a predicate on the shard key.
    /// If the table isn't partitioned or the predicate can't be pruned, returns all shards.
    pub fn resolve_shards(&self, table: &str, predicate: Option<&expr::expr::Expr>) -> Vec<&ShardInfo> {
        let scheme = match self.tables.get(table) {
            Some(s) => s,
            None => return Vec::new(),
        };

        match (&scheme.strategy, predicate) {
            (PartitionStrategy::Range, Some(_pred)) => {
                // TODO: Inspect the predicate. If it constrains the shard key column
                // to a specific value or range, prune shards whose range doesn't overlap.
                // Fall back to returning all shards if the predicate is too complex.
                todo!("prune shards based on range predicate")
            }
            (PartitionStrategy::Hash { .. }, Some(_pred)) => {
                // TODO: If the predicate is an equality on the shard key, hash the value
                // and return only the target shard. Otherwise return all.
                todo!("prune shards based on hash predicate")
            }
            _ => scheme.shards.iter().collect(),
        }
    }
}
