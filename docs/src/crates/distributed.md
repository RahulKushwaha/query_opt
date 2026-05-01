# distributed

Distributed query planning: splits a single node physical plan into fragments that execute across a cluster of shards.

## Concepts

### Partition Map

`PartitionMap` is the cluster wide catalog mapping table names to their `PartitionScheme`. Each scheme records:

- The **shard key** column.
- The **strategy**: `Range` (key range per shard) or `Hash` (hash mod N).
- A list of **`ShardInfo`** entries with shard ID, network address, and optional range bounds.

`resolve_shards(table, predicate)` prunes shards that cannot contain matching rows. For range partitioning it checks range overlap; for hash partitioning it hashes equality predicates on the shard key.

### Plan Fragments

A `PlanFragment` is a piece of the distributed plan that runs as a unit on one node. Each fragment has:

- A `PhysicalPlan` subtree.
- A `FragmentTarget`: `Shard(id)`, `AllShards`, or `Coordinator`.

### Exchanges

An `Exchange` is a network boundary between two fragments. Types:

| Type | Behavior |
|---|---|
| `Gather` | All shards send results to the coordinator (fan in) |
| `Repartition { key_column }` | Reshuffle rows by hashing a key so same key rows land on the same shard |
| `Broadcast` | Send the full result to every shard |

### Distributed Plan

`DistributedPlan` is a DAG of `PlanFragment`s connected by `Exchange`s, with a `root_fragment` that produces the final result on the coordinator.

## `DistributedPlanner`

Walks a `PhysicalPlan` and splits it into fragments:

- **Scan**: creates a fragment targeting all shards (or the coordinator for unpartitioned tables).
- **Filter**: if the predicate constrains the shard key, targets specific shards.
- **Join**: chooses between co located join (no exchange), broadcast join (small side), or repartition join.
- **Sort**: local sort per shard, gather exchange, merge sort on coordinator.
- **Aggregate**: two phase: partial aggregate on shards, gather exchange, final aggregate on coordinator.

## Rewrite Helpers

`rewrite.rs` provides `rewrite_partial_aggregate` and `rewrite_final_aggregate` for splitting aggregates into shard local and coordinator phases.

## Coordinator

`coordinator.rs` defines the `Coordinator` struct that dispatches fragments to shard workers and collects results. Currently a `todo!()` stub.
