# physical-plan

Defines the physical execution plan and the planner that converts logical plans into physical ones.

## `PhysicalPlan`

The concrete operations an execution engine must interpret:

```rust
enum PhysicalPlan {
    TableScan { table_name, schema },
    Filter { predicate, input },
    Projection { exprs, input },
    NestedLoopJoin { left, right, on, join_type },
    Sort { exprs, input },
    HashAggregate { group_by, aggr_exprs, input },
}
```

Unlike `LogicalPlan`, `PhysicalPlan` specifies *how* to execute. For example, `Join` becomes `NestedLoopJoin` (a future `HashJoin` variant could be added).

## `PhysicalPlanner`

Recursively maps each `LogicalPlan` variant to its physical counterpart:

| Logical | Physical |
|---|---|
| `Scan` | `TableScan` |
| `Filter` | `Filter` |
| `Projection` | `Projection` |
| `Join` | `NestedLoopJoin` |
| `Sort` | `Sort` |
| `Aggregate` | `HashAggregate` |

The planner currently contains a `todo!()` stub. When it is not available, `main.rs` performs the same 1:1 mapping inline as a fallback.
