# Query Pipeline

Every SQL query flows through five stages before results reach the user.

## 1. Parsing

The `sql-parser` crate uses the `sqlparser` library to tokenize and parse SQL text. The `SqlPlanner` converts the parsed AST into one of:

- `SqlStatement::CreateTable` / `CreateIndex` / `Insert` for DDL and DML.
- `SqlStatement::Query(LogicalPlan)` for SELECT statements.

The planner resolves table names against a catalog (`HashMap<String, Schema>`) and maps SQL expressions to the `Expr` enum.

## 2. Logical Optimization

The `Optimizer` accepts a `LogicalPlan` and applies a list of `OptimizerRule` implementations in a fixed point loop (up to 16 passes). Each rule rewrites the plan tree, and the loop terminates early when no rule produces a change.

Categories of rules include:

- **Predicate pushdown**: move filters closer to scans.
- **Projection pruning**: remove unused columns early.
- **Join reordering**: reorder joins using cost estimates.
- **Constant folding / propagation**: evaluate constant expressions at plan time.
- **Aggregate optimization**: merge, eliminate, or push down aggregates.
- **Subquery decorrelation**: convert correlated subqueries to joins.

See the [optimizer crate reference](../crates/optimizer.md) for the full rule list.

## 3. Physical Planning

The `PhysicalPlanner` converts each `LogicalPlan` node to its physical counterpart:

| Logical | Physical |
|---|---|
| `Scan` | `TableScan` |
| `Filter` | `Filter` |
| `Projection` | `Projection` |
| `Join` | `NestedLoopJoin` |
| `Sort` | `Sort` |
| `Aggregate` | `HashAggregate` |

If the physical planner is not yet implemented for a node, `main.rs` falls back to a direct 1:1 mapping.

## 4. Execution

Any type implementing the `ExecutionEngine` trait can execute a `PhysicalPlan`. Two engines exist:

- **`InMemoryEngine`**: operates on in memory tables (useful for testing).
- **`RocksEngine`**: reads from RocksDB, supports index scans when a secondary index exists.

The engine walks the `PhysicalPlan` tree recursively, evaluating expressions with the `evaluate_expr` function.

## 5. Result Display

`main.rs` derives column names from the plan's output schema, computes column widths, and prints a formatted table to stdout.

## EXPLAIN

Prefixing a SELECT with `EXPLAIN` skips execution and instead prints the logical and physical plan trees.
