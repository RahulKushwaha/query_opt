# execution

The execution layer: evaluates physical plans and produces result rows.

## `ExecutionEngine` trait

```rust
pub trait ExecutionEngine {
    fn execute(&self, plan: &PhysicalPlan) -> Result<ResultSet, ExecutionError>;
}
```

A `ResultSet` is `Vec<Vec<Value>>`. Any backend that can walk a `PhysicalPlan` tree and return rows can implement this trait.

## Engines

### `InMemoryEngine`

Operates on in memory `MemoryTable`s (a `HashMap<String, Vec<Vec<Value>>>`). Useful for unit tests and prototyping without disk I/O.

### `RocksEngine` (in the `storage` crate)

The production engine. Reads from RocksDB, supports secondary index scans, and handles aggregation and joins.

## Expression Evaluator

`evaluator::evaluate_expr` evaluates an `Expr` against a single row given a `Schema`:

- `Column(name)`: look up the column index, return the value.
- `Literal(v)`: return the value directly.
- `BinaryExpr`: recursively evaluate both sides, apply the operator (arithmetic, comparison, or logical). NULL propagates through arithmetic and comparisons.
- `AggregateFunction`: not evaluated per row; handled at the engine level.

The evaluator currently contains a `todo!()` stub.
