# sql-parser

Converts SQL text into `LogicalPlan` trees (or DDL/DML commands).

## Dependencies

Uses the [`sqlparser`](https://crates.io/crates/sqlparser) crate with `GenericDialect` for tokenizing and parsing.

## Key Types

- **`SqlPlanner`**: holds a catalog (`HashMap<String, Schema>`) and exposes `plan_sql(&str) -> Result<SqlStatement, PlanError>`.
- **`SqlStatement`**: the output of planning:
  - `CreateTable { name, schema }`
  - `CreateIndex { table, column }`
  - `Insert { table, rows }`
  - `Query(LogicalPlan)`

## How It Works

1. `Parser::parse_sql` produces an AST.
2. `plan_statement` dispatches on the AST node type.
3. For `SELECT` queries, `plan_query` walks the AST to build a `LogicalPlan` using `LogicalPlanBuilder`: resolving table references against the catalog, mapping SQL expressions to `Expr`, and layering `Filter`, `Projection`, `Join`, `Sort`, and `Aggregate` nodes.

## Error Types

`PlanError` variants: `Parse`, `Unsupported`, `TableNotFound`, `ColumnNotFound`.
