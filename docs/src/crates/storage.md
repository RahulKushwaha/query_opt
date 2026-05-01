# storage

RocksDB backed persistent storage layer.

## `RocksStorage`

The main storage struct. Opens a RocksDB database at a given path and manages tables as column families.

### Table Operations

- `create_table(name, schema)`: creates a column family and persists schema metadata in the `_meta` column family.
- `insert_row(table, row)`: assigns an auto incrementing row ID, serializes the row with `bincode`, and writes it. Also updates any secondary indexes.
- `scan_table(table)`: full table scan, iterating all rows in the column family.
- `get_schema(table)`: returns the `Schema` from the metadata cache.
- `list_tables()`: returns all registered table names.

### Secondary Indexes

- `create_index(table, column)`: creates a dedicated column family (`idx_{table}_{column}`) and backfills from existing rows.
- `has_index(table, column)`: checks if an index exists.
- `index_scan(table, column, op, value)`: scans the index column family for keys matching the condition, then point looks up the matching rows.

Index keys are encoded for correct byte ordering: a type prefix byte, the value in sort preserving binary form (sign flipped integers, IEEE 754 ordered floats, null terminated strings), and the row ID suffix.

### Range Scans

`scan_range(table, start_key, end_key)` reads rows within a key range. Used by the distributed layer for shard local reads. Currently a `todo!()` stub.

## `RocksEngine`

Implements `ExecutionEngine` for `RocksStorage`. Walks the `PhysicalPlan` tree:

- **TableScan**: delegates to `scan_table`.
- **Filter**: attempts an index scan first (if a secondary index exists on the filtered column); falls back to a full scan with predicate evaluation.
- **Projection**: evaluates each expression per row.
- **NestedLoopJoin**: nested loop with predicate evaluation.
- **Sort / HashAggregate**: in memory sort and hash based grouping.
