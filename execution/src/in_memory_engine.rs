// [File 30] InMemoryEngine — implements ExecutionEngine trait
//
// ┌──────────────────────────────────────────────────────┐
// │ IMPLEMENTATION ORDER: 14 of 15                       │
// │ Prerequisites: execution/src/evaluator.rs (step 13), │
// │                execution/src/engine.rs,              │
// │                execution/src/memory_table.rs         │
// │ Next: src/main.rs (step 15)                          │
// └──────────────────────────────────────────────────────┘
//
// DataFusion ref: datafusion/physical-plan/src/ (various operator implementations)

use crate::engine::{ExecutionEngine, ExecutionError, ResultSet};
use crate::evaluator::evaluate_expr;
use crate::memory_table::InMemoryDataStore;
use physical_plan::plan::PhysicalPlan;

/// A simple row-based in-memory execution engine.
///
/// This is the reference implementation of ExecutionEngine.
/// Your future custom engine will implement the same trait.
pub struct InMemoryEngine {
    pub data_store: InMemoryDataStore,
}

impl InMemoryEngine {
    pub fn new(data_store: InMemoryDataStore) -> Self {
        Self { data_store }
    }
}

impl ExecutionEngine for InMemoryEngine {
    fn execute(&self, plan: &PhysicalPlan) -> Result<ResultSet, ExecutionError> {
        // TODO: Recursively execute each PhysicalPlan node:
        //
        // TableScan:
        //   Look up the table in self.data_store. Return all rows.
        //   Error if table not found.
        //
        // Filter:
        //   Execute the child plan to get rows.
        //   For each row, evaluate the predicate using evaluate_expr.
        //   Keep rows where predicate evaluates to Bool(true).
        //
        // Projection:
        //   Execute the child plan to get rows.
        //   For each row, evaluate each projection expression to produce a new row.
        //
        // NestedLoopJoin:
        //   Execute left and right children.
        //   For each (left_row, right_row) pair, concatenate them and evaluate
        //   the join condition. Keep matching pairs.
        //   Handle join_type: Inner keeps only matches.
        //   (Left/Right/Full outer joins: include non-matching rows with NULLs)
        //
        // Sort:
        //   Execute the child plan.
        //   Sort rows by evaluating sort expressions and comparing values.
        //
        // HashAggregate:
        //   Execute the child plan.
        //   Group rows by evaluating group_by expressions (use as HashMap key).
        //   For each group, compute aggregate functions (Count, Sum, Min, Max, Avg).
        //   Return one row per group: [group_key_values..., aggregate_results...].
        todo!("implement in-memory execution engine")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_scan() {
        // TODO: Register a table, execute a TableScan, verify all rows returned
        todo!()
    }

    #[test]
    fn test_execute_filter() {
        // TODO: Execute Filter(TableScan) with a predicate, verify only matching rows
        todo!()
    }

    #[test]
    fn test_execute_join() {
        // TODO: Register two tables, execute NestedLoopJoin, verify joined rows
        todo!()
    }

    #[test]
    fn test_execute_aggregate() {
        // TODO: Execute HashAggregate with GROUP BY and SUM, verify grouped results
        todo!()
    }

    #[test]
    fn test_execute_sort() {
        // TODO: Execute Sort, verify rows are in expected order
        todo!()
    }
}
