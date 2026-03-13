// [File 23] PhysicalPlan enum
//
// DataFusion ref: datafusion/physical-plan/src/execution_plan.rs
//
// This is the contract between the optimizer and any execution engine.
// A custom engine only needs to interpret these nodes.

use expr::expr::Expr;
use expr::logical_plan::JoinType;
use expr::schema::Schema;

/// A physical execution plan — the concrete operations an engine must perform.
/// Unlike LogicalPlan, this specifies *how* to execute (e.g., NestedLoopJoin vs HashJoin).
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    /// Scan rows from a named table.
    TableScan {
        table_name: String,
        schema: Schema,
    },
    /// Filter rows using a predicate expression.
    Filter {
        predicate: Expr,
        input: Box<PhysicalPlan>,
    },
    /// Project (select) specific expressions from the input.
    Projection {
        exprs: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    /// Join two inputs using a nested loop strategy.
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Expr,
        join_type: JoinType,
    },
    /// Sort the input by the given expressions.
    Sort {
        exprs: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
    /// Hash-based aggregation: group by keys and compute aggregates.
    HashAggregate {
        group_by: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        input: Box<PhysicalPlan>,
    },
}
