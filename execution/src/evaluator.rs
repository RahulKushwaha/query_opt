// [File 29] Expression evaluator — evaluate an Expr against a row
//
// ┌──────────────────────────────────────────────────────┐
// │ IMPLEMENTATION ORDER: 13 of 15                       │
// │ Prerequisites: expr/src/expr.rs (step 3),            │
// │                expr/src/schema.rs (step 2)           │
// │ Next: execution/src/in_memory_engine.rs (step 14)    │
// └──────────────────────────────────────────────────────┘
//
// This is used by the in-memory engine to evaluate filter predicates,
// projection expressions, join conditions, etc.

use expr::expr::Expr;
use expr::schema::Schema;
use expr::types::Value;

/// Evaluate an expression against a single row, using the schema to resolve column references.
///
/// Returns the resulting Value.
pub fn evaluate_expr(expr: &Expr, row: &[Value], schema: &Schema) -> Result<Value, String> {
    // TODO: Match on the Expr variant and compute the result:
    //
    // Column(name):
    //   Look up the column index in the schema using schema.field_by_name(name).
    //   Return row[index].clone(). Error if column not found.
    //
    // Literal(value):
    //   Return value.clone() directly.
    //
    // BinaryExpr { left, op, right }:
    //   Recursively evaluate left and right.
    //   Apply the operator:
    //     Arithmetic (Plus, Minus, Multiply, Divide): operate on Int/Float values
    //     Comparison (Eq, NotEq, Lt, LtEq, Gt, GtEq): compare values, return Bool
    //     Logical (And, Or): operate on Bool values
    //   Handle type mismatches by returning Err.
    //   Handle Null propagation: any arithmetic/comparison with Null returns Null.
    //
    // AggregateFunction:
    //   This shouldn't be evaluated per-row — return Err indicating aggregates
    //   are handled at the engine level, not the expression evaluator.
    todo!("implement expression evaluation against a row")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_column_ref() {
        // TODO: Create a schema and row, evaluate a Column expr,
        // verify it returns the correct value from the row
        todo!()
    }

    #[test]
    fn test_evaluate_binary_comparison() {
        // TODO: Evaluate `x > 5` against a row where x=10, verify result is Bool(true)
        todo!()
    }

    #[test]
    fn test_evaluate_arithmetic() {
        // TODO: Evaluate `x + y` against a row, verify correct Int result
        todo!()
    }

    #[test]
    fn test_evaluate_null_propagation() {
        // TODO: Evaluate `x + NULL`, verify result is Null
        todo!()
    }
}
