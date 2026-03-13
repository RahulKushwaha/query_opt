// [File 02] Demo binary — full pipeline harness
//
// ┌──────────────────────────────────────────────────────┐
// │ IMPLEMENTATION ORDER: 15 of 15 (last!)               │
// │ Prerequisites: ALL other steps (1-14)                │
// │ This wires everything together for the final demo.   │
// └──────────────────────────────────────────────────────┘
//
// Run with: cargo run
// This wires together all crates to demonstrate the full query optimization pipeline.

use expr::expr::{col, lit, Expr, Operator, AggFunc};
use expr::logical_plan::LogicalPlanBuilder;
use expr::schema::{Field, Schema};
use expr::statistics::Statistics;
use expr::types::{DataType, Value};

fn main() {
    // TODO: Wire the full pipeline:
    //
    // 1. Define schemas and sample data for tables t1(x INT, y INT, z INT) and t2(y INT, w INT)
    //
    // 2. Build a logical plan using LogicalPlanBuilder:
    //    SELECT t1.x, SUM(t1.z)
    //    FROM t1 JOIN t2 ON t1.y = t2.y
    //    WHERE t1.z > 5
    //    GROUP BY t1.x
    //    ORDER BY t1.x
    //
    // 3. Print the original logical plan
    //
    // 4. Create an Optimizer with all rules:
    //    [ConstantFolding, PushDownFilter, PushDownProjection, JoinReorder]
    //    Run optimizer.optimize(plan)
    //
    // 5. Print the optimized logical plan
    //
    // 6. Convert to physical plan using PhysicalPlanner
    //    Print the physical plan
    //
    // 7. Create InMemoryDataStore, register tables with sample data
    //    Create InMemoryEngine, execute the physical plan
    //    Print the results

    println!("Query Optimizer Demo");
    println!("====================");
    println!();
    println!("TODO: Implement the pipeline steps above.");
    println!("Start by implementing the expr crate types (Files 04-12),");
    println!("then work through the optimizer, physical-plan, and execution crates.");
}
