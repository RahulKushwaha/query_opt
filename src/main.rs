use execution::engine::ExecutionEngine;
use expr::schema::Schema;
use expr::types::Value;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sql_parser::{SqlPlanner, SqlStatement};
use std::collections::HashMap;
use storage::engine::RocksEngine;
use storage::{Catalog, RocksStorage, StorageRead, StorageWrite};

fn main() {
    let db_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./query_opt_data".to_string());

    println!("Query Optimizer — Interactive REPL");
    println!("Database path: {}", db_path);
    println!("Type SQL statements. Use EXPLAIN before SELECT to see the query plan.");
    println!("Type .tables to list tables, .schema <table> to see a schema, .quit to exit.");
    println!();

    let mut storage = RocksStorage::new(&db_path);

    // Build initial catalog from persisted metadata.
    let mut catalog: HashMap<String, Schema> = HashMap::new();
    for table in storage.list_tables() {
        if let Some(schema) = storage.get_schema(&table) {
            catalog.insert(table, schema);
        }
    }

    let mut rl = DefaultEditor::new().expect("failed to create editor");

    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                if trimmed == ".quit" || trimmed == ".exit" {
                    break;
                }
                if trimmed == ".tables" {
                    let tables = storage.list_tables();
                    if tables.is_empty() {
                        println!("(no tables)");
                    } else {
                        for t in &tables {
                            println!("  {}", t);
                        }
                    }
                    continue;
                }
                if let Some(table) = trimmed.strip_prefix(".schema") {
                    let table = table.trim();
                    match storage.get_schema(table) {
                        Some(schema) => {
                            for f in &schema.fields {
                                println!("  {} {}", f.name, f.data_type);
                            }
                        }
                        None => println!("Table '{}' not found", table),
                    }
                    continue;
                }

                handle_sql(trimmed, &mut storage, &mut catalog);
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
}

fn handle_sql(sql: &str, storage: &mut RocksStorage, catalog: &mut HashMap<String, Schema>) {
    let is_explain = sql.to_uppercase().starts_with("EXPLAIN ");
    let actual_sql = if is_explain { &sql[8..] } else { sql };

    let planner = SqlPlanner::new(catalog.clone());
    let stmt = match planner.plan_sql(actual_sql) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };

    match stmt {
        SqlStatement::CreateTable { name, schema } => {
            storage.create_table(&name, &schema);
            catalog.insert(name.clone(), schema);
            println!("Table '{}' created.", name);
        }

        SqlStatement::CreateIndex { table, column } => {
            if !catalog.contains_key(&table) {
                eprintln!("Error: table '{}' not found", table);
                return;
            }
            storage.create_index(&table, &column);
            println!("Index on {}.{} created.", table, column);
        }

        SqlStatement::Insert { table, rows } => {
            let count = rows.len();
            let t = storage.get_table(&table).expect("table not found");
            for row in rows {
                storage.insert_row(&t, row);
            }
            println!("Inserted {} row(s) into '{}'.", count, table);
        }

        SqlStatement::Query(logical_plan) => {
            // Try to optimize (may panic with todo!() if not implemented yet).
            let optimized = try_optimize(logical_plan.clone());
            let plan_to_use = optimized.unwrap_or(logical_plan);

            // Try to convert to physical plan.
            let physical = try_physical_plan(&plan_to_use);

            if is_explain {
                println!("Logical Plan:");
                print_logical_plan(&plan_to_use, 0);
                if let Some(ref pp) = physical {
                    println!("\nPhysical Plan:");
                    print_physical_plan(pp, 0);
                } else {
                    println!("\n(Physical planner not yet implemented)");
                }
                return;
            }

            // Execute — try physical plan via RocksEngine first, fall back to direct execution.
            match physical {
                Some(pp) => {
                    let engine = RocksEngine::new(storage);
                    match engine.execute(&pp) {
                        Ok(rows) => print_results(&plan_to_use, &rows),
                        Err(e) => eprintln!("Execution error: {}", e),
                    }
                }
                None => {
                    // Physical planner not implemented — execute logical plan directly.
                    let engine = RocksEngine::new(storage);
                    match execute_logical_directly(&engine, &plan_to_use, storage) {
                        Ok(rows) => print_results(&plan_to_use, &rows),
                        Err(e) => eprintln!("Execution error: {}", e),
                    }
                }
            }
        }
    }
}

fn try_optimize(plan: expr::logical_plan::plan::LogicalPlan) -> Option<expr::logical_plan::plan::LogicalPlan> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        use optimizer::optimizer::Optimizer;
        let opt = Optimizer::new(vec![]);
        opt.optimize(plan).ok()
    }))
    .ok()
    .flatten()
}

fn try_physical_plan(
    plan: &expr::logical_plan::plan::LogicalPlan,
) -> Option<physical_plan::plan::PhysicalPlan> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let planner = physical_plan::planner::PhysicalPlanner::new();
        planner.create_physical_plan(plan).ok()
    }))
    .ok()
    .flatten()
}

/// Direct execution of a LogicalPlan when the PhysicalPlanner is not yet implemented.
/// Builds a PhysicalPlan manually (1:1 mapping) and executes it.
fn execute_logical_directly(
    engine: &RocksEngine<RocksStorage>,
    plan: &expr::logical_plan::plan::LogicalPlan,
    _storage: &RocksStorage,
) -> Result<Vec<Vec<Value>>, execution::engine::ExecutionError> {
    let pp = logical_to_physical(plan);
    engine.execute(&pp)
}

fn logical_to_physical(
    plan: &expr::logical_plan::plan::LogicalPlan,
) -> physical_plan::plan::PhysicalPlan {
    use expr::logical_plan::plan::LogicalPlan;
    use physical_plan::plan::PhysicalPlan;

    match plan {
        LogicalPlan::Scan {
            table_name,
            schema,
        } => PhysicalPlan::TableScan {
            table_name: table_name.clone(),
            schema: schema.clone(),
        },
        LogicalPlan::Filter { predicate, input } => PhysicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Projection { exprs, input } => PhysicalPlan::Projection {
            exprs: exprs.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
        } => PhysicalPlan::NestedLoopJoin {
            left: Box::new(logical_to_physical(left)),
            right: Box::new(logical_to_physical(right)),
            on: on.clone(),
            join_type: join_type.clone(),
        },
        LogicalPlan::Sort { exprs, input } => PhysicalPlan::Sort {
            exprs: exprs.clone(),
            input: Box::new(logical_to_physical(input)),
        },
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            input,
        } => PhysicalPlan::HashAggregate {
            group_by: group_by.clone(),
            aggr_exprs: aggr_exprs.clone(),
            input: Box::new(logical_to_physical(input)),
        },
    }
}

fn print_logical_plan(plan: &expr::logical_plan::plan::LogicalPlan, indent: usize) {
    use expr::logical_plan::plan::LogicalPlan;
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::Scan { table_name, .. } => println!("{}Scan: {}", pad, table_name),
        LogicalPlan::Filter { predicate, input } => {
            println!("{}Filter: {}", pad, predicate);
            print_logical_plan(input, indent + 1);
        }
        LogicalPlan::Projection { exprs, input } => {
            let cols: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
            println!("{}Projection: {}", pad, cols.join(", "));
            print_logical_plan(input, indent + 1);
        }
        LogicalPlan::Join {
            left,
            right,
            on,
            join_type,
            ..
        } => {
            println!("{}Join ({:?}) ON {}", pad, join_type, on);
            print_logical_plan(left, indent + 1);
            print_logical_plan(right, indent + 1);
        }
        LogicalPlan::Sort { exprs, input } => {
            let cols: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
            println!("{}Sort: {}", pad, cols.join(", "));
            print_logical_plan(input, indent + 1);
        }
        LogicalPlan::Aggregate {
            group_by,
            aggr_exprs,
            input,
        } => {
            let groups: Vec<String> = group_by.iter().map(|e| format!("{}", e)).collect();
            let aggs: Vec<String> = aggr_exprs.iter().map(|e| format!("{}", e)).collect();
            println!(
                "{}Aggregate: group_by=[{}], aggs=[{}]",
                pad,
                groups.join(", "),
                aggs.join(", ")
            );
            print_logical_plan(input, indent + 1);
        }
    }
}

fn print_physical_plan(plan: &physical_plan::plan::PhysicalPlan, indent: usize) {
    use physical_plan::plan::PhysicalPlan;
    let pad = "  ".repeat(indent);
    match plan {
        PhysicalPlan::TableScan { table_name, .. } => {
            println!("{}TableScan: {}", pad, table_name)
        }
        PhysicalPlan::Filter { predicate, input } => {
            println!("{}Filter: {}", pad, predicate);
            print_physical_plan(input, indent + 1);
        }
        PhysicalPlan::Projection { exprs, input } => {
            let cols: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
            println!("{}Projection: {}", pad, cols.join(", "));
            print_physical_plan(input, indent + 1);
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            on,
            join_type,
        } => {
            println!("{}NestedLoopJoin ({:?}) ON {}", pad, join_type, on);
            print_physical_plan(left, indent + 1);
            print_physical_plan(right, indent + 1);
        }
        PhysicalPlan::Sort { exprs, input } => {
            let cols: Vec<String> = exprs.iter().map(|e| format!("{}", e)).collect();
            println!("{}Sort: {}", pad, cols.join(", "));
            print_physical_plan(input, indent + 1);
        }
        PhysicalPlan::HashAggregate {
            group_by,
            aggr_exprs,
            input,
        } => {
            let groups: Vec<String> = group_by.iter().map(|e| format!("{}", e)).collect();
            let aggs: Vec<String> = aggr_exprs.iter().map(|e| format!("{}", e)).collect();
            println!(
                "{}HashAggregate: group_by=[{}], aggs=[{}]",
                pad,
                groups.join(", "),
                aggs.join(", ")
            );
            print_physical_plan(input, indent + 1);
        }
    }
}

fn print_results(plan: &expr::logical_plan::plan::LogicalPlan, rows: &[Vec<Value>]) {
    if rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Derive column names from the plan schema.
    let schema = plan.schema();
    let headers: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();

    // Compute column widths.
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    // Ensure widths vec matches actual row width (may differ if schema derivation is off).
    while widths.len() < rows.first().map(|r| r.len()).unwrap_or(0) {
        widths.push(3);
    }
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(format!("{}", val).len());
            }
        }
    }

    // Print header.
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:width$}", h, width = widths.get(i).copied().unwrap_or(3)))
        .collect();
    println!(" {} ", header_line.join(" | "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("-{}-", sep.join("-+-"));

    // Print rows.
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                format!(
                    "{:width$}",
                    format!("{}", v),
                    width = widths.get(i).copied().unwrap_or(3)
                )
            })
            .collect();
        println!(" {} ", cells.join(" | "));
    }
    println!("({} rows)", rows.len());
}
