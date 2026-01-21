mod executor;
mod storage;

use executor::QueryExecutor;
use storage::BitcaskStorage;

fn main() -> std::io::Result<()> {
    println!("=== SelfHealDB - SQL Executor Test ===\n");

    let storage = BitcaskStorage::new("sqltest.db")?;
    let mut executor = QueryExecutor::new(storage);

    // Create table
    println!("--- Creating Table ---");
    executor.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")?;

    // Insert data
    println!("\n--- Inserting Data ---");
    for i in 1..=1000 {
        let sql = format!(
            "INSERT INTO users VALUES ({}, 'User{}', {})",
            i,
            i,
            20 + (i % 30)
        );
        executor.execute(&sql)?;
    }
    println!("Inserted 1000 rows");

    // Query without index
    println!("\n--- Query WITHOUT Index ---");
    let result = executor.execute("SELECT * FROM users WHERE id = 500")?;
    println!("Query took: {:?}", result.duration);

    // Create index
    println!("\n--- Creating Index ---");
    executor.storage.create_index("users", "id")?;

    // Query with index
    println!("\n--- Query WITH Index ---");
    let result = executor.execute("SELECT * FROM users WHERE id = 500")?;
    println!("Query took: {:?}", result.duration);

    Ok(())
}
