mod storage;

use storage::{Column, ColumnType, Row, TableSchema, Value};

fn main() {
    // Create a table schema
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                column_type: ColumnType::Integer
            },
            Column{
                name:"name".to_string(),
                column_type: ColumnType::Text
            },
            Column{
                name:"age".to_string(),
                column_type: ColumnType::Integer
            }
        ]
    );
    println!("Created schema: {:?}", schema);

    // Create a valid row
    let row1 = Row::new(vec![
        Value::Integer(1),
        Value::Text("Vaibhav".to_string()),
        Value::Integer(23),
    ]);

    match schema.validate_row(&row1) {
        Ok(_) => println!("Row 1 is valid"),
        Err(e) => println!("Row 1 invalid: {}", e),
    }

    let row2 = Row::new(vec![
            Value::Integer(2),
            Value::Integer(999), // Should be Text!
            Value::Integer(25),
        ]);

    match schema.validate_row(&row2) {
        Ok(_) => println!("Row 2 is valid"),
        Err(e) => println!("Row 2 invalid: {}", e),
    }

    // Test column lookup
    if let Some(idx) = schema.get_column_index("name") {
        println!("Column 'name' is at index: {}", idx);
    }

    // 1. Let's create a valid row
    let valid_row = Row::new(vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Integer(30),
    ]);

    // 2. Let's use the 'get' and 'to_string' methods the compiler warned about
    if let Some(value) = valid_row.get(1) { // Get the "name" column
        println!("The user's name is: {}", value.to_string());
    }

    // 3. Let's use the 'get_column' method
    if let Some(col) = schema.get_column("age") {
        println!("The 'age' column type is: {:?}", col.column_type);
    }
}
