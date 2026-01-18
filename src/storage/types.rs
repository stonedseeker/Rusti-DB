use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Represents the data type of a column
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    Integer, 
    Text, 
    Float,
}

// Represents a column definition in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String, 
    pub column_type: ColumnType,
}

// Represents a single value in a row
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Integer(164),
    Text(String),
    Float(f64),
    Null,
}

impl Value {
    // Convert value to string for display
    pub fn to_string(&self) -> String {
        match self {
            Value::Integer(i) => i.to_string(),
            Value::Text(s) => s.clone(),
            Value::Float(f) => f.to_string(f),
            Value::Null => "Null".to_string(),
        }
    }
    
    // Check if this value matches the column type
    pub fn matches_type(&self, col_type: &ColumnType) -> bool {
        match (self, col_type) {
            (Value::Integer(_), ColumnType::Integer) => true,
            (Value::Text(_), ColumnType::Text) => true,
            (Value::Float(_), ColumnType::Float) => true, 
            (Value::Null(_), _) => true,
            _ => false,
        }
    }
    
}

// Represent a single row of data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row { 
    pub fn new(values: Vec<Value>) -> Self {
        Row { values }
    }
    
    // Get a value by column index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
    // Get a mutable value by column index 
    pub fn get_mut(&self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String, 
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(name:String, columns: Vec<Column>) -> Self {
        TableSchema {name, columns}
    }
    
    // Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
    
    // Get column by name 
    pub fn get_column(&self , name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
    
    // Validate that a row matches this schema
    pub fn validate_row(&self, row: &Row) -> Result<(), String> {
        if row.values.len() != self.columns.len() {
            return Err(format!(
                "Row has {} values but schema expects {}",
                row.values.len(),
                self.columns.len()
            ));
        }
        
        for (i, (value, column)) in row.values.iter().zip(self.columns.iter()).enumerate() {
            if !value.matches_type(&column.column_type) {
                return Err(format!(
                    "Column {} (index {}) expects {:?} but got {:?}",
                    column.name, i, column.column_type, value
                ));
            }
        }
        
        Ok(())
    }
}

// Represents an in-memory index for fast lookups
#[derive(Debug, Clone)]
pub struct Index {
    pub table_name: String,
    pub column_name: String,
    pub column_index: usize,
    // Maps value -> list of row offsets in the file
    pub index_map: HashMap<String, Vec<u64>>,
}

impl Index {
    pub fn new(table_name: String, column_name: String, column_index: usize) {
        Index {
            table_name,
            column_name,
            column_index,
            index_map: HashMap::new(),
        }
    }
    
    pub fn insert(&mut self, value: &Value, offset:u64) {
        let key = value.to_string();
        self.index_map.entry(key).or_insert_with(Vec::new).push(offset);
    }
    
    pub fn lookup(&self, value: &Value) -> Option<&Vec<u64>> {
        let key = value.to_string();
        self.index_map.get(&key)
    }
}