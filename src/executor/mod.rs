use crate::storage::{BitcaskStorage, Column, ColumnType, Row, TableSchema, Value};
use sqlparser::ast::{Expr, Query, Select, SetExpr, Statement, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::io;
use std::time::Instant;

pub struct QueryExecutor {
    pub storage: BitcaskStorage,
}

impl QueryExecutor {
    pub fn new(storage: BitcaskStorage) -> Self {
        QueryExecutor { storage }
    }

    /// Execute a SQL query string
    pub fn execute(&mut self, sql: &str) -> io::Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("Parse error: {}", e))
        })?;

        if ast.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No SQL statement found",
            ));
        }

        // Execute the first statement
        let result = match &ast[0] {
            Statement::CreateTable(create_table) => self.execute_create_table(create_table)?,
            Statement::Insert(insert) => self.execute_insert(insert)?,
            Statement::Query(query) => self.execute_query(query)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unsupported SQL statement",
                ))
            }
        };

        let duration = start.elapsed();

        Ok(QueryResult {
            result,
            duration,
            used_index: false, // We'll update this later
        })
    }

    fn execute_create_table(
        &mut self,
        create_table: &sqlparser::ast::CreateTable,
    ) -> io::Result<ExecutionResult> {
        let table_name = create_table.name.to_string();
        let mut columns = Vec::new();

        for col_def in &create_table.columns {
            let col_name = col_def.name.to_string();
            let col_type = match col_def.data_type {
                sqlparser::ast::DataType::Int(_)
                | sqlparser::ast::DataType::Integer(_)
                | sqlparser::ast::DataType::BigInt(_)
                | sqlparser::ast::DataType::SmallInt(_) => ColumnType::Integer,
                sqlparser::ast::DataType::Text
                | sqlparser::ast::DataType::Varchar(_)
                | sqlparser::ast::DataType::Char(_) => ColumnType::Text,
                sqlparser::ast::DataType::Float(_)
                | sqlparser::ast::DataType::Real
                | sqlparser::ast::DataType::Double => ColumnType::Float,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Unsupported data type: {:?}", col_def.data_type),
                    ))
                }
            };

            columns.push(Column {
                name: col_name,
                column_type: col_type,
            });
        }

        let schema = TableSchema::new(table_name, columns);
        self.storage.create_table(schema)?;

        Ok(ExecutionResult::Created)
    }

    fn execute_insert(&mut self, insert: &sqlparser::ast::Insert) -> io::Result<ExecutionResult> {
        let table_name = insert.table_name.to_string();

        let values = if let Some(source) = &insert.source {
            match &source.body.as_ref() {
                SetExpr::Values(values) => &values.rows,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Unsupported INSERT syntax",
                    ))
                }
            }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No values in INSERT",
            ));
        };

        let mut inserted_count = 0;

        for value_row in values {
            let mut row_values = Vec::new();

            for expr in value_row {
                let value = self.expr_to_value(expr)?;
                row_values.push(value);
            }

            let row = Row::new(row_values);
            self.storage.insert(&table_name, row)?;
            inserted_count += 1;
        }

        Ok(ExecutionResult::Inserted(inserted_count))
    }

    fn execute_query(&mut self, query: &Query) -> io::Result<ExecutionResult> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unsupported query type",
                ))
            }
        };

        self.execute_select(select)
    }

    fn execute_select(&mut self, select: &Select) -> io::Result<ExecutionResult> {
        // Get table name
        if select.from.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No table specified",
            ));
        }

        let table_name = select.from[0].relation.to_string();

        // Get rows based on WHERE clause
        let rows = if let Some(ref where_clause) = select.selection {
            self.execute_where(&table_name, where_clause)?
        } else {
            // No WHERE clause - full scan
            self.storage.scan(&table_name)?
        };

        Ok(ExecutionResult::Selected(rows))
    }

    fn execute_where(&mut self, table_name: &str, expr: &Expr) -> io::Result<Vec<Row>> {
        // Try to use index if possible
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                // Check if left is a column and right is a value
                if let (Expr::Identifier(col_ident), value_expr) = (left.as_ref(), right.as_ref()) {
                    let col_name = col_ident.value.as_str();
                    let value = self.expr_to_value(value_expr)?;

                    // Try index lookup first
                    match self.storage.index_lookup(table_name, col_name, &value) {
                        Ok(rows) => {
                            println!("  [Using index on {}]", col_name);
                            return Ok(rows);
                        }
                        Err(_) => {
                            // No index, fall through to scan
                            println!("  [No index on {}, using full scan]", col_name);
                        }
                    }
                }
            }
        }

        // No index available - do full scan with filter
        let all_rows = self.storage.scan(table_name)?;
        let schema = self.storage.get_schema(table_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Table schema not found")
        })?;

        let filtered: Vec<Row> = all_rows
            .into_iter()
            .filter(|row| self.evaluate_expr(expr, row, schema))
            .collect();

        Ok(filtered)
    }

    fn evaluate_expr(&self, expr: &Expr, row: &Row, schema: &TableSchema) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_to_value(left, row, schema);
                let right_val = self.eval_expr_to_value(right, row, schema);

                match op {
                    sqlparser::ast::BinaryOperator::Eq => left_val == right_val,
                    sqlparser::ast::BinaryOperator::Gt => match (&left_val, &right_val) {
                        (Value::Integer(l), Value::Integer(r)) => l > r,
                        (Value::Float(l), Value::Float(r)) => l > r,
                        _ => false,
                    },
                    sqlparser::ast::BinaryOperator::Lt => match (&left_val, &right_val) {
                        (Value::Integer(l), Value::Integer(r)) => l < r,
                        (Value::Float(l), Value::Float(r)) => l < r,
                        _ => false,
                    },
                    _ => false,
                }
            }
            _ => true,
        }
    }

    fn eval_expr_to_value(&self, expr: &Expr, row: &Row, schema: &TableSchema) -> Value {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = &ident.value;
                if let Some(col_idx) = schema.get_column_index(col_name) {
                    row.get(col_idx).cloned().unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            Expr::Value(sql_val) => self.sql_value_to_value(sql_val).unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn expr_to_value(&self, expr: &Expr) -> io::Result<Value> {
        match expr {
            Expr::Value(sql_val) => self.sql_value_to_value(sql_val),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Expected literal value",
            )),
        }
    }

    fn sql_value_to_value(&self, sql_val: &SqlValue) -> io::Result<Value> {
        match sql_val {
            SqlValue::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float(n.parse().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Invalid float")
                    })?))
                } else {
                    Ok(Value::Integer(n.parse().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Invalid integer")
                    })?))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            SqlValue::Null => Ok(Value::Null),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unsupported value type: {:?}", sql_val),
            )),
        }
    }
}

#[derive(Debug)]
pub enum ExecutionResult {
    Created,
    Inserted(usize),
    Selected(Vec<Row>),
}

#[derive(Debug)]
pub struct QueryResult {
    pub result: ExecutionResult,
    pub duration: std::time::Duration,
    pub used_index: bool,
}