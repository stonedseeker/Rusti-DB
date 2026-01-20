use super::types::{Index, Row, TableSchema, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The main storage engine using the Bitcask model
/// - Append-only log file for durability
/// - In-memory index for fast lookups
pub struct BitcaskStorage {
    data_file: File,
    data_file_path: String,
    /// Maps table_name -> (schema, row_count)
    pub tables: HashMap<String, (TableSchema, u64)>,
    /// Maps table_name -> column_name -> Index
    pub indexes: HashMap<String, HashMap<String, Index>>,
    /// Current file offset (where next write will go)
    current_offset: u64,
}

impl BitcaskStorage {
    /// Create a new storage engine with the given file path
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Get current file size (for appending)
        let metadata = file.metadata()?;
        let current_offset = metadata.len();

        Ok(BitcaskStorage {
            data_file: file,
            data_file_path: path.to_string(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            current_offset,
        })
    }

    /// Create a new table
    pub fn create_table(&mut self, schema: TableSchema) -> io::Result<()> {
        let table_name = schema.name.clone();

        if self.tables.contains_key(&table_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", table_name),
            ));
        }

        // Store schema in memory
        self.tables.insert(table_name.clone(), (schema.clone(), 0));

        // Persist the schema to disk
        self.write_schema(&schema)?;

        println!("✓ Created table '{}'", table_name);
        Ok(())
    }

    /// Write a schema to the data file
    fn write_schema(&mut self, schema: &TableSchema) -> io::Result<()> {
        // Format: [SCHEMA_MARKER][schema_bytes_length][schema_bytes]
        const SCHEMA_MARKER: u8 = 0xFF;

        let schema_bytes = bincode::serialize(schema).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Serialize error: {}", e))
        })?;

        let mut writer = BufWriter::new(&self.data_file);

        // Write marker
        writer.write_all(&[SCHEMA_MARKER])?;

        // Write length (as u32)
        let len = schema_bytes.len() as u32;
        writer.write_all(&len.to_le_bytes())?;

        // Write schema bytes
        writer.write_all(&schema_bytes)?;

        writer.flush()?;

        // Update offset
        self.current_offset += 1 + 4 + schema_bytes.len() as u64;

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert(&mut self, table_name: &str, row: Row) -> io::Result<u64> {
        // Get schema and validate
        let (schema, row_count) = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' not found", table_name),
                )
            })?;

        // Validate row matches schema
        schema.validate_row(&row).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })?;

        // Remember offset before writing
        let row_offset = self.current_offset;

        // Write row to disk
        self.write_row(table_name, &row)?;

        // Update indexes if they exist
        if let Some(table_indexes) = self.indexes.get_mut(table_name) {
            for (col_name, index) in table_indexes.iter_mut() {
                if let Some(col_idx) = schema.get_column_index(col_name) {
                    if let Some(value) = row.get(col_idx) {
                        index.insert(value, row_offset);
                    }
                }
            }
        }

        // Increment row count
        *row_count += 1;

        Ok(row_offset)
    }

    /// Write a row to the data file
    fn write_row(&mut self, table_name: &str, row: &Row) -> io::Result<()> {
        // Format: [ROW_MARKER][table_name_len][table_name][row_bytes_len][row_bytes]
        const ROW_MARKER: u8 = 0xAA;

        let row_bytes = bincode::serialize(row).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Serialize error: {}", e))
        })?;

        let table_name_bytes = table_name.as_bytes();

        let mut writer = BufWriter::new(&self.data_file);

        // Write marker
        writer.write_all(&[ROW_MARKER])?;

        // Write table name length and name
        let table_name_len = table_name_bytes.len() as u16;
        writer.write_all(&table_name_len.to_le_bytes())?;
        writer.write_all(table_name_bytes)?;

        // Write row bytes length and bytes
        let row_len = row_bytes.len() as u32;
        writer.write_all(&row_len.to_le_bytes())?;
        writer.write_all(&row_bytes)?;

        writer.flush()?;

        // Update offset
        self.current_offset += 1 + 2 + table_name_bytes.len() as u64 + 4 + row_bytes.len() as u64;

        Ok(())
    }

    /// Scan all rows in a table (slow path - no index)
    pub fn scan(&mut self, table_name: &str) -> io::Result<Vec<Row>> {
        let mut rows = Vec::new();

        // Rewind to start of file
        self.data_file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&self.data_file);
        let mut marker = [0u8; 1];

        loop {
            // Try to read marker
            match reader.read_exact(&mut marker) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            match marker[0] {
                0xFF => {
                    // Schema record - skip it
                    let mut len_bytes = [0u8; 4];
                    reader.read_exact(&mut len_bytes)?;
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    let mut schema_bytes = vec![0u8; len];
                    reader.read_exact(&mut schema_bytes)?;
                }
                0xAA => {
                    // Row record
                    let mut table_name_len_bytes = [0u8; 2];
                    reader.read_exact(&mut table_name_len_bytes)?;
                    let table_name_len = u16::from_le_bytes(table_name_len_bytes) as usize;

                    let mut table_name_bytes = vec![0u8; table_name_len];
                    reader.read_exact(&mut table_name_bytes)?;
                    let current_table = String::from_utf8_lossy(&table_name_bytes);

                    let mut row_len_bytes = [0u8; 4];
                    reader.read_exact(&mut row_len_bytes)?;
                    let row_len = u32::from_le_bytes(row_len_bytes) as usize;

                    let mut row_bytes = vec![0u8; row_len];
                    reader.read_exact(&mut row_bytes)?;

                    // Only deserialize if it's our table
                    if current_table == table_name {
                        let row: Row = bincode::deserialize(&row_bytes).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Deserialize error: {}", e),
                            )
                        })?;
                        rows.push(row);
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown marker: {:#x}", marker[0]),
                    ));
                }
            }
        }

        Ok(rows)
    }

    /// Create an index on a column (fast path)
    pub fn create_index(&mut self, table_name: &str, column_name: &str) -> io::Result<()> {
        // Get schema
        let (schema, _) = self.tables.get(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' not found", table_name),
            )
        })?;

        // Check column exists
        let column_index = schema.get_column_index(column_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Column '{}' not found in table '{}'", column_name, table_name),
            )
        })?;

        // Create the index structure
        let mut index = Index::new(table_name.to_string(), column_name.to_string(), column_index);

        // Build the index by scanning the file
        self.data_file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&self.data_file);
        let mut marker = [0u8; 1];
        let mut current_offset = 0u64;

        loop {
            match reader.read_exact(&mut marker) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            match marker[0] {
                0xFF => {
                    // Schema - skip
                    let mut len_bytes = [0u8; 4];
                    reader.read_exact(&mut len_bytes)?;
                    let len = u32::from_le_bytes(len_bytes) as usize;

                    let mut schema_bytes = vec![0u8; len];
                    reader.read_exact(&mut schema_bytes)?;

                    current_offset += 1 + 4 + len as u64;
                }
                0xAA => {
                    // Row
                    let row_start_offset = current_offset;

                    let mut table_name_len_bytes = [0u8; 2];
                    reader.read_exact(&mut table_name_len_bytes)?;
                    let table_name_len = u16::from_le_bytes(table_name_len_bytes) as usize;

                    let mut table_name_bytes = vec![0u8; table_name_len];
                    reader.read_exact(&mut table_name_bytes)?;
                    let current_table = String::from_utf8_lossy(&table_name_bytes);

                    let mut row_len_bytes = [0u8; 4];
                    reader.read_exact(&mut row_len_bytes)?;
                    let row_len = u32::from_le_bytes(row_len_bytes) as usize;

                    let mut row_bytes = vec![0u8; row_len];
                    reader.read_exact(&mut row_bytes)?;

                    current_offset += 1 + 2 + table_name_len as u64 + 4 + row_len as u64;

                    // If this row belongs to our table, add to index
                    if current_table == table_name {
                        let row: Row = bincode::deserialize(&row_bytes).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Deserialize error: {}", e),
                            )
                        })?;

                        if let Some(value) = row.get(column_index) {
                            index.insert(value, row_start_offset);
                        }
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown marker: {:#x}", marker[0]),
                    ));
                }
            }
        }

        // Store the index
        self.indexes
            .entry(table_name.to_string())
            .or_insert_with(HashMap::new)
            .insert(column_name.to_string(), index);

        println!("✓ Created index on {}.{}", table_name, column_name);
        Ok(())
    }

    /// Lookup rows using an index (fast path)
    pub fn index_lookup(&mut self, table_name: &str, column_name: &str, value: &Value) -> io::Result<Vec<Row>> {
        // Check if index exists
        let offsets = self
            .indexes
            .get(table_name)
            .and_then(|table_indexes| table_indexes.get(column_name))
            .and_then(|index| index.lookup(value))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("No index on {}.{}", table_name, column_name),
                )
            })?;

        let mut rows = Vec::new();

        // Read each row from disk using the offset
        for &offset in offsets {
            let row = self.read_row_at_offset(offset)?;
            rows.push(row);
        }

        Ok(rows)
    }

    /// Read a single row from a specific file offset
    fn read_row_at_offset(&mut self, offset: u64) -> io::Result<Row> {
        self.data_file.seek(SeekFrom::Start(offset))?;

        let mut reader = BufReader::new(&self.data_file);

        // Read marker
        let mut marker = [0u8; 1];
        reader.read_exact(&mut marker)?;

        if marker[0] != 0xAA {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected row marker",
            ));
        }

        // Read table name
        let mut table_name_len_bytes = [0u8; 2];
        reader.read_exact(&mut table_name_len_bytes)?;
        let table_name_len = u16::from_le_bytes(table_name_len_bytes) as usize;

        let mut table_name_bytes = vec![0u8; table_name_len];
        reader.read_exact(&mut table_name_bytes)?;

        // Read row
        let mut row_len_bytes = [0u8; 4];
        reader.read_exact(&mut row_len_bytes)?;
        let row_len = u32::from_le_bytes(row_len_bytes) as usize;

        let mut row_bytes = vec![0u8; row_len];
        reader.read_exact(&mut row_bytes)?;

        let row: Row = bincode::deserialize(&row_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Deserialize error: {}", e),
            )
        })?;

        Ok(row)
    }

    /// Get table schema
    pub fn get_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.tables.get(table_name).map(|(schema, _)| schema)
    }
}
