use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use mydb_core::{
    Column, ColumnId, ColumnInfo, DataType, IndexId, IndexInfo, MyDbError, Result, Row, Schema,
    TableId, TableInfo, Value,
};

use crate::{HeapFile, Rid};

#[derive(Debug)]
pub struct Catalog {
    tables_heap: HeapFile,
    columns_heap: HeapFile,
    indexes_heap: HeapFile,
    tables: BTreeMap<TableId, TableEntry>,
    table_name_to_id: HashMap<String, TableId>,
    columns_by_table: BTreeMap<TableId, Vec<ColumnEntry>>,
    indexes_by_table: BTreeMap<TableId, Vec<IndexEntry>>,
    next_table_id: TableId,
    next_column_id: ColumnId,
    next_index_id: IndexId,
}

#[derive(Debug, Clone)]
struct TableEntry {
    rid: Rid,
    info: TableInfo,
}

#[derive(Debug, Clone)]
struct ColumnEntry {
    rid: Rid,
    info: ColumnInfo,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    rid: Rid,
    info: IndexInfo,
}

impl Catalog {
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref();
        if let Some(parent) = base_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut catalog = Self {
            tables_heap: HeapFile::open(path_with_suffix(base_path, "tables"))?,
            columns_heap: HeapFile::open(path_with_suffix(base_path, "columns"))?,
            indexes_heap: HeapFile::open(path_with_suffix(base_path, "indexes"))?,
            tables: BTreeMap::new(),
            table_name_to_id: HashMap::new(),
            columns_by_table: BTreeMap::new(),
            indexes_by_table: BTreeMap::new(),
            next_table_id: 1,
            next_column_id: 1,
            next_index_id: 1,
        };
        catalog.bootstrap_from_disk()?;
        Ok(catalog)
    }

    pub fn create_table(&mut self, table_name: &str, columns: Vec<Column>) -> Result<TableInfo> {
        let normalized = normalize_name(table_name);
        if normalized.is_empty() {
            return Err(MyDbError::InvalidSchema(
                "table name cannot be empty".to_string(),
            ));
        }
        if self.table_name_to_id.contains_key(&normalized) {
            return Err(MyDbError::AlreadyExists {
                object: "table".to_string(),
                name: table_name.to_string(),
            });
        }

        let schema = Schema::new(columns)?;
        let table_id = self.next_table_id;
        self.next_table_id += 1;

        let table_info = TableInfo {
            table_id,
            name: table_name.to_string(),
        };
        let table_row = Row::new(vec![
            Value::BigInt(i64::from(table_id)),
            Value::Varchar(table_info.name.clone()),
        ]);
        let table_rid = self
            .tables_heap
            .insert_record(&table_row.encode(&tables_schema())?)?;

        let mut column_entries = Vec::with_capacity(schema.columns.len());
        for mut column in schema.columns {
            let column_id = self.next_column_id;
            self.next_column_id += 1;
            column.id = column_id;
            let info = ColumnInfo {
                table_id,
                column_id,
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            };
            let column_row = Row::new(vec![
                Value::BigInt(i64::from(column_id)),
                Value::BigInt(i64::from(table_id)),
                Value::Varchar(info.name.clone()),
                Value::Varchar(info.data_type.as_str().to_string()),
                Value::Bool(info.nullable),
            ]);
            let rid = self
                .columns_heap
                .insert_record(&column_row.encode(&columns_schema())?)?;
            column_entries.push(ColumnEntry { rid, info });
        }

        self.tables_heap.sync()?;
        self.columns_heap.sync()?;

        self.tables.insert(
            table_id,
            TableEntry {
                rid: table_rid,
                info: table_info.clone(),
            },
        );
        self.table_name_to_id.insert(normalized, table_id);
        self.columns_by_table.insert(table_id, column_entries);
        Ok(table_info)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let table_id = self
            .table_name_to_id
            .get(&normalize_name(table_name))
            .copied()
            .ok_or(MyDbError::NotFound {
                object: "table".to_string(),
                name: table_name.to_string(),
            })?;

        if let Some(table_entry) = self.tables.remove(&table_id) {
            self.tables_heap.delete_record(table_entry.rid)?;
        }
        if let Some(column_entries) = self.columns_by_table.remove(&table_id) {
            for entry in column_entries {
                self.columns_heap.delete_record(entry.rid)?;
            }
        }
        if let Some(index_entries) = self.indexes_by_table.remove(&table_id) {
            for entry in index_entries {
                self.indexes_heap.delete_record(entry.rid)?;
            }
        }

        self.table_name_to_id.remove(&normalize_name(table_name));
        self.tables_heap.sync()?;
        self.columns_heap.sync()?;
        self.indexes_heap.sync()?;
        Ok(())
    }

    pub fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column_name: &str,
    ) -> Result<IndexInfo> {
        let table = self.get_table(table_name)?;
        let schema = self.describe_table(table_name)?;
        if schema.find_column(column_name).is_none() {
            return Err(MyDbError::NotFound {
                object: "column".to_string(),
                name: column_name.to_string(),
            });
        }

        let normalized_index_name = normalize_name(index_name);
        if normalized_index_name.is_empty() {
            return Err(MyDbError::InvalidSchema(
                "index name cannot be empty".to_string(),
            ));
        }
        if self
            .indexes_by_table
            .get(&table.table_id)
            .map(|items| {
                items
                    .iter()
                    .any(|entry| normalize_name(&entry.info.name) == normalized_index_name)
            })
            .unwrap_or(false)
        {
            return Err(MyDbError::AlreadyExists {
                object: "index".to_string(),
                name: index_name.to_string(),
            });
        }

        let index_id = self.next_index_id;
        self.next_index_id += 1;
        let info = IndexInfo {
            index_id,
            table_id: table.table_id,
            name: index_name.to_string(),
            key_columns: column_name.to_string(),
        };
        let row = Row::new(vec![
            Value::BigInt(i64::from(info.index_id)),
            Value::BigInt(i64::from(info.table_id)),
            Value::Varchar(info.name.clone()),
            Value::Varchar(info.key_columns.clone()),
        ]);
        let rid = self
            .indexes_heap
            .insert_record(&row.encode(&indexes_schema())?)?;
        self.indexes_heap.sync()?;
        self.indexes_by_table
            .entry(info.table_id)
            .or_default()
            .push(IndexEntry {
                rid,
                info: info.clone(),
            });
        Ok(info)
    }

    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.tables
            .values()
            .map(|entry| entry.info.clone())
            .collect()
    }

    pub fn get_table(&self, table_name: &str) -> Result<TableInfo> {
        let table_id = self
            .table_name_to_id
            .get(&normalize_name(table_name))
            .copied()
            .ok_or(MyDbError::NotFound {
                object: "table".to_string(),
                name: table_name.to_string(),
            })?;
        self.tables
            .get(&table_id)
            .map(|entry| entry.info.clone())
            .ok_or(MyDbError::Corruption(format!(
                "table id {} has name mapping but no metadata",
                table_id
            )))
    }

    pub fn list_indexes(&self, table_name: &str) -> Result<Vec<IndexInfo>> {
        let table = self.get_table(table_name)?;
        Ok(self
            .indexes_by_table
            .get(&table.table_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|entry| entry.info)
            .collect())
    }

    pub fn describe_table(&self, table_name: &str) -> Result<Schema> {
        let table_id = self.get_table(table_name)?.table_id;

        let mut columns = self
            .columns_by_table
            .get(&table_id)
            .cloned()
            .unwrap_or_default();
        columns.sort_by_key(|entry| entry.info.column_id);
        let schema_columns = columns
            .into_iter()
            .map(|entry| {
                Column::with_id(
                    entry.info.column_id,
                    entry.info.name,
                    entry.info.data_type,
                    entry.info.nullable,
                )
            })
            .collect();
        Schema::new(schema_columns)
    }

    fn bootstrap_from_disk(&mut self) -> Result<()> {
        let table_schema = tables_schema();
        let column_schema = columns_schema();
        let index_schema = indexes_schema();

        for (rid, raw) in self.tables_heap.scan_records()? {
            let row = Row::decode(&table_schema, &raw)?;
            let table_id = value_as_u32(&row.values[0], "table_id")?;
            let table_name = value_as_string(&row.values[1], "name")?;
            let info = TableInfo {
                table_id,
                name: table_name,
            };
            let normalized = normalize_name(&info.name);
            if self.table_name_to_id.insert(normalized, table_id).is_some() {
                return Err(MyDbError::Corruption(format!(
                    "duplicate table id or name detected while loading table '{}'",
                    info.name
                )));
            }
            self.next_table_id = self.next_table_id.max(table_id.saturating_add(1));
            self.tables.insert(table_id, TableEntry { rid, info });
        }

        for (rid, raw) in self.columns_heap.scan_records()? {
            let row = Row::decode(&column_schema, &raw)?;
            let column_id = value_as_u32(&row.values[0], "column_id")?;
            let table_id = value_as_u32(&row.values[1], "table_id")?;
            let name = value_as_string(&row.values[2], "name")?;
            let data_type = value_as_data_type(&row.values[3], "data_type")?;
            let nullable = value_as_bool(&row.values[4], "nullable")?;
            let info = ColumnInfo {
                table_id,
                column_id,
                name,
                data_type,
                nullable,
            };
            self.next_column_id = self.next_column_id.max(column_id.saturating_add(1));
            self.columns_by_table
                .entry(table_id)
                .or_default()
                .push(ColumnEntry { rid, info });
        }

        for (rid, raw) in self.indexes_heap.scan_records()? {
            let row = Row::decode(&index_schema, &raw)?;
            let index_id = value_as_u32(&row.values[0], "index_id")?;
            let table_id = value_as_u32(&row.values[1], "table_id")?;
            let name = value_as_string(&row.values[2], "name")?;
            let key_columns = value_as_string(&row.values[3], "key_columns")?;
            self.next_index_id = self.next_index_id.max(index_id.saturating_add(1));
            self.indexes_by_table
                .entry(table_id)
                .or_default()
                .push(IndexEntry {
                    rid,
                    info: IndexInfo {
                        index_id,
                        table_id,
                        name,
                        key_columns,
                    },
                });
        }

        for table_id in self.columns_by_table.keys() {
            if !self.tables.contains_key(table_id) {
                return Err(MyDbError::Corruption(format!(
                    "column references missing table_id {}",
                    table_id
                )));
            }
        }
        for entries in self.indexes_by_table.values() {
            for entry in entries {
                if !self.tables.contains_key(&entry.info.table_id) {
                    return Err(MyDbError::Corruption(format!(
                        "index '{}' references missing table_id {}",
                        entry.info.name, entry.info.table_id
                    )));
                }
            }
        }

        Ok(())
    }
}

fn tables_schema() -> Schema {
    Schema::new(vec![
        Column::new("table_id", DataType::BigInt, false),
        Column::new("name", DataType::Varchar, false),
    ])
    .expect("system table schema")
}

fn columns_schema() -> Schema {
    Schema::new(vec![
        Column::new("column_id", DataType::BigInt, false),
        Column::new("table_id", DataType::BigInt, false),
        Column::new("name", DataType::Varchar, false),
        Column::new("data_type", DataType::Varchar, false),
        Column::new("nullable", DataType::Bool, false),
    ])
    .expect("system column schema")
}

fn indexes_schema() -> Schema {
    Schema::new(vec![
        Column::new("index_id", DataType::BigInt, false),
        Column::new("table_id", DataType::BigInt, false),
        Column::new("name", DataType::Varchar, false),
        Column::new("key_columns", DataType::Varchar, false),
    ])
    .expect("system index schema")
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn path_with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut os: OsString = base.as_os_str().to_os_string();
    os.push(format!(".{suffix}"));
    PathBuf::from(os)
}

fn value_as_u32(value: &Value, column_name: &str) -> Result<u32> {
    match value {
        Value::BigInt(v) => u32::try_from(*v).map_err(|_| {
            MyDbError::Corruption(format!(
                "column '{column_name}' value {} does not fit u32",
                v
            ))
        }),
        other => Err(MyDbError::Corruption(format!(
            "column '{column_name}' expected BIGINT, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_string(value: &Value, column_name: &str) -> Result<String> {
    match value {
        Value::Varchar(v) => Ok(v.clone()),
        other => Err(MyDbError::Corruption(format!(
            "column '{column_name}' expected VARCHAR, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_bool(value: &Value, column_name: &str) -> Result<bool> {
    match value {
        Value::Bool(v) => Ok(*v),
        other => Err(MyDbError::Corruption(format!(
            "column '{column_name}' expected BOOL, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_data_type(value: &Value, column_name: &str) -> Result<DataType> {
    let text = value_as_string(value, column_name)?;
    text.parse()
}
