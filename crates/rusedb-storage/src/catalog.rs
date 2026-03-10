use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use rusedb_core::{
    Column, ColumnId, ColumnInfo, DataType, IndexId, IndexInfo, Result, Row, RuseDbError, Schema,
    TableId, TableInfo, Value,
};

use crate::{HeapFile, Rid};

pub type ConstraintId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    ForeignKey,
}

impl ConstraintKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::PrimaryKey => "PRIMARY_KEY",
            Self::Unique => "UNIQUE",
            Self::ForeignKey => "FOREIGN_KEY",
        }
    }

    fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "PRIMARY_KEY" | "PRIMARY KEY" => Ok(Self::PrimaryKey),
            "UNIQUE" => Ok(Self::Unique),
            "FOREIGN_KEY" | "FOREIGN KEY" => Ok(Self::ForeignKey),
            other => Err(RuseDbError::Corruption(format!(
                "unknown constraint kind '{}'",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintDef {
    pub name: String,
    pub kind: ConstraintKind,
    pub key_columns: Vec<String>,
    pub referenced_table: Option<String>,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintInfo {
    pub constraint_id: ConstraintId,
    pub table_id: TableId,
    pub name: String,
    pub kind: ConstraintKind,
    pub key_columns: Vec<String>,
    pub referenced_table: Option<String>,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug)]
pub struct Catalog {
    tables_heap: HeapFile,
    columns_heap: HeapFile,
    indexes_heap: HeapFile,
    constraints_heap: HeapFile,
    tables: BTreeMap<TableId, TableEntry>,
    table_name_to_id: HashMap<String, TableId>,
    columns_by_table: BTreeMap<TableId, Vec<ColumnEntry>>,
    indexes_by_table: BTreeMap<TableId, Vec<IndexEntry>>,
    constraints_by_table: BTreeMap<TableId, Vec<ConstraintEntry>>,
    next_table_id: TableId,
    next_column_id: ColumnId,
    next_index_id: IndexId,
    next_constraint_id: ConstraintId,
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

#[derive(Debug, Clone)]
struct ConstraintEntry {
    rid: Rid,
    info: ConstraintInfo,
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
            constraints_heap: HeapFile::open(path_with_suffix(base_path, "constraints"))?,
            tables: BTreeMap::new(),
            table_name_to_id: HashMap::new(),
            columns_by_table: BTreeMap::new(),
            indexes_by_table: BTreeMap::new(),
            constraints_by_table: BTreeMap::new(),
            next_table_id: 1,
            next_column_id: 1,
            next_index_id: 1,
            next_constraint_id: 1,
        };
        catalog.bootstrap_from_disk()?;
        Ok(catalog)
    }

    pub fn create_table(
        &mut self,
        table_name: &str,
        columns: Vec<Column>,
        constraints: Vec<ConstraintDef>,
    ) -> Result<TableInfo> {
        let normalized = normalize_name(table_name);
        if normalized.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "table name cannot be empty".to_string(),
            ));
        }
        if self.table_name_to_id.contains_key(&normalized) {
            return Err(RuseDbError::AlreadyExists {
                object: "table".to_string(),
                name: table_name.to_string(),
            });
        }

        let schema = Schema::new(columns)?;
        let constraint_defs =
            self.normalize_and_validate_constraints(table_name, &schema, constraints)?;
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

        let mut constraint_entries = Vec::with_capacity(constraint_defs.len());
        for definition in constraint_defs {
            let constraint_id = self.next_constraint_id;
            self.next_constraint_id += 1;
            let info = ConstraintInfo {
                constraint_id,
                table_id,
                name: definition.name,
                kind: definition.kind,
                key_columns: definition.key_columns,
                referenced_table: definition.referenced_table,
                referenced_columns: definition.referenced_columns,
            };
            let row = Row::new(vec![
                Value::BigInt(i64::from(info.constraint_id)),
                Value::BigInt(i64::from(info.table_id)),
                Value::Varchar(info.name.clone()),
                Value::Varchar(info.kind.as_str().to_string()),
                Value::Varchar(encode_constraint_payload(&info)?),
            ]);
            let rid = self
                .constraints_heap
                .insert_record(&row.encode(&constraints_schema())?)?;
            constraint_entries.push(ConstraintEntry { rid, info });
        }

        self.tables_heap.sync()?;
        self.columns_heap.sync()?;
        self.constraints_heap.sync()?;

        self.tables.insert(
            table_id,
            TableEntry {
                rid: table_rid,
                info: table_info.clone(),
            },
        );
        self.table_name_to_id.insert(normalized, table_id);
        self.columns_by_table.insert(table_id, column_entries);
        self.constraints_by_table
            .insert(table_id, constraint_entries);
        Ok(table_info)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
        let table_id = self
            .table_name_to_id
            .get(&normalize_name(table_name))
            .copied()
            .ok_or(RuseDbError::NotFound {
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
        if let Some(constraint_entries) = self.constraints_by_table.remove(&table_id) {
            for entry in constraint_entries {
                self.constraints_heap.delete_record(entry.rid)?;
            }
        }

        self.table_name_to_id.remove(&normalize_name(table_name));
        self.tables_heap.sync()?;
        self.columns_heap.sync()?;
        self.indexes_heap.sync()?;
        self.constraints_heap.sync()?;
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
            return Err(RuseDbError::NotFound {
                object: "column".to_string(),
                name: column_name.to_string(),
            });
        }

        let normalized_index_name = normalize_name(index_name);
        if normalized_index_name.is_empty() {
            return Err(RuseDbError::InvalidSchema(
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
            return Err(RuseDbError::AlreadyExists {
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

    pub fn add_column(&mut self, table_name: &str, column: Column) -> Result<ColumnInfo> {
        let table = self.get_table(table_name)?;
        let column_name = column.name.trim();
        if column_name.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "column name cannot be empty".to_string(),
            ));
        }

        let normalized = normalize_name(column_name);
        if self
            .columns_by_table
            .get(&table.table_id)
            .map(|entries| {
                entries
                    .iter()
                    .any(|entry| normalize_name(&entry.info.name) == normalized)
            })
            .unwrap_or(false)
        {
            return Err(RuseDbError::AlreadyExists {
                object: "column".to_string(),
                name: column_name.to_string(),
            });
        }

        let column_id = self.next_column_id;
        self.next_column_id += 1;
        let info = ColumnInfo {
            table_id: table.table_id,
            column_id,
            name: column_name.to_string(),
            data_type: column.data_type,
            nullable: column.nullable,
        };
        let rid = self
            .columns_heap
            .insert_record(&encode_column_row(&info).encode(&columns_schema())?)?;
        self.columns_heap.sync()?;
        self.columns_by_table
            .entry(table.table_id)
            .or_default()
            .push(ColumnEntry {
                rid,
                info: info.clone(),
            });
        Ok(info)
    }

    pub fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<ColumnInfo> {
        let table = self.get_table(table_name)?;
        let entries =
            self.columns_by_table
                .get_mut(&table.table_id)
                .ok_or(RuseDbError::Corruption(format!(
                    "table '{}' has no column metadata",
                    table_name
                )))?;
        if entries.len() <= 1 {
            return Err(RuseDbError::InvalidSchema(format!(
                "cannot drop last column from table '{}'",
                table_name
            )));
        }

        let normalized = normalize_name(column_name);
        let idx = entries
            .iter()
            .position(|entry| normalize_name(&entry.info.name) == normalized)
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: column_name.to_string(),
            })?;
        let entry = entries.remove(idx);
        self.columns_heap.delete_record(entry.rid)?;
        self.columns_heap.sync()?;
        Ok(entry.info)
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<TableInfo> {
        let old_normalized = normalize_name(old_name);
        let new_name_trimmed = new_name.trim();
        let new_normalized = normalize_name(new_name_trimmed);
        if new_normalized.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "table name cannot be empty".to_string(),
            ));
        }

        let table_id =
            self.table_name_to_id
                .get(&old_normalized)
                .copied()
                .ok_or(RuseDbError::NotFound {
                    object: "table".to_string(),
                    name: old_name.to_string(),
                })?;
        if old_normalized != new_normalized && self.table_name_to_id.contains_key(&new_normalized) {
            return Err(RuseDbError::AlreadyExists {
                object: "table".to_string(),
                name: new_name_trimmed.to_string(),
            });
        }

        self.table_name_to_id.remove(&old_normalized);
        self.table_name_to_id.insert(new_normalized, table_id);

        let entry = self
            .tables
            .get_mut(&table_id)
            .ok_or(RuseDbError::Corruption(format!(
                "table id {} missing metadata",
                table_id
            )))?;
        entry.info.name = new_name_trimmed.to_string();
        entry.rid = self.tables_heap.update_record(
            entry.rid,
            &encode_table_row(&entry.info).encode(&tables_schema())?,
        )?;

        let renamed_table = entry.info.name.clone();
        for entries in self.constraints_by_table.values_mut() {
            for constraint in entries.iter_mut() {
                if let Some(referenced_table) = constraint.info.referenced_table.as_ref()
                    && normalize_name(referenced_table) == old_normalized
                {
                    constraint.info.referenced_table = Some(renamed_table.clone());
                    constraint.rid = self.constraints_heap.update_record(
                        constraint.rid,
                        &encode_constraint_row(&constraint.info)?.encode(&constraints_schema())?,
                    )?;
                }
            }
        }

        self.tables_heap.sync()?;
        self.constraints_heap.sync()?;
        Ok(entry.info.clone())
    }

    pub fn rename_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<ColumnInfo> {
        let table = self.get_table(table_name)?;
        let table_name_actual = table.name.clone();
        let table_normalized = normalize_name(&table_name_actual);
        let old_normalized = normalize_name(old_name);
        let new_name_trimmed = new_name.trim();
        let new_normalized = normalize_name(new_name_trimmed);
        if new_normalized.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "column name cannot be empty".to_string(),
            ));
        }

        let columns =
            self.columns_by_table
                .get_mut(&table.table_id)
                .ok_or(RuseDbError::Corruption(format!(
                    "table '{}' has no column metadata",
                    table_name
                )))?;
        let old_idx = columns
            .iter()
            .position(|entry| normalize_name(&entry.info.name) == old_normalized)
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: old_name.to_string(),
            })?;
        if old_normalized != new_normalized
            && columns
                .iter()
                .any(|entry| normalize_name(&entry.info.name) == new_normalized)
        {
            return Err(RuseDbError::AlreadyExists {
                object: "column".to_string(),
                name: new_name_trimmed.to_string(),
            });
        }

        columns[old_idx].info.name = new_name_trimmed.to_string();
        columns[old_idx].rid = self.columns_heap.update_record(
            columns[old_idx].rid,
            &encode_column_row(&columns[old_idx].info).encode(&columns_schema())?,
        )?;

        if let Some(indexes) = self.indexes_by_table.get_mut(&table.table_id) {
            for index in indexes.iter_mut() {
                if normalize_name(&index.info.key_columns) == old_normalized {
                    index.info.key_columns = new_name_trimmed.to_string();
                    index.rid = self.indexes_heap.update_record(
                        index.rid,
                        &encode_index_row(&index.info).encode(&indexes_schema())?,
                    )?;
                }
            }
        }

        for constraints in self.constraints_by_table.values_mut() {
            for constraint in constraints.iter_mut() {
                let mut changed = false;
                if constraint.info.table_id == table.table_id {
                    changed |= rename_in_column_list(
                        &mut constraint.info.key_columns,
                        &old_normalized,
                        new_name_trimmed,
                    );
                }
                if let Some(referenced_table) = constraint.info.referenced_table.as_ref()
                    && normalize_name(referenced_table) == table_normalized
                {
                    changed |= rename_in_column_list(
                        &mut constraint.info.referenced_columns,
                        &old_normalized,
                        new_name_trimmed,
                    );
                }
                if changed {
                    constraint.rid = self.constraints_heap.update_record(
                        constraint.rid,
                        &encode_constraint_row(&constraint.info)?.encode(&constraints_schema())?,
                    )?;
                }
            }
        }

        self.columns_heap.sync()?;
        self.indexes_heap.sync()?;
        self.constraints_heap.sync()?;
        Ok(columns[old_idx].info.clone())
    }

    pub fn alter_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        data_type: Option<DataType>,
        nullable: Option<bool>,
    ) -> Result<ColumnInfo> {
        let table = self.get_table(table_name)?;
        let columns =
            self.columns_by_table
                .get_mut(&table.table_id)
                .ok_or(RuseDbError::Corruption(format!(
                    "table '{}' has no column metadata",
                    table_name
                )))?;
        let target_idx = columns
            .iter()
            .position(|entry| normalize_name(&entry.info.name) == normalize_name(column_name))
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: column_name.to_string(),
            })?;

        if let Some(dt) = data_type {
            columns[target_idx].info.data_type = dt;
        }
        if let Some(is_nullable) = nullable {
            columns[target_idx].info.nullable = is_nullable;
        }

        columns[target_idx].rid = self.columns_heap.update_record(
            columns[target_idx].rid,
            &encode_column_row(&columns[target_idx].info).encode(&columns_schema())?,
        )?;
        self.columns_heap.sync()?;
        Ok(columns[target_idx].info.clone())
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
            .ok_or(RuseDbError::NotFound {
                object: "table".to_string(),
                name: table_name.to_string(),
            })?;
        self.tables
            .get(&table_id)
            .map(|entry| entry.info.clone())
            .ok_or(RuseDbError::Corruption(format!(
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

    pub fn list_constraints(&self, table_name: &str) -> Result<Vec<ConstraintInfo>> {
        let table = self.get_table(table_name)?;
        Ok(self
            .constraints_by_table
            .get(&table.table_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|entry| entry.info)
            .collect())
    }

    pub fn list_all_constraints(&self) -> Vec<ConstraintInfo> {
        self.constraints_by_table
            .values()
            .flat_map(|entries| entries.iter().map(|entry| entry.info.clone()))
            .collect()
    }

    fn normalize_and_validate_constraints(
        &self,
        table_name: &str,
        schema: &Schema,
        constraints: Vec<ConstraintDef>,
    ) -> Result<Vec<ConstraintDef>> {
        let mut has_primary_key = false;
        let mut names = HashSet::new();
        let mut normalized = Vec::with_capacity(constraints.len());

        for constraint in constraints {
            let raw_name = constraint.name.trim();
            if raw_name.is_empty() {
                return Err(RuseDbError::InvalidSchema(
                    "constraint name cannot be empty".to_string(),
                ));
            }
            let normalized_name = raw_name.to_ascii_lowercase();
            if !names.insert(normalized_name) {
                return Err(RuseDbError::AlreadyExists {
                    object: "constraint".to_string(),
                    name: constraint.name,
                });
            }
            if matches!(constraint.kind, ConstraintKind::PrimaryKey) {
                if has_primary_key {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "table '{}' can have only one PRIMARY KEY",
                        table_name
                    )));
                }
                has_primary_key = true;
            }
            if constraint.key_columns.is_empty() {
                return Err(RuseDbError::InvalidSchema(format!(
                    "constraint '{}' must reference at least one column",
                    raw_name
                )));
            }

            let mut keys = Vec::new();
            let mut key_names = HashSet::new();
            for key in constraint.key_columns {
                let key_trimmed = key.trim();
                if key_trimmed.is_empty() {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "constraint '{}' contains empty column name",
                        raw_name
                    )));
                }
                if schema.find_column(key_trimmed).is_none() {
                    return Err(RuseDbError::NotFound {
                        object: "column".to_string(),
                        name: key_trimmed.to_string(),
                    });
                }
                let normalized_key = key_trimmed.to_ascii_lowercase();
                if !key_names.insert(normalized_key) {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "constraint '{}' has duplicate key column '{}'",
                        raw_name, key_trimmed
                    )));
                }
                keys.push(key_trimmed.to_string());
            }

            let (referenced_table, referenced_columns) = if matches!(
                constraint.kind,
                ConstraintKind::ForeignKey
            ) {
                let reference_table =
                    constraint
                        .referenced_table
                        .ok_or(RuseDbError::InvalidSchema(format!(
                            "FOREIGN KEY constraint '{}' missing referenced table",
                            raw_name
                        )))?;
                let reference_table = reference_table.trim().to_string();
                if reference_table.is_empty() {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "FOREIGN KEY constraint '{}' has empty referenced table",
                        raw_name
                    )));
                }
                let parent_schema =
                    self.describe_table(&reference_table)
                        .map_err(|_| RuseDbError::NotFound {
                            object: "table".to_string(),
                            name: reference_table.clone(),
                        })?;
                let mut ref_cols = Vec::new();
                let mut ref_names = HashSet::new();
                for col in constraint.referenced_columns {
                    let col = col.trim();
                    if col.is_empty() {
                        return Err(RuseDbError::InvalidSchema(format!(
                            "FOREIGN KEY constraint '{}' has empty referenced column",
                            raw_name
                        )));
                    }
                    if parent_schema.find_column(col).is_none() {
                        return Err(RuseDbError::NotFound {
                            object: "column".to_string(),
                            name: format!("{}.{}", reference_table, col),
                        });
                    }
                    let normalized_col = col.to_ascii_lowercase();
                    if !ref_names.insert(normalized_col) {
                        return Err(RuseDbError::InvalidSchema(format!(
                            "FOREIGN KEY constraint '{}' has duplicate referenced column '{}'",
                            raw_name, col
                        )));
                    }
                    ref_cols.push(col.to_string());
                }
                if ref_cols.len() != keys.len() {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "FOREIGN KEY constraint '{}' requires same number of source and referenced columns",
                        raw_name
                    )));
                }
                (Some(reference_table), ref_cols)
            } else {
                if constraint.referenced_table.is_some()
                    || !constraint.referenced_columns.is_empty()
                {
                    return Err(RuseDbError::InvalidSchema(format!(
                        "constraint '{}' has unexpected REFERENCES target",
                        raw_name
                    )));
                }
                (None, Vec::new())
            };

            normalized.push(ConstraintDef {
                name: raw_name.to_string(),
                kind: constraint.kind,
                key_columns: keys,
                referenced_table,
                referenced_columns,
            });
        }

        Ok(normalized)
    }

    fn bootstrap_from_disk(&mut self) -> Result<()> {
        let table_schema = tables_schema();
        let column_schema = columns_schema();
        let index_schema = indexes_schema();
        let constraint_schema = constraints_schema();

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
                return Err(RuseDbError::Corruption(format!(
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

        for (rid, raw) in self.constraints_heap.scan_records()? {
            let row = Row::decode(&constraint_schema, &raw)?;
            let constraint_id = value_as_u32(&row.values[0], "constraint_id")?;
            let table_id = value_as_u32(&row.values[1], "table_id")?;
            let name = value_as_string(&row.values[2], "name")?;
            let kind = ConstraintKind::from_str(&value_as_string(&row.values[3], "kind")?)?;
            let key_columns_raw = value_as_string(&row.values[4], "constraint_payload")?;
            let (key_columns, referenced_table, referenced_columns) =
                decode_constraint_payload(kind, &key_columns_raw)?;
            self.next_constraint_id = self.next_constraint_id.max(constraint_id.saturating_add(1));
            self.constraints_by_table
                .entry(table_id)
                .or_default()
                .push(ConstraintEntry {
                    rid,
                    info: ConstraintInfo {
                        constraint_id,
                        table_id,
                        name,
                        kind,
                        key_columns,
                        referenced_table,
                        referenced_columns,
                    },
                });
        }

        for table_id in self.columns_by_table.keys() {
            if !self.tables.contains_key(table_id) {
                return Err(RuseDbError::Corruption(format!(
                    "column references missing table_id {}",
                    table_id
                )));
            }
        }
        for entries in self.indexes_by_table.values() {
            for entry in entries {
                if !self.tables.contains_key(&entry.info.table_id) {
                    return Err(RuseDbError::Corruption(format!(
                        "index '{}' references missing table_id {}",
                        entry.info.name, entry.info.table_id
                    )));
                }
            }
        }
        for entries in self.constraints_by_table.values() {
            for entry in entries {
                if !self.tables.contains_key(&entry.info.table_id) {
                    return Err(RuseDbError::Corruption(format!(
                        "constraint '{}' references missing table_id {}",
                        entry.info.name, entry.info.table_id
                    )));
                }
                if matches!(entry.info.kind, ConstraintKind::ForeignKey) {
                    let referenced_table_name =
                        entry
                            .info
                            .referenced_table
                            .as_ref()
                            .ok_or(RuseDbError::Corruption(format!(
                                "FOREIGN KEY constraint '{}' missing referenced table",
                                entry.info.name
                            )))?;
                    let referenced_table_id = self
                        .table_name_to_id
                        .get(&normalize_name(referenced_table_name))
                        .copied()
                        .ok_or(RuseDbError::Corruption(format!(
                            "FOREIGN KEY constraint '{}' references unknown table '{}'",
                            entry.info.name, referenced_table_name
                        )))?;
                    let parent_columns = self
                        .columns_by_table
                        .get(&referenced_table_id)
                        .cloned()
                        .unwrap_or_default();
                    for ref_col in &entry.info.referenced_columns {
                        if !parent_columns
                            .iter()
                            .any(|c| normalize_name(&c.info.name) == normalize_name(ref_col))
                        {
                            return Err(RuseDbError::Corruption(format!(
                                "FOREIGN KEY constraint '{}' references unknown column '{}.{}'",
                                entry.info.name, referenced_table_name, ref_col
                            )));
                        }
                    }
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

fn constraints_schema() -> Schema {
    Schema::new(vec![
        Column::new("constraint_id", DataType::BigInt, false),
        Column::new("table_id", DataType::BigInt, false),
        Column::new("name", DataType::Varchar, false),
        Column::new("kind", DataType::Varchar, false),
        Column::new("key_columns", DataType::Varchar, false),
    ])
    .expect("system constraint schema")
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn path_with_suffix(base: &Path, suffix: &str) -> PathBuf {
    let mut os: OsString = base.as_os_str().to_os_string();
    os.push(format!(".{suffix}"));
    PathBuf::from(os)
}

fn encode_table_row(info: &TableInfo) -> Row {
    Row::new(vec![
        Value::BigInt(i64::from(info.table_id)),
        Value::Varchar(info.name.clone()),
    ])
}

fn encode_column_row(info: &ColumnInfo) -> Row {
    Row::new(vec![
        Value::BigInt(i64::from(info.column_id)),
        Value::BigInt(i64::from(info.table_id)),
        Value::Varchar(info.name.clone()),
        Value::Varchar(info.data_type.as_str().to_string()),
        Value::Bool(info.nullable),
    ])
}

fn encode_index_row(info: &IndexInfo) -> Row {
    Row::new(vec![
        Value::BigInt(i64::from(info.index_id)),
        Value::BigInt(i64::from(info.table_id)),
        Value::Varchar(info.name.clone()),
        Value::Varchar(info.key_columns.clone()),
    ])
}

fn encode_constraint_row(info: &ConstraintInfo) -> Result<Row> {
    Ok(Row::new(vec![
        Value::BigInt(i64::from(info.constraint_id)),
        Value::BigInt(i64::from(info.table_id)),
        Value::Varchar(info.name.clone()),
        Value::Varchar(info.kind.as_str().to_string()),
        Value::Varchar(encode_constraint_payload(info)?),
    ]))
}

fn encode_constraint_payload(info: &ConstraintInfo) -> Result<String> {
    match info.kind {
        ConstraintKind::PrimaryKey | ConstraintKind::Unique => Ok(info.key_columns.join(",")),
        ConstraintKind::ForeignKey => {
            let reference_table = info
                .referenced_table
                .as_ref()
                .ok_or(RuseDbError::Corruption(format!(
                    "FOREIGN KEY constraint '{}' missing referenced table",
                    info.name
                )))?;
            if info.key_columns.len() != info.referenced_columns.len() {
                return Err(RuseDbError::Corruption(format!(
                    "FOREIGN KEY constraint '{}' has mismatched key/reference column count",
                    info.name
                )));
            }
            Ok(format!(
                "{}=>{}({})",
                info.key_columns.join(","),
                reference_table,
                info.referenced_columns.join(",")
            ))
        }
    }
}

fn decode_constraint_payload(
    kind: ConstraintKind,
    payload: &str,
) -> Result<(Vec<String>, Option<String>, Vec<String>)> {
    match kind {
        ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
            Ok((split_column_list(payload), None, Vec::new()))
        }
        ConstraintKind::ForeignKey => {
            let (source, target) =
                payload
                    .split_once("=>")
                    .ok_or(RuseDbError::Corruption(format!(
                        "invalid FOREIGN KEY payload '{}'",
                        payload
                    )))?;
            let (table, columns) =
                target
                    .split_once('(')
                    .ok_or(RuseDbError::Corruption(format!(
                        "invalid FOREIGN KEY payload '{}'",
                        payload
                    )))?;
            let referenced_table = table.trim();
            let referenced_columns_raw =
                columns
                    .strip_suffix(')')
                    .ok_or(RuseDbError::Corruption(format!(
                        "invalid FOREIGN KEY payload '{}'",
                        payload
                    )))?;
            let key_columns = split_column_list(source);
            let referenced_columns = split_column_list(referenced_columns_raw);
            if key_columns.len() != referenced_columns.len() {
                return Err(RuseDbError::Corruption(format!(
                    "invalid FOREIGN KEY payload '{}': source/reference columns count mismatch",
                    payload
                )));
            }
            Ok((
                key_columns,
                Some(referenced_table.to_string()),
                referenced_columns,
            ))
        }
    }
}

fn split_column_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn rename_in_column_list(columns: &mut [String], old_normalized: &str, new_name: &str) -> bool {
    let mut changed = false;
    for column in columns.iter_mut() {
        if normalize_name(column) == old_normalized {
            *column = new_name.to_string();
            changed = true;
        }
    }
    changed
}

fn value_as_u32(value: &Value, column_name: &str) -> Result<u32> {
    match value {
        Value::BigInt(v) => u32::try_from(*v).map_err(|_| {
            RuseDbError::Corruption(format!(
                "column '{column_name}' value {} does not fit u32",
                v
            ))
        }),
        other => Err(RuseDbError::Corruption(format!(
            "column '{column_name}' expected BIGINT, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_string(value: &Value, column_name: &str) -> Result<String> {
    match value {
        Value::Varchar(v) => Ok(v.clone()),
        other => Err(RuseDbError::Corruption(format!(
            "column '{column_name}' expected VARCHAR, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_bool(value: &Value, column_name: &str) -> Result<bool> {
    match value {
        Value::Bool(v) => Ok(*v),
        other => Err(RuseDbError::Corruption(format!(
            "column '{column_name}' expected BOOL, got {}",
            other.type_name()
        ))),
    }
}

fn value_as_data_type(value: &Value, column_name: &str) -> Result<DataType> {
    let text = value_as_string(value, column_name)?;
    text.parse()
}
