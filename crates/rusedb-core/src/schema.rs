use crate::{DataType, Result, RuseDbError};

pub type TableId = u32;
pub type ColumnId = u32;
pub type IndexId = u32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            id: 0,
            name: name.into(),
            data_type,
            nullable,
        }
    }

    pub fn with_id(
        id: ColumnId,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Result<Self> {
        if columns.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "schema must have at least one column".to_string(),
            ));
        }

        let mut names = std::collections::HashSet::new();
        for column in &columns {
            if column.name.trim().is_empty() {
                return Err(RuseDbError::InvalidSchema(
                    "column name cannot be empty".to_string(),
                ));
            }
            let normalized = column.name.to_ascii_lowercase();
            if !names.insert(normalized) {
                return Err(RuseDbError::InvalidSchema(format!(
                    "duplicate column name '{}'",
                    column.name
                )));
            }
        }

        Ok(Self { columns })
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    pub fn find_column(&self, name: &str) -> Option<(usize, &Column)> {
        let normalized = name.to_ascii_lowercase();
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.to_ascii_lowercase() == normalized)
    }

    pub fn fixed_area_len(&self) -> usize {
        self.columns
            .iter()
            .filter_map(|c| c.data_type.fixed_size())
            .sum()
    }

    pub fn variable_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.data_type == DataType::Varchar)
            .count()
    }

    pub fn null_bitmap_len(&self) -> usize {
        self.len().div_ceil(8)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub table_id: TableId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub index_id: IndexId,
    pub table_id: TableId,
    pub name: String,
    pub key_columns: String,
}
