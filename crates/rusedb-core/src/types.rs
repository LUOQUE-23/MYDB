use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::{Result, RuseDbError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int,
    BigInt,
    Bool,
    Double,
    Varchar,
}

impl DataType {
    pub fn fixed_size(self) -> Option<usize> {
        match self {
            Self::Int => Some(4),
            Self::BigInt => Some(8),
            Self::Bool => Some(1),
            Self::Double => Some(8),
            Self::Varchar => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Bool => "BOOL",
            Self::Double => "DOUBLE",
            Self::Varchar => "VARCHAR",
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for DataType {
    type Err = RuseDbError;

    fn from_str(s: &str) -> Result<Self> {
        let normalized = s.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "INT" | "INTEGER" => Ok(Self::Int),
            "BIGINT" => Ok(Self::BigInt),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "DOUBLE" | "FLOAT" | "FLOAT64" => Ok(Self::Double),
            "VARCHAR" | "TEXT" | "STRING" => Ok(Self::Varchar),
            _ => Err(RuseDbError::Parse(format!("unknown data type: {s}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i32),
    BigInt(i64),
    Bool(bool),
    Double(f64),
    Varchar(String),
    Null,
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "INT",
            Self::BigInt(_) => "BIGINT",
            Self::Bool(_) => "BOOL",
            Self::Double(_) => "DOUBLE",
            Self::Varchar(_) => "VARCHAR",
            Self::Null => "NULL",
        }
    }
}
