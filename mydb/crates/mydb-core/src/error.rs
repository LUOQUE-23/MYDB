use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum MyDbError {
    Io(std::io::Error),
    Corruption(String),
    InvalidSchema(String),
    Parse(String),
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },
    NullConstraintViolation {
        column: String,
    },
    AlreadyExists {
        object: String,
        name: String,
    },
    NotFound {
        object: String,
        name: String,
    },
    RecordTooLarge {
        size: usize,
    },
    PageFull {
        page_id: u32,
    },
    PageOutOfRange {
        page_id: u32,
        page_count: u32,
    },
    InvalidRid {
        page_id: u32,
        slot_id: u16,
    },
}

impl Display for MyDbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Corruption(msg) => write!(f, "corrupted page layout: {msg}"),
            Self::InvalidSchema(msg) => write!(f, "invalid schema: {msg}"),
            Self::Parse(msg) => write!(f, "parse error: {msg}"),
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => write!(
                f,
                "type mismatch on column '{column}': expected {expected}, got {actual}"
            ),
            Self::NullConstraintViolation { column } => {
                write!(f, "column '{column}' does not allow NULL")
            }
            Self::AlreadyExists { object, name } => {
                write!(f, "{object} '{name}' already exists")
            }
            Self::NotFound { object, name } => write!(f, "{object} '{name}' not found"),
            Self::RecordTooLarge { size } => write!(f, "record too large: {size} bytes"),
            Self::PageFull { page_id } => {
                write!(f, "page {page_id} does not have enough free space")
            }
            Self::PageOutOfRange {
                page_id,
                page_count,
            } => write!(
                f,
                "page id out of range: requested={page_id}, page_count={page_count}"
            ),
            Self::InvalidRid { page_id, slot_id } => {
                write!(f, "invalid rid: page={page_id}, slot={slot_id}")
            }
        }
    }
}

impl std::error::Error for MyDbError {}

impl From<std::io::Error> for MyDbError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, MyDbError>;
