use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum RuseDbError {
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

impl RuseDbError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "RDB-IO-001",
            Self::Corruption(_) => "RDB-CORRUPTION-001",
            Self::InvalidSchema(_) => "RDB-SCHEMA-001",
            Self::Parse(_) => "RDB-PARSE-001",
            Self::TypeMismatch { .. } => "RDB-TYPE-001",
            Self::NullConstraintViolation { .. } => "RDB-NULL-001",
            Self::AlreadyExists { .. } => "RDB-EXISTS-001",
            Self::NotFound { .. } => "RDB-NOTFOUND-001",
            Self::RecordTooLarge { .. } => "RDB-RECORD-001",
            Self::PageFull { .. } | Self::PageOutOfRange { .. } | Self::InvalidRid { .. } => {
                "RDB-PAGE-001"
            }
        }
    }
}

impl Display for RuseDbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let code = self.code();
        match self {
            Self::Io(err) => write!(f, "[{code}] io error: {err}"),
            Self::Corruption(msg) => write!(f, "[{code}] corrupted page layout: {msg}"),
            Self::InvalidSchema(msg) => write!(f, "[{code}] invalid schema: {msg}"),
            Self::Parse(msg) => write!(f, "[{code}] parse error: {msg}"),
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => write!(
                f,
                "[{code}] type mismatch on column '{column}': expected {expected}, got {actual}"
            ),
            Self::NullConstraintViolation { column } => {
                write!(f, "[{code}] column '{column}' does not allow NULL")
            }
            Self::AlreadyExists { object, name } => {
                write!(f, "[{code}] {object} '{name}' already exists")
            }
            Self::NotFound { object, name } => write!(f, "[{code}] {object} '{name}' not found"),
            Self::RecordTooLarge { size } => write!(f, "[{code}] record too large: {size} bytes"),
            Self::PageFull { page_id } => {
                write!(f, "[{code}] page {page_id} does not have enough free space")
            }
            Self::PageOutOfRange {
                page_id,
                page_count,
            } => write!(
                f,
                "[{code}] page id out of range: requested={page_id}, page_count={page_count}"
            ),
            Self::InvalidRid { page_id, slot_id } => {
                write!(f, "[{code}] invalid rid: page={page_id}, slot={slot_id}")
            }
        }
    }
}

impl std::error::Error for RuseDbError {}

impl From<std::io::Error> for RuseDbError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, RuseDbError>;
