use rusedb_core::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    CreateDatabase {
        name: String,
    },
    DropDatabase {
        name: String,
    },
    UseDatabase {
        name: String,
    },
    ShowDatabases,
    ShowCurrentDatabase,
    ShowTables,
    DropTable {
        name: String,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    CreateIndex {
        name: String,
        table: String,
        column: String,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select {
        table: String,
        joins: Vec<JoinClause>,
        projection: Vec<SelectItem>,
        selection: Option<Expr>,
        group_by: Vec<String>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
    },
    Delete {
        table: String,
        selection: Option<Expr>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<ReferenceDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReferenceDef {
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Column(String),
    Aggregate {
        func: AggregateFunction,
        column: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub table: String,
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Identifier(String),
    Literal(Literal),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}
