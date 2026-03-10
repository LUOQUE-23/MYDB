use std::fmt::{Display, Formatter};

use rusedb_core::{Result, RuseDbError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Identifier(String),
    Number(i64),
    StringLiteral(String),
    Keyword(Keyword),
    Comma,
    LParen,
    RParen,
    Star,
    Dot,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Minus,
    Semicolon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Explain,
    Analyze,
    Create,
    Drop,
    Alter,
    Rename,
    Begin,
    Commit,
    Rollback,
    Database,
    Databases,
    Tables,
    Current,
    Use,
    Show,
    Index,
    On,
    To,
    Table,
    Column,
    Add,
    Insert,
    Into,
    Values,
    Select,
    From,
    Join,
    Left,
    Group,
    Having,
    Where,
    Delete,
    Update,
    Set,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Null,
    True,
    False,
    Is,
    In,
    Like,
    Between,
    And,
    Or,
    Not,
    Primary,
    Key,
    Unique,
    Constraint,
    Foreign,
    References,
    Restrict,
}

impl Display for TokenKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(v) => write!(f, "identifier({v})"),
            Self::Number(v) => write!(f, "number({v})"),
            Self::StringLiteral(v) => write!(f, "string('{v}')"),
            Self::Keyword(v) => write!(f, "{v:?}"),
            Self::Comma => write!(f, ","),
            Self::LParen => write!(f, "("),
            Self::RParen => write!(f, ")"),
            Self::Star => write!(f, "*"),
            Self::Dot => write!(f, "."),
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::Lte => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Gte => write!(f, ">="),
            Self::Minus => write!(f, "-"),
            Self::Semicolon => write!(f, ";"),
        }
    }
}

pub fn lex(input: &str) -> Result<Vec<Token>> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut tokens = Vec::new();
    let mut i = 0usize;

    while i < chars.len() {
        let (start, ch) = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        if is_identifier_start(ch) {
            let mut j = i + 1;
            while j < chars.len() && is_identifier_continue(chars[j].1) {
                j += 1;
            }
            let end = char_byte_index(&chars, j, input.len());
            let text = &input[start..end];
            let kind = match keyword_of(text) {
                Some(keyword) => TokenKind::Keyword(keyword),
                None => TokenKind::Identifier(text.to_string()),
            };
            tokens.push(Token {
                kind,
                span: Span { start, end },
            });
            i = j;
            continue;
        }

        if ch.is_ascii_digit() || (ch == '-' && next_char_is_ascii_digit(&chars, i)) {
            let mut j = i + 1;
            while j < chars.len() && chars[j].1.is_ascii_digit() {
                j += 1;
            }
            let end = char_byte_index(&chars, j, input.len());
            let text = &input[start..end];
            let value = text.parse::<i64>().map_err(|_| {
                parse_error(input, start, &format!("invalid integer literal '{}'", text))
            })?;
            tokens.push(Token {
                kind: TokenKind::Number(value),
                span: Span { start, end },
            });
            i = j;
            continue;
        }

        if ch == '\'' {
            let mut j = i + 1;
            let mut out = String::new();
            let mut closed = false;
            while j < chars.len() {
                let c = chars[j].1;
                if c == '\'' {
                    if j + 1 < chars.len() && chars[j + 1].1 == '\'' {
                        out.push('\'');
                        j += 2;
                        continue;
                    }
                    let end = char_byte_index(&chars, j + 1, input.len());
                    tokens.push(Token {
                        kind: TokenKind::StringLiteral(out),
                        span: Span { start, end },
                    });
                    i = j + 1;
                    closed = true;
                    break;
                }
                out.push(c);
                j += 1;
            }
            if !closed {
                return Err(parse_error(input, start, "unterminated string literal"));
            }
            continue;
        }

        let two_char_kind = if i + 1 < chars.len() {
            match (ch, chars[i + 1].1) {
                ('<', '=') => Some(TokenKind::Lte),
                ('>', '=') => Some(TokenKind::Gte),
                ('!', '=') => Some(TokenKind::NotEq),
                ('<', '>') => Some(TokenKind::NotEq),
                _ => None,
            }
        } else {
            None
        };
        if let Some(kind) = two_char_kind {
            let end = char_byte_index(&chars, i + 2, input.len());
            tokens.push(Token {
                kind,
                span: Span { start, end },
            });
            i += 2;
            continue;
        }

        let kind = match ch {
            ',' => TokenKind::Comma,
            '(' => TokenKind::LParen,
            ')' => TokenKind::RParen,
            '*' => TokenKind::Star,
            '.' => TokenKind::Dot,
            '=' => TokenKind::Eq,
            '<' => TokenKind::Lt,
            '>' => TokenKind::Gt,
            '-' => TokenKind::Minus,
            ';' => TokenKind::Semicolon,
            _ => {
                return Err(parse_error(
                    input,
                    start,
                    &format!("unexpected character '{}'", ch),
                ));
            }
        };
        let end = char_byte_index(&chars, i + 1, input.len());
        tokens.push(Token {
            kind,
            span: Span { start, end },
        });
        i += 1;
    }

    Ok(tokens)
}

fn char_byte_index(chars: &[(usize, char)], i: usize, fallback: usize) -> usize {
    if i < chars.len() {
        chars[i].0
    } else {
        fallback
    }
}

fn keyword_of(text: &str) -> Option<Keyword> {
    let upper = text.to_ascii_uppercase();
    match upper.as_str() {
        "CREATE" => Some(Keyword::Create),
        "EXPLAIN" => Some(Keyword::Explain),
        "ANALYZE" => Some(Keyword::Analyze),
        "DROP" => Some(Keyword::Drop),
        "ALTER" => Some(Keyword::Alter),
        "RENAME" => Some(Keyword::Rename),
        "BEGIN" => Some(Keyword::Begin),
        "COMMIT" => Some(Keyword::Commit),
        "ROLLBACK" => Some(Keyword::Rollback),
        "DATABASE" => Some(Keyword::Database),
        "DATABASES" => Some(Keyword::Databases),
        "TABLES" => Some(Keyword::Tables),
        "CURRENT" => Some(Keyword::Current),
        "USE" => Some(Keyword::Use),
        "SHOW" => Some(Keyword::Show),
        "INDEX" => Some(Keyword::Index),
        "ON" => Some(Keyword::On),
        "TO" => Some(Keyword::To),
        "TABLE" => Some(Keyword::Table),
        "COLUMN" => Some(Keyword::Column),
        "ADD" => Some(Keyword::Add),
        "INSERT" => Some(Keyword::Insert),
        "INTO" => Some(Keyword::Into),
        "VALUES" => Some(Keyword::Values),
        "SELECT" => Some(Keyword::Select),
        "FROM" => Some(Keyword::From),
        "JOIN" => Some(Keyword::Join),
        "LEFT" => Some(Keyword::Left),
        "GROUP" => Some(Keyword::Group),
        "HAVING" => Some(Keyword::Having),
        "WHERE" => Some(Keyword::Where),
        "DELETE" => Some(Keyword::Delete),
        "UPDATE" => Some(Keyword::Update),
        "SET" => Some(Keyword::Set),
        "ORDER" => Some(Keyword::Order),
        "BY" => Some(Keyword::By),
        "ASC" => Some(Keyword::Asc),
        "DESC" => Some(Keyword::Desc),
        "LIMIT" => Some(Keyword::Limit),
        "NULL" => Some(Keyword::Null),
        "TRUE" => Some(Keyword::True),
        "FALSE" => Some(Keyword::False),
        "IS" => Some(Keyword::Is),
        "IN" => Some(Keyword::In),
        "LIKE" => Some(Keyword::Like),
        "BETWEEN" => Some(Keyword::Between),
        "AND" => Some(Keyword::And),
        "OR" => Some(Keyword::Or),
        "NOT" => Some(Keyword::Not),
        "PRIMARY" => Some(Keyword::Primary),
        "KEY" => Some(Keyword::Key),
        "UNIQUE" => Some(Keyword::Unique),
        "CONSTRAINT" => Some(Keyword::Constraint),
        "FOREIGN" => Some(Keyword::Foreign),
        "REFERENCES" => Some(Keyword::References),
        "RESTRICT" => Some(Keyword::Restrict),
        _ => None,
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn next_char_is_ascii_digit(chars: &[(usize, char)], i: usize) -> bool {
    i + 1 < chars.len() && chars[i + 1].1.is_ascii_digit()
}

pub fn parse_error(input: &str, pos: usize, message: &str) -> RuseDbError {
    let start = pos.saturating_sub(16);
    let end = (pos + 16).min(input.len());
    let snippet = input[start..end].replace('\n', " ");
    RuseDbError::Parse(format!("{message} at byte {pos} near `{snippet}`"))
}
