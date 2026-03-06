use mydb_core::{DataType, MyDbError, Result};

use crate::ast::{
    AggregateFunction, Assignment, BinaryOp, ColumnDef, Expr, JoinClause, Literal, OrderByItem,
    SelectItem, Statement, UnaryOp,
};
use crate::lexer::{Keyword, Token, TokenKind, lex, parse_error};

pub fn parse_sql(input: &str) -> Result<Statement> {
    let tokens = lex(input)?;
    let mut parser = Parser::new(input, tokens);
    parser.parse_statement()
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, tokens: Vec<Token>) -> Self {
        Self {
            input,
            tokens,
            cursor: 0,
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let stmt = match self.peek_keyword() {
            Some(Keyword::Begin) => {
                self.expect_keyword(Keyword::Begin)?;
                Statement::Begin
            }
            Some(Keyword::Commit) => {
                self.expect_keyword(Keyword::Commit)?;
                Statement::Commit
            }
            Some(Keyword::Rollback) => {
                self.expect_keyword(Keyword::Rollback)?;
                Statement::Rollback
            }
            Some(Keyword::Create) => self.parse_create()?,
            Some(Keyword::Insert) => self.parse_insert()?,
            Some(Keyword::Select) => self.parse_select()?,
            Some(Keyword::Delete) => self.parse_delete()?,
            Some(Keyword::Update) => self.parse_update()?,
            _ => return Err(self.error_here("expected SQL statement")),
        };

        self.consume_kind(&TokenKind::Semicolon);
        if self.peek().is_some() {
            return Err(self.error_here("unexpected token after statement"));
        }
        Ok(stmt)
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        match self.peek_keyword() {
            Some(Keyword::Table) => self.parse_create_table(),
            Some(Keyword::Index) => self.parse_create_index(),
            _ => Err(self.error_here("expected TABLE or INDEX after CREATE")),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Table)?;
        let name = self.parse_identifier_path()?;
        self.expect_kind(&TokenKind::LParen)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_column_def()?);
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }
        self.expect_kind(&TokenKind::RParen)?;

        Ok(Statement::CreateTable { name, columns })
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Index)?;
        let name = self.parse_identifier_path()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.parse_identifier_path()?;
        self.expect_kind(&TokenKind::LParen)?;
        let column = self.parse_identifier_path()?;
        self.expect_kind(&TokenKind::RParen)?;
        Ok(Statement::CreateIndex {
            name,
            table,
            column,
        })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_identifier_path()?;

        let columns = if self.consume_kind(&TokenKind::LParen) {
            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_identifier_path()?);
                if self.consume_kind(&TokenKind::Comma) {
                    continue;
                }
                break;
            }
            self.expect_kind(&TokenKind::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect_kind(&TokenKind::LParen)?;
            let mut values = Vec::new();
            loop {
                values.push(self.parse_expr()?);
                if self.consume_kind(&TokenKind::Comma) {
                    continue;
                }
                break;
            }
            self.expect_kind(&TokenKind::RParen)?;
            rows.push(values);
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }

        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;

        let mut projection = Vec::new();
        loop {
            projection.push(self.parse_select_item()?);
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }

        self.expect_keyword(Keyword::From)?;
        let table = self.parse_identifier_path()?;
        let mut joins = Vec::new();
        while self.consume_keyword(Keyword::Join) {
            let join_table = self.parse_identifier_path()?;
            self.expect_keyword(Keyword::On)?;
            let on = self.parse_expr()?;
            joins.push(JoinClause {
                table: join_table,
                on,
            });
        }
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.consume_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_group_by_items()?
        } else {
            Vec::new()
        };
        let order_by = if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            self.parse_order_by_items()?
        } else {
            Vec::new()
        };
        let limit = if self.consume_keyword(Keyword::Limit) {
            Some(self.parse_limit_value()?)
        } else {
            None
        };

        Ok(Statement::Select {
            table,
            joins,
            projection,
            selection,
            group_by,
            order_by,
            limit,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.parse_identifier_path()?;
        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete { table, selection })
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;
        let table = self.parse_identifier_path()?;
        self.expect_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.parse_identifier_path()?;
            self.expect_kind(&TokenKind::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }

        let selection = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            selection,
        })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.parse_identifier_path()?;
        let type_name = self.parse_type_name()?;
        let data_type: DataType = type_name.parse()?;

        let nullable = if self.consume_keyword(Keyword::Not) {
            self.expect_keyword(Keyword::Null)?;
            false
        } else {
            self.consume_keyword(Keyword::Null);
            true
        };

        Ok(ColumnDef {
            name,
            data_type,
            nullable,
        })
    }

    fn parse_type_name(&mut self) -> Result<String> {
        let token = self.next().ok_or(self.error_here("expected type name"))?;
        match &token.kind {
            TokenKind::Identifier(name) => Ok(name.clone()),
            TokenKind::Keyword(keyword) => Ok(format!("{keyword:?}").to_ascii_uppercase()),
            _ => Err(self.error_at(
                token.span.start,
                &format!("expected type name, got {}", token.kind),
            )),
        }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.consume_keyword(Keyword::Or) {
            let right = self.parse_and()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while self.consume_keyword(Keyword::And) {
            let right = self.parse_comparison()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        while let Some(op) = self.try_comparison_op() {
            let right = self.parse_unary()?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.consume_keyword(Keyword::Not) {
            let expr = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        if self.consume_kind(&TokenKind::Minus) {
            let expr = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        let token = self.next().ok_or(self.error_here("expected expression"))?;
        match token.kind {
            TokenKind::Identifier(name) => {
                let mut full = name;
                while self.consume_kind(&TokenKind::Dot) {
                    let part = self.parse_identifier_path_segment()?;
                    full.push('.');
                    full.push_str(&part);
                }
                Ok(Expr::Identifier(full))
            }
            TokenKind::Number(v) => Ok(Expr::Literal(Literal::Integer(v))),
            TokenKind::StringLiteral(v) => Ok(Expr::Literal(Literal::String(v))),
            TokenKind::Keyword(Keyword::Null) => Ok(Expr::Literal(Literal::Null)),
            TokenKind::Keyword(Keyword::True) => Ok(Expr::Literal(Literal::Bool(true))),
            TokenKind::Keyword(Keyword::False) => Ok(Expr::Literal(Literal::Bool(false))),
            TokenKind::LParen => {
                let expr = self.parse_expr()?;
                self.expect_kind(&TokenKind::RParen)?;
                Ok(expr)
            }
            other => Err(self.error_at(
                token.span.start,
                &format!("expected expression, got {other}"),
            )),
        }
    }

    fn try_comparison_op(&mut self) -> Option<BinaryOp> {
        let kind = self.peek()?;
        let op = match kind.kind {
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::NotEq => BinaryOp::NotEq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Lte => BinaryOp::Lte,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::Gte => BinaryOp::Gte,
            _ => return None,
        };
        self.cursor += 1;
        Some(op)
    }

    fn parse_identifier_path(&mut self) -> Result<String> {
        let mut name = self.parse_identifier_path_segment()?;
        while self.consume_kind(&TokenKind::Dot) {
            let segment = self.parse_identifier_path_segment()?;
            name.push('.');
            name.push_str(&segment);
        }
        Ok(name)
    }

    fn parse_identifier_path_segment(&mut self) -> Result<String> {
        let token = self.next().ok_or(self.error_here("expected identifier"))?;
        match token.kind {
            TokenKind::Identifier(name) => Ok(name),
            _ => Err(self.error_at(
                token.span.start,
                &format!("expected identifier, got {}", token.kind),
            )),
        }
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        if self.consume_kind(&TokenKind::Star) {
            return Ok(SelectItem::Wildcard);
        }

        let token = self.next().ok_or(self.error_here("expected select item"))?;
        let kind = token.kind.clone();
        let identifier = match kind {
            TokenKind::Identifier(identifier) => identifier,
            other => {
                return Err(self.error_at(
                    token.span.start,
                    &format!("expected column or aggregate function, got {other}"),
                ));
            }
        };

        if !self.consume_kind(&TokenKind::LParen) {
            let mut name = identifier;
            while self.consume_kind(&TokenKind::Dot) {
                let segment = self.parse_identifier_path_segment()?;
                name.push('.');
                name.push_str(&segment);
            }
            return Ok(SelectItem::Column(name));
        }

        let func = parse_aggregate_function(&identifier)
            .ok_or_else(|| self.error_at(token.span.start, "unknown aggregate function"))?;
        let column = if self.consume_kind(&TokenKind::Star) {
            if !matches!(func, AggregateFunction::Count) {
                return Err(
                    self.error_at(token.span.start, "only COUNT supports wildcard argument")
                );
            }
            None
        } else {
            Some(self.parse_identifier_path()?)
        };
        self.expect_kind(&TokenKind::RParen)?;
        Ok(SelectItem::Aggregate { func, column })
    }

    fn parse_group_by_items(&mut self) -> Result<Vec<String>> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_identifier_path()?);
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_order_by_items(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();
        loop {
            let column = self.parse_identifier_path()?;
            let descending = if self.consume_keyword(Keyword::Desc) {
                true
            } else {
                self.consume_keyword(Keyword::Asc);
                false
            };
            items.push(OrderByItem { column, descending });
            if self.consume_kind(&TokenKind::Comma) {
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_limit_value(&mut self) -> Result<usize> {
        let token = self.next().ok_or(self.error_here("expected LIMIT value"))?;
        match token.kind {
            TokenKind::Number(v) => {
                if v < 0 {
                    return Err(self.error_at(token.span.start, "LIMIT cannot be negative"));
                }
                usize::try_from(v)
                    .map_err(|_| self.error_at(token.span.start, "LIMIT value is too large"))
            }
            _ => Err(self.error_at(token.span.start, "expected numeric LIMIT value")),
        }
    }

    fn peek_keyword(&self) -> Option<Keyword> {
        match self.peek()?.kind {
            TokenKind::Keyword(keyword) => Some(keyword),
            _ => None,
        }
    }

    fn consume_keyword(&mut self, expected: Keyword) -> bool {
        if self.peek_keyword() == Some(expected) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(self.error_here(&format!("expected keyword {:?}", expected)))
        }
    }

    fn consume_kind(&mut self, expected: &TokenKind) -> bool {
        if self
            .peek()
            .map(|token| same_token_variant(&token.kind, expected))
            .unwrap_or(false)
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_kind(&mut self, expected: &TokenKind) -> Result<()> {
        if self.consume_kind(expected) {
            Ok(())
        } else {
            Err(self.error_here(&format!("expected token {}", expected)))
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.cursor).cloned();
        if token.is_some() {
            self.cursor += 1;
        }
        token
    }

    fn error_here(&self, message: &str) -> MyDbError {
        let pos = self
            .peek()
            .map(|token| token.span.start)
            .unwrap_or(self.input.len());
        self.error_at(pos, message)
    }

    fn error_at(&self, pos: usize, message: &str) -> MyDbError {
        parse_error(self.input, pos, message)
    }
}

fn same_token_variant(actual: &TokenKind, expected: &TokenKind) -> bool {
    use TokenKind::*;
    matches!(
        (actual, expected),
        (Comma, Comma)
            | (LParen, LParen)
            | (RParen, RParen)
            | (Star, Star)
            | (Dot, Dot)
            | (Eq, Eq)
            | (NotEq, NotEq)
            | (Lt, Lt)
            | (Lte, Lte)
            | (Gt, Gt)
            | (Gte, Gte)
            | (Minus, Minus)
            | (Semicolon, Semicolon)
    )
}

fn parse_aggregate_function(name: &str) -> Option<AggregateFunction> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggregateFunction::Count),
        "SUM" => Some(AggregateFunction::Sum),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        _ => None,
    }
}
