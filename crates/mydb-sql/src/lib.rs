mod ast;
mod lexer;
mod parser;

pub use ast::{
    AggregateFunction, Assignment, BinaryOp, ColumnDef, Expr, JoinClause, Literal, OrderByItem,
    SelectItem, Statement, UnaryOp,
};
pub use lexer::{Keyword, Span, Token, TokenKind};
pub use parser::parse_sql;

#[cfg(test)]
mod tests {
    use mydb_core::DataType;

    use crate::{
        AggregateFunction, BinaryOp, ColumnDef, Expr, JoinClause, Literal, OrderByItem, SelectItem,
        Statement, UnaryOp, parse_sql,
    };

    #[test]
    fn parse_create_table() {
        let stmt = parse_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR, active BOOL)")
            .unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::BigInt,
                        nullable: false
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                        nullable: true
                    },
                    ColumnDef {
                        name: "active".to_string(),
                        data_type: DataType::Bool,
                        nullable: true
                    },
                ]
            }
        );
    }

    #[test]
    fn parse_create_index() {
        let stmt = parse_sql("CREATE INDEX idx_users_id ON users (id)").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateIndex {
                name: "idx_users_id".to_string(),
                table: "users".to_string(),
                column: "id".to_string()
            }
        );
    }

    #[test]
    fn parse_insert_multi_rows() {
        let stmt =
            parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").unwrap();
        assert_eq!(
            stmt,
            Statement::Insert {
                table: "users".to_string(),
                columns: Some(vec!["id".to_string(), "name".to_string()]),
                rows: vec![
                    vec![
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("alice".to_string()))
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::String("bob".to_string()))
                    ]
                ]
            }
        );
    }

    #[test]
    fn parse_select_where() {
        let stmt = parse_sql(
            "SELECT id, name FROM users WHERE id >= 10 AND name != 'test' OR active = true",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "users".to_string(),
                joins: vec![],
                projection: vec![
                    SelectItem::Column("id".to_string()),
                    SelectItem::Column("name".to_string())
                ],
                selection: Some(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(Expr::Identifier("id".to_string())),
                            op: BinaryOp::Gte,
                            right: Box::new(Expr::Literal(Literal::Integer(10)))
                        }),
                        op: BinaryOp::And,
                        right: Box::new(Expr::Binary {
                            left: Box::new(Expr::Identifier("name".to_string())),
                            op: BinaryOp::NotEq,
                            right: Box::new(Expr::Literal(Literal::String("test".to_string())))
                        })
                    }),
                    op: BinaryOp::Or,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Identifier("active".to_string())),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::Literal(Literal::Bool(true)))
                    })
                }),
                group_by: vec![],
                order_by: vec![],
                limit: None,
            }
        );
    }

    #[test]
    fn parse_delete_and_update() {
        let delete_stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(delete_stmt, Statement::Delete { .. }));

        let update_stmt =
            parse_sql("UPDATE users SET name = 'eve', active = false WHERE id = 1").unwrap();
        assert!(matches!(update_stmt, Statement::Update { .. }));
    }

    #[test]
    fn parse_unary_expression() {
        let stmt = parse_sql("SELECT * FROM t WHERE NOT active AND score >= -10").unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "t".to_string(),
                joins: vec![],
                projection: vec![SelectItem::Wildcard],
                selection: Some(Expr::Binary {
                    left: Box::new(Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(Expr::Identifier("active".to_string()))
                    }),
                    op: BinaryOp::And,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Identifier("score".to_string())),
                        op: BinaryOp::Gte,
                        right: Box::new(Expr::Literal(Literal::Integer(-10)))
                    })
                }),
                group_by: vec![],
                order_by: vec![],
                limit: None,
            }
        );
    }

    #[test]
    fn parse_select_order_by_limit() {
        let stmt = parse_sql(
            "SELECT id, name FROM users WHERE active = true ORDER BY name DESC, id ASC LIMIT 3",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "users".to_string(),
                joins: vec![],
                projection: vec![
                    SelectItem::Column("id".to_string()),
                    SelectItem::Column("name".to_string()),
                ],
                selection: Some(Expr::Binary {
                    left: Box::new(Expr::Identifier("active".to_string())),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(true))),
                }),
                group_by: vec![],
                order_by: vec![
                    OrderByItem {
                        column: "name".to_string(),
                        descending: true
                    },
                    OrderByItem {
                        column: "id".to_string(),
                        descending: false
                    }
                ],
                limit: Some(3),
            }
        );
    }

    #[test]
    fn parse_select_join_and_group_aggregate() {
        let stmt = parse_sql(
            "SELECT users.id, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.id",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "users".to_string(),
                joins: vec![JoinClause {
                    table: "orders".to_string(),
                    on: Expr::Binary {
                        left: Box::new(Expr::Identifier("users.id".to_string())),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::Identifier("orders.user_id".to_string())),
                    },
                }],
                projection: vec![
                    SelectItem::Column("users.id".to_string()),
                    SelectItem::Aggregate {
                        func: AggregateFunction::Count,
                        column: None,
                    },
                ],
                selection: None,
                group_by: vec!["users.id".to_string()],
                order_by: vec![],
                limit: None,
            }
        );
    }

    #[test]
    fn parse_reports_position() {
        let err = parse_sql("SELECT FROM users").unwrap_err().to_string();
        assert!(err.contains("at byte"));
        assert!(err.contains("near"));
    }

    #[test]
    fn parse_transaction_statements() {
        assert_eq!(parse_sql("BEGIN").unwrap(), Statement::Begin);
        assert_eq!(parse_sql("COMMIT;").unwrap(), Statement::Commit);
        assert_eq!(parse_sql("ROLLBACK").unwrap(), Statement::Rollback);
    }
}
