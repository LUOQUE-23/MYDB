mod ast;
mod lexer;
mod parser;

pub use ast::{
    AggregateFunction, AlterColumnAction, Assignment, BinaryOp, ColumnDef, Expr, JoinClause,
    JoinType, Literal, OrderByItem, ReferenceDef, SelectItem, Statement, TableConstraint,
    TableConstraintKind, UnaryOp,
};
pub use lexer::{Keyword, Span, Token, TokenKind};
pub use parser::parse_sql;

#[cfg(test)]
mod tests {
    use rusedb_core::DataType;

    use crate::{
        AggregateFunction, AlterColumnAction, BinaryOp, ColumnDef, Expr, JoinClause, JoinType,
        Literal, OrderByItem, SelectItem, Statement, TableConstraint, TableConstraintKind, UnaryOp,
        parse_sql,
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
                        nullable: false,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                        nullable: true,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    ColumnDef {
                        name: "active".to_string(),
                        data_type: DataType::Bool,
                        nullable: true,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                ],
                constraints: vec![],
            }
        );
    }

    #[test]
    fn parse_create_table_with_pk_and_unique() {
        let stmt = parse_sql(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR, CONSTRAINT uq_users_name UNIQUE (name))",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                name: "users".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::BigInt,
                        nullable: false,
                        primary_key: true,
                        unique: false,
                        references: None,
                    },
                    ColumnDef {
                        name: "email".to_string(),
                        data_type: DataType::Varchar,
                        nullable: true,
                        primary_key: false,
                        unique: true,
                        references: None,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Varchar,
                        nullable: true,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                ],
                constraints: vec![TableConstraint {
                    name: Some("uq_users_name".to_string()),
                    kind: TableConstraintKind::Unique(vec!["name".to_string()]),
                }],
            }
        );
    }

    #[test]
    fn parse_create_table_with_foreign_key() {
        let stmt = parse_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES users(id), CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();
        assert!(matches!(stmt, Statement::CreateTable { .. }));
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
                having: None,
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
                having: None,
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
                having: None,
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
                    kind: JoinType::Inner,
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
                having: None,
                order_by: vec![],
                limit: None,
            }
        );
    }

    #[test]
    fn parse_predicate_extensions_and_left_join_having() {
        let stmt = parse_sql(
            "SELECT * FROM users WHERE name LIKE 'a%' AND id IN (1, 2, 3) AND score BETWEEN 10 AND 20 AND deleted_at IS NULL",
        )
        .unwrap();
        assert!(matches!(stmt, Statement::Select { .. }));

        let stmt = parse_sql(
            "SELECT users.id, COUNT(*) FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id HAVING COUNT(*) > 1",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "users".to_string(),
                joins: vec![JoinClause {
                    kind: JoinType::Left,
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
                having: Some(Expr::Binary {
                    left: Box::new(Expr::Aggregate {
                        func: AggregateFunction::Count,
                        column: None,
                    }),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr::Literal(Literal::Integer(1))),
                }),
                order_by: vec![],
                limit: None,
            }
        );

        let stmt = parse_sql(
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount >= 10)",
        )
        .unwrap();
        assert!(matches!(
            stmt,
            Statement::Select {
                selection: Some(Expr::InSubquery { .. }),
                ..
            }
        ));

        let stmt = parse_sql(
            "SELECT id FROM users WHERE id = (SELECT user_id FROM orders WHERE amount >= 10)",
        )
        .unwrap();
        assert!(matches!(
            stmt,
            Statement::Select {
                selection: Some(Expr::Binary {
                    right,
                    op: BinaryOp::Eq,
                    ..
                }),
                ..
            } if matches!(*right, Expr::ScalarSubquery { .. })
        ));
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

    #[test]
    fn parse_database_statements() {
        assert_eq!(
            parse_sql("CREATE DATABASE appdb").unwrap(),
            Statement::CreateDatabase {
                name: "appdb".to_string()
            }
        );
        assert_eq!(
            parse_sql("DROP DATABASE appdb").unwrap(),
            Statement::DropDatabase {
                name: "appdb".to_string()
            }
        );
        assert_eq!(
            parse_sql("USE appdb;").unwrap(),
            Statement::UseDatabase {
                name: "appdb".to_string()
            }
        );
        assert_eq!(
            parse_sql("SHOW DATABASES").unwrap(),
            Statement::ShowDatabases
        );
        assert_eq!(parse_sql("SHOW TABLES").unwrap(), Statement::ShowTables);
        assert_eq!(
            parse_sql("SHOW CURRENT DATABASE").unwrap(),
            Statement::ShowCurrentDatabase
        );
        assert_eq!(
            parse_sql("DROP TABLE users").unwrap(),
            Statement::DropTable {
                name: "users".to_string()
            }
        );
        assert_eq!(
            parse_sql("ANALYZE TABLE users").unwrap(),
            Statement::AnalyzeTable {
                table: "users".to_string()
            }
        );
        assert!(matches!(
            parse_sql("EXPLAIN SELECT id FROM users").unwrap(),
            Statement::Explain { analyze: false, .. }
        ));
        assert!(matches!(
            parse_sql("EXPLAIN ANALYZE SELECT id FROM users").unwrap(),
            Statement::Explain { analyze: true, .. }
        ));
    }

    #[test]
    fn parse_alter_and_rename_statements() {
        assert_eq!(
            parse_sql("ALTER TABLE users ADD COLUMN age BIGINT").unwrap(),
            Statement::AlterTableAddColumn {
                table: "users".to_string(),
                column: ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::BigInt,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    references: None,
                },
            }
        );

        assert_eq!(
            parse_sql("ALTER TABLE users DROP COLUMN age").unwrap(),
            Statement::AlterTableDropColumn {
                table: "users".to_string(),
                column: "age".to_string(),
            }
        );

        assert_eq!(
            parse_sql("ALTER TABLE users ALTER COLUMN age TYPE DOUBLE").unwrap(),
            Statement::AlterTableAlterColumn {
                table: "users".to_string(),
                column: "age".to_string(),
                action: AlterColumnAction::SetDataType(DataType::Double),
            }
        );
        assert_eq!(
            parse_sql("ALTER TABLE users ALTER COLUMN age SET DATA TYPE BIGINT").unwrap(),
            Statement::AlterTableAlterColumn {
                table: "users".to_string(),
                column: "age".to_string(),
                action: AlterColumnAction::SetDataType(DataType::BigInt),
            }
        );
        assert_eq!(
            parse_sql("ALTER TABLE users ALTER COLUMN age SET NOT NULL").unwrap(),
            Statement::AlterTableAlterColumn {
                table: "users".to_string(),
                column: "age".to_string(),
                action: AlterColumnAction::SetNotNull,
            }
        );
        assert_eq!(
            parse_sql("ALTER TABLE users ALTER COLUMN age DROP NOT NULL").unwrap(),
            Statement::AlterTableAlterColumn {
                table: "users".to_string(),
                column: "age".to_string(),
                action: AlterColumnAction::DropNotNull,
            }
        );

        assert_eq!(
            parse_sql("RENAME TABLE users TO app_users").unwrap(),
            Statement::RenameTable {
                old_name: "users".to_string(),
                new_name: "app_users".to_string(),
            }
        );

        assert_eq!(
            parse_sql("RENAME COLUMN users.name TO full_name").unwrap(),
            Statement::RenameColumn {
                table: "users".to_string(),
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            }
        );

        assert_eq!(
            parse_sql("RENAME COLUMN users name TO full_name").unwrap(),
            Statement::RenameColumn {
                table: "users".to_string(),
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            }
        );
    }
}
