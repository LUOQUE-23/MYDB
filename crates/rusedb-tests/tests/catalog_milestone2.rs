use std::path::{Path, PathBuf};
use std::{fs, thread, time};

use rusedb_core::{Column, DataType, Row, Schema, Value};
use rusedb_storage::{Catalog, ConstraintDef, ConstraintKind};

fn unique_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let tick = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("rusedb-{name}-{}-{tick}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn cleanup_dir(path: &Path) {
    for _ in 0..5 {
        if fs::remove_dir_all(path).is_ok() {
            return;
        }
        thread::sleep(time::Duration::from_millis(50));
    }
}

#[test]
fn row_encode_decode_roundtrip() {
    let schema = Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("nickname", DataType::Varchar, true),
        Column::new("active", DataType::Bool, false),
        Column::new("score", DataType::Double, true),
    ])
    .unwrap();

    let row = Row::new(vec![
        Value::Int(42),
        Value::Null,
        Value::Bool(true),
        Value::Double(9.5),
    ]);
    let bytes = row.encode(&schema).unwrap();
    let decoded = Row::decode(&schema, &bytes).unwrap();
    assert_eq!(decoded, row);
}

#[test]
fn catalog_persists_tables_and_schema_across_restart() {
    let dir = unique_test_dir("catalog-restart");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("name", DataType::Varchar, false),
                    Column::new("email", DataType::Varchar, true),
                    Column::new("is_admin", DataType::Bool, false),
                ],
                Vec::new(),
            )
            .unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");

        let schema = catalog.describe_table("users").unwrap();
        assert_eq!(schema.columns.len(), 4);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, DataType::BigInt);
        assert!(schema.columns[2].nullable);
        assert!(schema.columns[0].id > 0);
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_drop_table_persists() {
    let dir = unique_test_dir("catalog-drop");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "logs",
                vec![Column::new("id", DataType::Int, false)],
                Vec::new(),
            )
            .unwrap();
        catalog.drop_table("logs").unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        assert!(catalog.list_tables().is_empty());
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_persists_constraints_across_restart() {
    let dir = unique_test_dir("catalog-constraints");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("email", DataType::Varchar, true),
                ],
                vec![
                    ConstraintDef {
                        name: "pk_users".to_string(),
                        kind: ConstraintKind::PrimaryKey,
                        key_columns: vec!["id".to_string()],
                        referenced_table: None,
                        referenced_columns: Vec::new(),
                    },
                    ConstraintDef {
                        name: "uq_users_email".to_string(),
                        kind: ConstraintKind::Unique,
                        key_columns: vec!["email".to_string()],
                        referenced_table: None,
                        referenced_columns: Vec::new(),
                    },
                ],
            )
            .unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let constraints = catalog.list_constraints("users").unwrap();
        assert_eq!(constraints.len(), 2);
        assert!(constraints.iter().any(|c| {
            matches!(c.kind, ConstraintKind::PrimaryKey)
                && c.name == "pk_users"
                && c.key_columns == vec!["id".to_string()]
        }));
        assert!(constraints.iter().any(|c| {
            matches!(c.kind, ConstraintKind::Unique)
                && c.name == "uq_users_email"
                && c.key_columns == vec!["email".to_string()]
        }));
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_persists_foreign_key_constraints_across_restart() {
    let dir = unique_test_dir("catalog-fk");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "users",
                vec![Column::new("id", DataType::BigInt, false)],
                vec![ConstraintDef {
                    name: "pk_users".to_string(),
                    kind: ConstraintKind::PrimaryKey,
                    key_columns: vec!["id".to_string()],
                    referenced_table: None,
                    referenced_columns: Vec::new(),
                }],
            )
            .unwrap();
        catalog
            .create_table(
                "orders",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("user_id", DataType::BigInt, true),
                ],
                vec![
                    ConstraintDef {
                        name: "pk_orders".to_string(),
                        kind: ConstraintKind::PrimaryKey,
                        key_columns: vec!["id".to_string()],
                        referenced_table: None,
                        referenced_columns: Vec::new(),
                    },
                    ConstraintDef {
                        name: "fk_orders_user".to_string(),
                        kind: ConstraintKind::ForeignKey,
                        key_columns: vec!["user_id".to_string()],
                        referenced_table: Some("users".to_string()),
                        referenced_columns: vec!["id".to_string()],
                    },
                ],
            )
            .unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let constraints = catalog.list_constraints("orders").unwrap();
        assert_eq!(constraints.len(), 2);
        let fk = constraints
            .iter()
            .find(|c| c.name == "fk_orders_user")
            .unwrap();
        assert!(matches!(fk.kind, ConstraintKind::ForeignKey));
        assert_eq!(fk.key_columns, vec!["user_id".to_string()]);
        assert_eq!(fk.referenced_table.as_deref(), Some("users"));
        assert_eq!(fk.referenced_columns, vec!["id".to_string()]);
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_add_drop_column_persists() {
    let dir = unique_test_dir("catalog-alter-columns");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "logs",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("message", DataType::Varchar, true),
                ],
                Vec::new(),
            )
            .unwrap();
        catalog
            .add_column("logs", Column::new("created_at", DataType::BigInt, true))
            .unwrap();
        catalog.drop_column("logs", "message").unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let schema = catalog.describe_table("logs").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[1].name, "created_at");
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_rename_table_and_column_updates_foreign_key_reference() {
    let dir = unique_test_dir("catalog-rename");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "users",
                vec![Column::new("id", DataType::BigInt, false)],
                vec![ConstraintDef {
                    name: "pk_users".to_string(),
                    kind: ConstraintKind::PrimaryKey,
                    key_columns: vec!["id".to_string()],
                    referenced_table: None,
                    referenced_columns: Vec::new(),
                }],
            )
            .unwrap();
        catalog
            .create_table(
                "orders",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("user_id", DataType::BigInt, true),
                ],
                vec![
                    ConstraintDef {
                        name: "pk_orders".to_string(),
                        kind: ConstraintKind::PrimaryKey,
                        key_columns: vec!["id".to_string()],
                        referenced_table: None,
                        referenced_columns: Vec::new(),
                    },
                    ConstraintDef {
                        name: "fk_orders_user".to_string(),
                        kind: ConstraintKind::ForeignKey,
                        key_columns: vec!["user_id".to_string()],
                        referenced_table: Some("users".to_string()),
                        referenced_columns: vec!["id".to_string()],
                    },
                ],
            )
            .unwrap();

        catalog.rename_table("users", "accounts").unwrap();
        catalog
            .rename_column("accounts", "id", "account_id")
            .unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let users_constraints = catalog.list_constraints("accounts").unwrap();
        assert!(users_constraints.iter().any(|c| {
            matches!(c.kind, ConstraintKind::PrimaryKey)
                && c.key_columns == vec!["account_id".to_string()]
        }));

        let order_constraints = catalog.list_constraints("orders").unwrap();
        let fk = order_constraints
            .iter()
            .find(|c| c.name == "fk_orders_user")
            .unwrap();
        assert_eq!(fk.referenced_table.as_deref(), Some("accounts"));
        assert_eq!(fk.referenced_columns, vec!["account_id".to_string()]);
    }

    cleanup_dir(&dir);
}

#[test]
fn catalog_alter_column_type_and_nullability_persists() {
    let dir = unique_test_dir("catalog-alter-column");
    let base = dir.join("rusedb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table(
                "metrics",
                vec![
                    Column::new("id", DataType::BigInt, false),
                    Column::new("score", DataType::BigInt, true),
                ],
                Vec::new(),
            )
            .unwrap();
        catalog
            .alter_column("metrics", "score", Some(DataType::Double), Some(false))
            .unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        let schema = catalog.describe_table("metrics").unwrap();
        let (_, score) = schema.find_column("score").unwrap();
        assert_eq!(score.data_type, DataType::Double);
        assert!(!score.nullable);
    }

    cleanup_dir(&dir);
}
