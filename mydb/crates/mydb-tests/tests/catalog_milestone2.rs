use std::path::{Path, PathBuf};
use std::{fs, thread, time};

use mydb_core::{Column, DataType, Row, Schema, Value};
use mydb_storage::Catalog;

fn unique_test_dir(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let tick = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("mydb-{name}-{}-{tick}", std::process::id()));
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
    let base = dir.join("mydb");

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
    let base = dir.join("mydb");

    {
        let mut catalog = Catalog::open(&base).unwrap();
        catalog
            .create_table("logs", vec![Column::new("id", DataType::Int, false)])
            .unwrap();
        catalog.drop_table("logs").unwrap();
    }

    {
        let catalog = Catalog::open(&base).unwrap();
        assert!(catalog.list_tables().is_empty());
    }

    cleanup_dir(&dir);
}
