use std::path::{Path, PathBuf};
use std::{fs, thread, time};

use rusedb_core::Value;
use rusedb_exec::{Engine, Executor, QueryResult};

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
fn sql_engine_create_insert_select_delete() {
    let dir = unique_test_dir("sql-engine");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    let create = engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR, active BOOL)")
        .unwrap();
    assert!(matches!(create, QueryResult::Message(_)));

    let inserted = engine
        .execute_sql(
            "INSERT INTO users (id, name, active) VALUES (1, 'alice', true), (2, 'bob', false)",
        )
        .unwrap();
    assert_eq!(inserted, QueryResult::AffectedRows(2));

    let create_index = engine
        .execute_sql("CREATE INDEX idx_users_id ON users (id)")
        .unwrap();
    assert!(matches!(create_index, QueryResult::Message(_)));

    let sorted_limited = engine
        .execute_sql("SELECT id, name FROM users ORDER BY id DESC LIMIT 1")
        .unwrap();
    assert_eq!(
        sorted_limited,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(2), Value::Varchar("bob".to_string())]]
        }
    );

    let selected = engine
        .execute_sql("SELECT id, name FROM users WHERE active = true")
        .unwrap();
    assert_eq!(
        selected,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("alice".to_string())]]
        }
    );

    let deleted = engine
        .execute_sql("DELETE FROM users WHERE id = 2")
        .unwrap();
    assert_eq!(deleted, QueryResult::AffectedRows(1));

    let selected_eq = engine
        .execute_sql("SELECT id, name FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(
        selected_eq,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("alice".to_string())]]
        }
    );

    let selected_after_delete = engine.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(
        selected_after_delete,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string(), "active".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("alice".to_string()),
                Value::Bool(true)
            ]]
        }
    );

    let updated = engine
        .execute_sql("UPDATE users SET id = 10, name = 'alice-10' WHERE id = 1")
        .unwrap();
    assert_eq!(updated, QueryResult::AffectedRows(1));

    let selected_new_id = engine
        .execute_sql("SELECT id, name FROM users WHERE id = 10")
        .unwrap();
    assert_eq!(
        selected_new_id,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![
                Value::BigInt(10),
                Value::Varchar("alice-10".to_string())
            ]]
        }
    );

    let selected_old_id = engine
        .execute_sql("SELECT id, name FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(
        selected_old_id,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![]
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_transaction_commit_rollback_and_recovery() {
    let dir = unique_test_dir("sql-engine-tx");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();

    let begin = engine.execute_sql("BEGIN").unwrap();
    assert!(matches!(begin, QueryResult::Message(_)));
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (1, 'tx-rollback')")
        .unwrap();
    let rollback = engine.execute_sql("ROLLBACK").unwrap();
    assert!(matches!(rollback, QueryResult::Message(_)));

    let after_rollback = engine.execute_sql("SELECT * FROM t").unwrap();
    assert_eq!(
        after_rollback,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
        }
    );

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (1, 'tx-commit')")
        .unwrap();
    let commit = engine.execute_sql("COMMIT").unwrap();
    assert!(matches!(commit, QueryResult::Message(_)));

    let after_commit = engine.execute_sql("SELECT id, name FROM t").unwrap();
    assert_eq!(
        after_commit,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("tx-commit".to_string())
            ]],
        }
    );

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (2, 'crash-like')")
        .unwrap();
    drop(engine);

    let recovered_engine = Engine::new(&base);
    let recovered_rows = recovered_engine
        .execute_sql("SELECT id, name FROM t ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        recovered_rows,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("tx-commit".to_string())
            ]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_join_group_and_aggregate() {
    let dir = unique_test_dir("sql-engine-join-group");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT NOT NULL, amount BIGINT)",
        )
        .unwrap();

    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    engine
        .execute_sql(
            "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 10), (2, 1, 30), (3, 2, 5)",
        )
        .unwrap();

    let result = engine
        .execute_sql(
            "SELECT users.id, COUNT(*), SUM(orders.amount), MIN(orders.amount), MAX(orders.amount) \
             FROM users JOIN orders ON users.id = orders.user_id \
             GROUP BY users.id ORDER BY users.id ASC",
        )
        .unwrap();
    assert_eq!(
        result,
        QueryResult::Rows {
            columns: vec![
                "users.id".to_string(),
                "count(*)".to_string(),
                "sum(orders.amount)".to_string(),
                "min(orders.amount)".to_string(),
                "max(orders.amount)".to_string(),
            ],
            rows: vec![
                vec![
                    Value::BigInt(1),
                    Value::BigInt(2),
                    Value::Double(40.0),
                    Value::BigInt(10),
                    Value::BigInt(30),
                ],
                vec![
                    Value::BigInt(2),
                    Value::BigInt(1),
                    Value::Double(5.0),
                    Value::BigInt(5),
                    Value::BigInt(5),
                ],
            ],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_multi_database_isolation_and_switching() {
    let dir = unique_test_dir("sql-engine-multidb");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine.execute_sql("CREATE DATABASE app1").unwrap();
    engine.execute_sql("CREATE DATABASE app2").unwrap();

    let shown = engine.execute_sql("SHOW DATABASES").unwrap();
    assert_eq!(
        shown,
        QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows: vec![
                vec![Value::Varchar("app1".to_string())],
                vec![Value::Varchar("app2".to_string())],
                vec![Value::Varchar("default".to_string())],
            ],
        }
    );
    let current_default = engine.execute_sql("SHOW CURRENT DATABASE").unwrap();
    assert_eq!(
        current_default,
        QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows: vec![vec![Value::Varchar("default".to_string())]],
        }
    );

    engine.execute_sql("USE app1").unwrap();
    let current_app1 = engine.execute_sql("SHOW CURRENT DATABASE").unwrap();
    assert_eq!(
        current_app1,
        QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows: vec![vec![Value::Varchar("app1".to_string())]],
        }
    );
    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (1, 'a1')")
        .unwrap();

    engine.execute_sql("USE app2").unwrap();
    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (1, 'a2')")
        .unwrap();

    let app2_rows = engine.execute_sql("SELECT id, name FROM t").unwrap();
    assert_eq!(
        app2_rows,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("a2".to_string())]],
        }
    );

    engine.execute_sql("USE app1").unwrap();
    let app1_rows = engine.execute_sql("SELECT id, name FROM t").unwrap();
    assert_eq!(
        app1_rows,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("a1".to_string())]],
        }
    );
    let app1_tables = engine.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(
        app1_tables,
        QueryResult::Rows {
            columns: vec!["table_id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("t".to_string())]],
        }
    );

    engine.execute_sql("USE app2").unwrap();
    let app2_tables = engine.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(
        app2_tables,
        QueryResult::Rows {
            columns: vec!["table_id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("t".to_string())]],
        }
    );
    engine.execute_sql("DROP TABLE t").unwrap();
    let app2_tables_after_drop = engine.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(
        app2_tables_after_drop,
        QueryResult::Rows {
            columns: vec!["table_id".to_string(), "name".to_string()],
            rows: vec![],
        }
    );
    let select_after_drop = engine.execute_sql("SELECT id, name FROM t").unwrap_err();
    assert!(
        select_after_drop
            .to_string()
            .contains("table 't' not found")
    );
    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id, name) VALUES (2, 'a2-recreated')")
        .unwrap();
    let app2_rows_after_recreate = engine.execute_sql("SELECT id, name FROM t").unwrap();
    assert_eq!(
        app2_rows_after_recreate,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![
                Value::BigInt(2),
                Value::Varchar("a2-recreated".to_string())
            ]],
        }
    );

    let missing = engine.execute_sql("USE missing").unwrap_err();
    assert!(missing.to_string().contains("database 'missing' not found"));

    engine.execute_sql("BEGIN").unwrap();
    let switch_in_tx = engine.execute_sql("USE app2").unwrap_err();
    assert!(
        switch_in_tx
            .to_string()
            .contains("cannot switch database while a transaction is active")
    );
    let drop_in_tx = engine.execute_sql("DROP DATABASE app1").unwrap_err();
    assert!(
        drop_in_tx
            .to_string()
            .contains("cannot drop database while a transaction is active")
    );
    engine.execute_sql("ROLLBACK").unwrap();

    engine.execute_sql("USE default").unwrap();
    let default_tables = engine.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(
        default_tables,
        QueryResult::Rows {
            columns: vec!["table_id".to_string(), "name".to_string()],
            rows: vec![],
        }
    );

    engine.execute_sql("DROP DATABASE app2").unwrap();
    let shown_after_drop = engine.execute_sql("SHOW DATABASES").unwrap();
    assert_eq!(
        shown_after_drop,
        QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows: vec![
                vec![Value::Varchar("app1".to_string())],
                vec![Value::Varchar("default".to_string())],
            ],
        }
    );
    let use_dropped = engine.execute_sql("USE app2").unwrap_err();
    assert!(
        use_dropped
            .to_string()
            .contains("database 'app2' not found")
    );

    let drop_default = engine.execute_sql("DROP DATABASE default").unwrap_err();
    assert!(
        drop_default
            .to_string()
            .contains("cannot drop default database")
    );

    engine.execute_sql("USE app1").unwrap();
    let drop_current = engine.execute_sql("DROP DATABASE app1").unwrap_err();
    assert!(
        drop_current
            .to_string()
            .contains("cannot drop database currently in use")
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_primary_key_and_unique_constraints() {
    let dir = unique_test_dir("sql-engine-constraints");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR)",
        )
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'alice')")
        .unwrap();

    let duplicate_pk = engine
        .execute_sql("INSERT INTO users (id, email, name) VALUES (1, 'b@example.com', 'bob')")
        .unwrap_err();
    assert!(
        duplicate_pk
            .to_string()
            .contains("duplicate key violates PRIMARY KEY constraint")
    );

    let duplicate_unique = engine
        .execute_sql("INSERT INTO users (id, email, name) VALUES (2, 'a@example.com', 'alex')")
        .unwrap_err();
    assert!(
        duplicate_unique
            .to_string()
            .contains("duplicate key violates UNIQUE constraint")
    );

    let null_pk = engine
        .execute_sql("INSERT INTO users (id, email, name) VALUES (NULL, 'c@example.com', 'carol')")
        .unwrap_err();
    assert!(null_pk.to_string().contains("does not allow NULL"));

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_unique_constraint_update_conflict_and_multi_column() {
    let dir = unique_test_dir("sql-engine-constraints-update");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql(
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, email VARCHAR, CONSTRAINT uq_accounts_email UNIQUE (email))",
        )
        .unwrap();
    engine
        .execute_sql(
            "INSERT INTO accounts (id, email) VALUES (1, 'a@example.com'), (2, 'b@example.com')",
        )
        .unwrap();

    let update_conflict = engine
        .execute_sql("UPDATE accounts SET email = 'a@example.com' WHERE id = 2")
        .unwrap_err();
    assert!(
        update_conflict
            .to_string()
            .contains("duplicate key violates UNIQUE constraint")
    );

    engine
        .execute_sql(
            "CREATE TABLE pairs (a BIGINT, b BIGINT, CONSTRAINT uq_pairs_ab UNIQUE (a, b))",
        )
        .unwrap();
    engine
        .execute_sql("INSERT INTO pairs (a, b) VALUES (1, 2), (1, NULL), (1, NULL)")
        .unwrap();
    let duplicate_pair = engine
        .execute_sql("INSERT INTO pairs (a, b) VALUES (1, 2)")
        .unwrap_err();
    assert!(
        duplicate_pair
            .to_string()
            .contains("duplicate key violates UNIQUE constraint")
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_foreign_key_restrict_behaviors() {
    let dir = unique_test_dir("sql-engine-fk");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();

    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (10, 1), (11, NULL)")
        .unwrap();

    let missing_parent = engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (12, 999)")
        .unwrap_err();
    assert!(
        missing_parent
            .to_string()
            .contains("foreign key constraint 'fk_orders_user' fails")
    );

    let update_missing_parent = engine
        .execute_sql("UPDATE orders SET user_id = 999 WHERE id = 10")
        .unwrap_err();
    assert!(
        update_missing_parent
            .to_string()
            .contains("foreign key constraint 'fk_orders_user' fails")
    );

    let delete_parent = engine
        .execute_sql("DELETE FROM users WHERE id = 1")
        .unwrap_err();
    assert!(
        delete_parent
            .to_string()
            .contains("cannot delete from 'users'")
    );

    let update_parent = engine
        .execute_sql("UPDATE users SET id = 2 WHERE id = 1")
        .unwrap_err();
    assert!(update_parent.to_string().contains("cannot update 'users'"));

    let drop_parent = engine.execute_sql("DROP TABLE users").unwrap_err();
    assert!(
        drop_parent
            .to_string()
            .contains("cannot drop table 'users'")
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_foreign_key_transaction_commit_and_rollback() {
    let dir = unique_test_dir("sql-engine-fk-tx");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (100, 1)")
        .unwrap();
    engine.execute_sql("COMMIT").unwrap();

    let committed_users = engine
        .execute_sql("SELECT id, name FROM users ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        committed_users,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("alice".to_string())]],
        }
    );
    let committed_orders = engine
        .execute_sql("SELECT id, user_id FROM orders ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        committed_orders,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "user_id".to_string()],
            rows: vec![vec![Value::BigInt(100), Value::BigInt(1)]],
        }
    );

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (2, 'bob')")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (101, 2)")
        .unwrap();
    engine.execute_sql("ROLLBACK").unwrap();

    let rolled_back_users = engine
        .execute_sql("SELECT id FROM users ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        rolled_back_users,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)]],
        }
    );
    let rolled_back_orders = engine
        .execute_sql("SELECT id, user_id FROM orders ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        rolled_back_orders,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "user_id".to_string()],
            rows: vec![vec![Value::BigInt(100), Value::BigInt(1)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_foreign_key_restrict_consistent_within_transaction() {
    let dir = unique_test_dir("sql-engine-fk-tx-restrict");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (100, 1)")
        .unwrap();

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("DELETE FROM orders WHERE id = 100")
        .unwrap();
    let delete_parent_after_child = engine
        .execute_sql("DELETE FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(delete_parent_after_child, QueryResult::AffectedRows(1));
    engine.execute_sql("COMMIT").unwrap();

    let users_after = engine.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(
        users_after,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
        }
    );
    let orders_after = engine.execute_sql("SELECT * FROM orders").unwrap();
    assert_eq!(
        orders_after,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "user_id".to_string()],
            rows: vec![],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_alter_add_drop_and_rename_table_column() {
    let dir = unique_test_dir("sql-engine-alter-rename");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();

    engine
        .execute_sql("ALTER TABLE users ADD COLUMN email VARCHAR")
        .unwrap();
    let after_add = engine
        .execute_sql("SELECT * FROM users ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        after_add,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("alice".to_string()),
                Value::Null,
            ]],
        }
    );

    engine
        .execute_sql("UPDATE users SET email = 'alice@example.com' WHERE id = 1")
        .unwrap();
    engine
        .execute_sql("RENAME COLUMN users.email TO contact")
        .unwrap();
    let renamed_col = engine
        .execute_sql("SELECT id, contact FROM users ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        renamed_col,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "contact".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("alice@example.com".to_string()),
            ]],
        }
    );

    engine
        .execute_sql("ALTER TABLE users DROP COLUMN contact")
        .unwrap();
    let after_drop = engine.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(
        after_drop,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("alice".to_string())]],
        }
    );

    engine
        .execute_sql("RENAME TABLE users TO app_users")
        .unwrap();
    let old_table = engine.execute_sql("SELECT * FROM users").unwrap_err();
    assert!(old_table.to_string().contains("table 'users' not found"));
    let renamed_table = engine.execute_sql("SELECT * FROM app_users").unwrap();
    assert_eq!(
        renamed_table,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::Varchar("alice".to_string())]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_rename_updates_constraints_and_drop_column_restrictions() {
    let dir = unique_test_dir("sql-engine-rename-metadata");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql(
            "CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR, CONSTRAINT uq_users_email UNIQUE (email))",
        )
        .unwrap();
    engine
        .execute_sql("CREATE INDEX idx_users_email ON users (email)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, email) VALUES (1, 'a@example.com')")
        .unwrap();

    engine
        .execute_sql("RENAME COLUMN users.email TO login")
        .unwrap();
    let duplicate_login = engine
        .execute_sql("INSERT INTO users (id, login) VALUES (2, 'a@example.com')")
        .unwrap_err();
    assert!(
        duplicate_login
            .to_string()
            .contains("duplicate key violates UNIQUE constraint")
    );

    engine
        .execute_sql("RENAME TABLE users TO accounts")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (100, 1)")
        .unwrap();
    let delete_parent = engine
        .execute_sql("DELETE FROM accounts WHERE id = 1")
        .unwrap_err();
    assert!(
        delete_parent
            .to_string()
            .contains("cannot delete from 'accounts'")
    );

    let drop_indexed = engine
        .execute_sql("ALTER TABLE accounts DROP COLUMN login")
        .unwrap_err();
    assert!(drop_indexed.to_string().contains("cannot drop column"));

    let drop_fk_parent = engine
        .execute_sql("ALTER TABLE accounts DROP COLUMN id")
        .unwrap_err();
    assert!(drop_fk_parent.to_string().contains("cannot drop column"));

    let add_not_null = engine
        .execute_sql("ALTER TABLE accounts ADD COLUMN created_at BIGINT NOT NULL")
        .unwrap_err();
    assert!(
        add_not_null
            .to_string()
            .contains("cannot add NOT NULL column")
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_ddl_works_with_transaction_rollback() {
    let dir = unique_test_dir("sql-engine-ddl-tx");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id) VALUES (1), (2)")
        .unwrap();

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("ALTER TABLE t ADD COLUMN age BIGINT")
        .unwrap();
    engine.execute_sql("ROLLBACK").unwrap();

    let after_rollback_add = engine
        .execute_sql("SELECT * FROM t ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        after_rollback_add,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)], vec![Value::BigInt(2)]],
        }
    );

    engine.execute_sql("BEGIN").unwrap();
    engine.execute_sql("RENAME TABLE t TO t_new").unwrap();
    engine.execute_sql("ROLLBACK").unwrap();
    let old_name_ok = engine
        .execute_sql("SELECT id FROM t ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        old_name_ok,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)], vec![Value::BigInt(2)]],
        }
    );
    let renamed_missing = engine.execute_sql("SELECT id FROM t_new").unwrap_err();
    assert!(
        renamed_missing
            .to_string()
            .contains("table 't_new' not found")
    );

    engine.execute_sql("BEGIN").unwrap();
    engine.execute_sql("RENAME COLUMN t.id TO user_id").unwrap();
    engine.execute_sql("ROLLBACK").unwrap();
    let still_old_column = engine
        .execute_sql("SELECT id FROM t ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        still_old_column,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)], vec![Value::BigInt(2)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_alter_column_type_and_nullability() {
    let dir = unique_test_dir("sql-engine-alter-column");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE metrics (id BIGINT NOT NULL, score BIGINT)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO metrics (id, score) VALUES (1, 10), (2, NULL)")
        .unwrap();

    engine
        .execute_sql("ALTER TABLE metrics ALTER COLUMN score TYPE DOUBLE")
        .unwrap();
    let after_type = engine
        .execute_sql("SELECT id, score FROM metrics ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        after_type,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "score".to_string()],
            rows: vec![
                vec![Value::BigInt(1), Value::Double(10.0)],
                vec![Value::BigInt(2), Value::Null],
            ],
        }
    );

    let set_not_null_err = engine
        .execute_sql("ALTER TABLE metrics ALTER COLUMN score SET NOT NULL")
        .unwrap_err();
    assert!(set_not_null_err.to_string().contains("contain NULL values"));

    engine
        .execute_sql("UPDATE metrics SET score = 20 WHERE id = 2")
        .unwrap();
    engine
        .execute_sql("ALTER TABLE metrics ALTER COLUMN score SET NOT NULL")
        .unwrap();
    let insert_null_err = engine
        .execute_sql("INSERT INTO metrics (id, score) VALUES (3, NULL)")
        .unwrap_err();
    assert!(insert_null_err.to_string().contains("does not allow NULL"));

    engine
        .execute_sql("ALTER TABLE metrics ALTER COLUMN score DROP NOT NULL")
        .unwrap();
    engine
        .execute_sql("INSERT INTO metrics (id, score) VALUES (3, NULL)")
        .unwrap();
    let after_drop_not_null = engine
        .execute_sql("SELECT id, score FROM metrics ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        after_drop_not_null,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "score".to_string()],
            rows: vec![
                vec![Value::BigInt(1), Value::Double(10.0)],
                vec![Value::BigInt(2), Value::Double(20.0)],
                vec![Value::BigInt(3), Value::Null],
            ],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_alter_column_restrictions_and_upgrade_compatibility() {
    let dir = unique_test_dir("sql-engine-alter-column-restrict");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE RESTRICT)",
        )
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();

    let alter_pk_type_err = engine
        .execute_sql("ALTER TABLE users ALTER COLUMN id TYPE INT")
        .unwrap_err();
    assert!(alter_pk_type_err.to_string().contains("used by constraint"));

    let drop_pk_not_null_err = engine
        .execute_sql("ALTER TABLE users ALTER COLUMN id DROP NOT NULL")
        .unwrap_err();
    assert!(
        drop_pk_not_null_err
            .to_string()
            .contains("part of PRIMARY KEY")
    );

    drop(engine);

    // Compatibility check: existing old-style directory can be reopened and migrated with B features.
    let reopened = Engine::new(&base);
    reopened
        .execute_sql("ALTER TABLE users ADD COLUMN email VARCHAR")
        .unwrap();
    reopened
        .execute_sql("RENAME TABLE users TO accounts")
        .unwrap();
    let rows = reopened
        .execute_sql("SELECT id, name, email FROM accounts")
        .unwrap();
    assert_eq!(
        rows,
        QueryResult::Rows {
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string()],
            rows: vec![vec![
                Value::BigInt(1),
                Value::Varchar("alice".to_string()),
                Value::Null,
            ]],
        }
    );

    cleanup_dir(&dir);
}
