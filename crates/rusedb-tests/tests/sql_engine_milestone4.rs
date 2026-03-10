use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;
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

fn wal_file_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".wal");
    PathBuf::from(os)
}

fn slow_log_file_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".slowlog");
    PathBuf::from(os)
}

fn active_begin_tx_id(base: &Path) -> u64 {
    let content = fs::read_to_string(wal_file_path(base)).unwrap();
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(tx_id_raw) = trimmed.strip_prefix("BEGIN ") {
            return tx_id_raw.parse::<u64>().unwrap();
        }
    }
    panic!("no BEGIN record found in WAL");
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
fn sql_engine_read_committed_for_non_owner_reader() {
    let dir = unique_test_dir("sql-engine-read-committed");
    let base = dir.join("rusedb");
    let engine = Arc::new(Engine::new(&base));

    engine
        .execute_sql("CREATE TABLE accounts (id BIGINT NOT NULL, balance BIGINT)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO accounts (id, balance) VALUES (1, 10)")
        .unwrap();

    let (updated_tx, updated_rx) = mpsc::channel();
    let (commit_tx, commit_rx) = mpsc::channel();
    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        writer_engine.execute_sql("BEGIN").unwrap();
        writer_engine
            .execute_sql("UPDATE accounts SET balance = 20 WHERE id = 1")
            .unwrap();
        updated_tx.send(()).unwrap();
        commit_rx.recv().unwrap();
        writer_engine.execute_sql("COMMIT").unwrap();
    });

    updated_rx.recv().unwrap();
    let before_commit = engine
        .execute_sql("SELECT balance FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(
        before_commit,
        QueryResult::Rows {
            columns: vec!["balance".to_string()],
            rows: vec![vec![Value::BigInt(10)]],
        }
    );

    commit_tx.send(()).unwrap();
    writer.join().unwrap();

    let after_commit = engine
        .execute_sql("SELECT balance FROM accounts WHERE id = 1")
        .unwrap();
    assert_eq!(
        after_commit,
        QueryResult::Rows {
            columns: vec!["balance".to_string()],
            rows: vec![vec![Value::BigInt(20)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_lock_wait_timeout_for_concurrent_writer() {
    let dir = unique_test_dir("sql-engine-lock-timeout");
    let base = dir.join("rusedb");
    let engine = Arc::new(Engine::new_with_lock_wait_timeout(
        &base,
        Duration::from_millis(150),
    ));

    engine
        .execute_sql("CREATE TABLE items (id BIGINT NOT NULL)")
        .unwrap();

    let (begun_tx, begun_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder_engine = Arc::clone(&engine);
    let holder = thread::spawn(move || {
        holder_engine.execute_sql("BEGIN").unwrap();
        begun_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        holder_engine.execute_sql("ROLLBACK").unwrap();
    });

    begun_rx.recv().unwrap();
    let err = engine
        .execute_sql("INSERT INTO items (id) VALUES (1)")
        .unwrap_err();
    assert!(err.to_string().contains("lock wait timeout"));

    release_tx.send(()).unwrap();
    holder.join().unwrap();

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_wal_commit_start_recovery_applies_tx_files() {
    let dir = unique_test_dir("sql-engine-wal-commit-start");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, value BIGINT)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id, value) VALUES (1, 10)")
        .unwrap();
    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("UPDATE t SET value = 20 WHERE id = 1")
        .unwrap();

    let tx_id = active_begin_tx_id(&base);
    let mut wal_file = fs::OpenOptions::new()
        .append(true)
        .open(wal_file_path(&base))
        .unwrap();
    writeln!(wal_file, "COMMIT_START {tx_id}").unwrap();
    wal_file.flush().unwrap();
    drop(engine);

    let recovered = Engine::new(&base);
    let result = recovered
        .execute_sql("SELECT value FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(
        result,
        QueryResult::Rows {
            columns: vec!["value".to_string()],
            rows: vec![vec![Value::BigInt(20)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_wal_checkpoint_and_legacy_format_are_supported() {
    let dir = unique_test_dir("sql-engine-wal-checkpoint-legacy");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, value BIGINT)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO t (id, value) VALUES (1, 10)")
        .unwrap();

    let wal_after_commit = fs::read_to_string(wal_file_path(&base)).unwrap();
    assert!(wal_after_commit.contains("WAL 1"));
    assert!(wal_after_commit.contains("CHECKPOINT"));
    assert!(!wal_after_commit.contains("COMMIT_START"));

    engine.execute_sql("BEGIN").unwrap();
    engine
        .execute_sql("UPDATE t SET value = 30 WHERE id = 1")
        .unwrap();
    let tx_id = active_begin_tx_id(&base);
    fs::write(
        wal_file_path(&base),
        format!("BEGIN {tx_id}\nCOMMIT_START {tx_id}\n"),
    )
    .unwrap();
    drop(engine);

    let recovered = Engine::new(&base);
    let result = recovered
        .execute_sql("SELECT value FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(
        result,
        QueryResult::Rows {
            columns: vec!["value".to_string()],
            rows: vec![vec![Value::BigInt(30)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_explain_analyze_and_stats_work() {
    let dir = unique_test_dir("sql-engine-explain-stats");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, score BIGINT)")
        .unwrap();
    engine
        .execute_sql("CREATE INDEX idx_users_id ON users (id)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, score) VALUES (1, 10), (2, NULL), (3, 10)")
        .unwrap();

    let analyzed = engine.execute_sql("ANALYZE TABLE users").unwrap();
    assert!(matches!(analyzed, QueryResult::Message(_)));

    let explain = engine
        .execute_sql("EXPLAIN SELECT id FROM users WHERE id = 2")
        .unwrap();
    let QueryResult::Rows { rows, .. } = explain else {
        panic!("EXPLAIN should return rows");
    };
    let mut map = std::collections::HashMap::<String, String>::new();
    for row in rows {
        let key = match &row[0] {
            Value::Varchar(v) => v.clone(),
            other => format!("{other:?}"),
        };
        let value = match &row[1] {
            Value::Varchar(v) => v.clone(),
            other => format!("{other:?}"),
        };
        map.insert(key, value);
    }
    assert!(map.get("access_path").unwrap().contains("INDEX"));
    assert!(map.contains_key("table_rows"));
    assert!(map.contains_key("estimated_rows"));
    assert!(map.contains_key("predicate_column_stats"));

    let explain_analyze = engine
        .execute_sql("EXPLAIN ANALYZE SELECT id FROM users WHERE id = 2")
        .unwrap();
    let QueryResult::Rows {
        rows: analyze_rows, ..
    } = explain_analyze
    else {
        panic!("EXPLAIN ANALYZE should return rows");
    };
    let analyze_items = analyze_rows
        .into_iter()
        .filter_map(|row| match &row[0] {
            Value::Varchar(key) => Some(key.clone()),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
    assert!(analyze_items.contains("analyze_elapsed_ms"));
    assert!(analyze_items.contains("analyze_scanned_rows"));
    assert!(analyze_items.contains("analyze_output_rows"));

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_slow_query_log_records_metrics() {
    let dir = unique_test_dir("sql-engine-slow-log");
    let base = dir.join("rusedb");
    let engine = Engine::new_with_timeouts(&base, Duration::from_secs(3), Duration::from_millis(0));

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .unwrap();
    let _ = engine
        .execute_sql("SELECT id, name FROM users WHERE id = 1")
        .unwrap();

    let log = fs::read_to_string(slow_log_file_path(&base)).unwrap();
    assert!(log.contains("SELECT id, name FROM users WHERE id = 1"));
    assert!(log.contains("rows=1"));

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
fn sql_engine_predicate_extensions_work() {
    let dir = unique_test_dir("sql-engine-predicate-extensions");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql(
            "CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR, score BIGINT, deleted_at BIGINT)",
        )
        .unwrap();
    engine
        .execute_sql(
            "INSERT INTO users (id, name, score, deleted_at) VALUES \
             (1, 'alice', 15, NULL), \
             (2, 'alex', 9, NULL), \
             (3, 'bob', 20, 1), \
             (4, 'anna', 18, NULL)",
        )
        .unwrap();

    let filtered = engine
        .execute_sql(
            "SELECT id FROM users \
             WHERE name LIKE 'a%' \
               AND id IN (1, 2, 4) \
               AND score BETWEEN 10 AND 20 \
               AND deleted_at IS NULL \
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        filtered,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)], vec![Value::BigInt(4)]],
        }
    );

    let not_null = engine
        .execute_sql("SELECT id FROM users WHERE deleted_at IS NOT NULL ORDER BY id ASC")
        .unwrap();
    assert_eq!(
        not_null,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(3)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_left_join_and_having_work() {
    let dir = unique_test_dir("sql-engine-left-join-having");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT)")
        .unwrap();
    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    engine
        .execute_sql("INSERT INTO orders (id, user_id) VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();

    let joined = engine
        .execute_sql(
            "SELECT users.id, COUNT(orders.id) \
             FROM users LEFT JOIN orders ON users.id = orders.user_id \
             GROUP BY users.id \
             ORDER BY users.id ASC",
        )
        .unwrap();
    assert_eq!(
        joined,
        QueryResult::Rows {
            columns: vec!["users.id".to_string(), "count(orders.id)".to_string()],
            rows: vec![
                vec![Value::BigInt(1), Value::BigInt(2)],
                vec![Value::BigInt(2), Value::BigInt(1)],
                vec![Value::BigInt(3), Value::BigInt(0)],
            ],
        }
    );

    let having = engine
        .execute_sql(
            "SELECT users.id, COUNT(orders.id) \
             FROM users LEFT JOIN orders ON users.id = orders.user_id \
             GROUP BY users.id \
             HAVING COUNT(orders.id) > 1 \
             ORDER BY users.id ASC",
        )
        .unwrap();
    assert_eq!(
        having,
        QueryResult::Rows {
            columns: vec!["users.id".to_string(), "count(orders.id)".to_string()],
            rows: vec![vec![Value::BigInt(1), Value::BigInt(2)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_in_subquery_first_stage_works() {
    let dir = unique_test_dir("sql-engine-in-subquery");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT, amount BIGINT)")
        .unwrap();

    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    engine
        .execute_sql(
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100), (11, 2, 5), (12, 1, 20)",
        )
        .unwrap();

    let result = engine
        .execute_sql(
            "SELECT id FROM users \
             WHERE id IN (SELECT user_id FROM orders WHERE amount >= 20) \
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        result,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)]],
        }
    );

    let not_in_result = engine
        .execute_sql(
            "SELECT id FROM users \
             WHERE id NOT IN (SELECT user_id FROM orders WHERE amount >= 20) \
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        not_in_result,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(2)], vec![Value::BigInt(3)]],
        }
    );

    cleanup_dir(&dir);
}

#[test]
fn sql_engine_scalar_subquery_second_stage_works() {
    let dir = unique_test_dir("sql-engine-scalar-subquery");
    let base = dir.join("rusedb");
    let engine = Engine::new(&base);

    engine
        .execute_sql("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR)")
        .unwrap();
    engine
        .execute_sql("CREATE TABLE orders (id BIGINT NOT NULL, user_id BIGINT, amount BIGINT)")
        .unwrap();

    engine
        .execute_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .unwrap();
    engine
        .execute_sql(
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100), (11, 2, 5), (12, 1, 20)",
        )
        .unwrap();

    let single_row = engine
        .execute_sql(
            "SELECT id FROM users \
             WHERE id = (SELECT user_id FROM orders WHERE id = 10) \
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        single_row,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![vec![Value::BigInt(1)]],
        }
    );

    let null_subquery = engine
        .execute_sql(
            "SELECT id FROM users \
             WHERE (SELECT user_id FROM orders WHERE id = 999) IS NULL \
             ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        null_subquery,
        QueryResult::Rows {
            columns: vec!["id".to_string()],
            rows: vec![
                vec![Value::BigInt(1)],
                vec![Value::BigInt(2)],
                vec![Value::BigInt(3)],
            ],
        }
    );

    let multi_row_err = engine
        .execute_sql(
            "SELECT id FROM users WHERE id = (SELECT user_id FROM orders WHERE amount >= 20)",
        )
        .unwrap_err();
    assert!(
        multi_row_err
            .to_string()
            .contains("scalar subquery must return at most one row")
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
