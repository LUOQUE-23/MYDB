use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusedb_core::{
    Column, DataType, IndexInfo, Result, Row, RuseDbError, Schema, TableInfo, Value,
};
use rusedb_sql::{
    AggregateFunction, AlterColumnAction, BinaryOp, ColumnDef, Expr, JoinClause, Literal,
    OrderByItem, SelectItem, Statement, TableConstraint, TableConstraintKind, UnaryOp, parse_sql,
};
use rusedb_storage::{
    Catalog, ConstraintDef, ConstraintInfo, ConstraintKind, HeapFile, IndexKeyKind, OrderedIndex,
    Rid,
};

#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    AffectedRows(usize),
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Message(String),
}

pub trait Executor {
    fn execute_sql(&self, sql: &str) -> Result<QueryResult>;
}

const DEFAULT_DATABASE_NAME: &str = "default";
const DEFAULT_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_SLOW_QUERY_THRESHOLD: Duration = Duration::from_millis(200);
const BACKUP_LOCK_WAIT_POLL: Duration = Duration::from_millis(25);
const ISOLATION_LEVEL: &str = "READ COMMITTED";
const WAL_MAGIC: &str = "WAL";
const WAL_VERSION: u32 = 1;
const STATS_MAGIC: &str = "RUSEDB_STATS";
const STATS_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct Engine {
    catalog_base: PathBuf,
    state: Arc<Mutex<EngineState>>,
    tx_condvar: Arc<Condvar>,
    lock_wait_timeout: Duration,
    slow_query_threshold: Duration,
}

#[derive(Debug)]
struct EngineState {
    tx: Option<TransactionContext>,
    tx_starting: bool,
    wal_recovered: bool,
    next_tx_id: u64,
    current_db: String,
}

#[derive(Debug, Clone)]
struct TransactionContext {
    tx_id: u64,
    live_base: PathBuf,
    working_base: PathBuf,
    owner_thread: ThreadId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalEventKind {
    Begin,
    CommitStart,
    CommitDone,
    Checkpoint,
}

#[derive(Debug)]
struct TableIndex {
    info: IndexInfo,
    column_index: usize,
    index: OrderedIndex,
}

#[derive(Debug)]
struct IndexPredicate {
    column: String,
    op: BinaryOp,
    value: Value,
}

#[derive(Debug, Clone)]
struct ConstraintCheck {
    info: ConstraintInfo,
    column_indexes: Vec<usize>,
    referenced_column_indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
struct SelectPlan {
    table: String,
    joins: Vec<JoinClause>,
    projection: Vec<SelectItem>,
    selection: Option<Expr>,
    group_by: Vec<String>,
    having: Option<Expr>,
    order_by: Vec<OrderByItem>,
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct RowContext {
    values: HashMap<String, Value>,
    ambiguous: HashSet<String>,
    wildcard_columns: Vec<String>,
}

#[derive(Debug, Clone)]
struct ColumnStatistics {
    name: String,
    distinct_count: usize,
    null_count: usize,
}

#[derive(Debug, Clone)]
struct TableStatistics {
    table: String,
    row_count: usize,
    updated_unix_ms: u64,
    columns: Vec<ColumnStatistics>,
}

impl Engine {
    pub fn new(catalog_base: impl AsRef<Path>) -> Self {
        Self::new_with_timeouts(
            catalog_base,
            DEFAULT_LOCK_WAIT_TIMEOUT,
            DEFAULT_SLOW_QUERY_THRESHOLD,
        )
    }

    pub fn new_with_lock_wait_timeout(
        catalog_base: impl AsRef<Path>,
        lock_wait_timeout: Duration,
    ) -> Self {
        Self::new_with_timeouts(
            catalog_base,
            lock_wait_timeout,
            DEFAULT_SLOW_QUERY_THRESHOLD,
        )
    }

    pub fn new_with_timeouts(
        catalog_base: impl AsRef<Path>,
        lock_wait_timeout: Duration,
        slow_query_threshold: Duration,
    ) -> Self {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        Self {
            catalog_base: catalog_base.as_ref().to_path_buf(),
            state: Arc::new(Mutex::new(EngineState {
                tx: None,
                tx_starting: false,
                wal_recovered: false,
                next_tx_id: now_nanos.max(1),
                current_db: DEFAULT_DATABASE_NAME.to_string(),
            })),
            tx_condvar: Arc::new(Condvar::new()),
            lock_wait_timeout,
            slow_query_threshold,
        }
    }

    pub fn execute_statement(&self, statement: Statement) -> Result<QueryResult> {
        self.ensure_recovered()?;
        if statement_requires_backup_lock_wait(&statement) {
            self.wait_for_backup_lock_release_for_statement(&statement)?;
        }
        match statement {
            Statement::Begin => self.begin_transaction(),
            Statement::Commit => self.commit_transaction(),
            Statement::Rollback => self.rollback_transaction(),
            Statement::CreateDatabase { name } => self.create_database(&name),
            Statement::DropDatabase { name } => self.drop_database(&name),
            Statement::UseDatabase { name } => self.use_database(&name),
            Statement::ShowDatabases => self.show_databases(),
            Statement::ShowCurrentDatabase => self.show_current_database(),
            Statement::ShowTables => self.show_tables(),
            other => self.execute_regular_statement(other),
        }
    }

    fn execute_regular_statement(&self, statement: Statement) -> Result<QueryResult> {
        let current_thread = std::thread::current().id();
        let tx_ctx = {
            let state = self.lock_state()?;
            state.tx.clone()
        };
        if let Some(tx) = tx_ctx {
            if tx.owner_thread == current_thread {
                return self.execute_statement_on_base(&tx.working_base, statement);
            }
            if !is_mutating_statement(&statement) {
                return self.execute_statement_on_base(&tx.live_base, statement);
            }
        }

        let live_base = self.current_live_base()?;
        if is_mutating_statement(&statement) {
            self.begin_transaction_internal()?;
            let execution = {
                let tx_base = self.current_tx_base()?;
                self.execute_statement_on_base(&tx_base, statement)
            };
            match execution {
                Ok(result) => {
                    self.commit_transaction_internal()?;
                    Ok(result)
                }
                Err(err) => {
                    let _ = self.rollback_transaction_internal();
                    Err(err)
                }
            }
        } else {
            self.execute_statement_on_base(&live_base, statement)
        }
    }

    fn execute_statement_on_base(
        &self,
        catalog_base: &Path,
        statement: Statement,
    ) -> Result<QueryResult> {
        match statement {
            Statement::Explain { analyze, statement } => {
                self.exec_explain(catalog_base, analyze, *statement)
            }
            Statement::AnalyzeTable { table } => self.exec_analyze_table(catalog_base, &table),
            Statement::CreateTable {
                name,
                columns,
                constraints,
            } => self.exec_create_table(catalog_base, &name, columns, constraints),
            Statement::DropTable { name } => self.exec_drop_table(catalog_base, &name),
            Statement::AlterTableAddColumn { table, column } => {
                self.exec_alter_table_add_column(catalog_base, &table, column)
            }
            Statement::AlterTableDropColumn { table, column } => {
                self.exec_alter_table_drop_column(catalog_base, &table, &column)
            }
            Statement::AlterTableAlterColumn {
                table,
                column,
                action,
            } => self.exec_alter_table_alter_column(catalog_base, &table, &column, action),
            Statement::RenameTable { old_name, new_name } => {
                self.exec_rename_table(catalog_base, &old_name, &new_name)
            }
            Statement::RenameColumn {
                table,
                old_name,
                new_name,
            } => self.exec_rename_column(catalog_base, &table, &old_name, &new_name),
            Statement::CreateIndex {
                name,
                table,
                column,
            } => self.exec_create_index(catalog_base, &name, &table, &column),
            Statement::Insert {
                table,
                columns,
                rows,
            } => self.exec_insert(catalog_base, &table, columns, rows),
            Statement::Select {
                table,
                joins,
                projection,
                selection,
                group_by,
                having,
                order_by,
                limit,
            } => self.exec_select(
                catalog_base,
                SelectPlan {
                    joins,
                    table,
                    projection,
                    selection,
                    group_by,
                    having,
                    order_by,
                    limit,
                },
            ),
            Statement::Delete { table, selection } => {
                self.exec_delete(catalog_base, &table, selection)
            }
            Statement::Update {
                table,
                assignments,
                selection,
            } => self.exec_update(catalog_base, &table, assignments, selection),
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::UseDatabase { .. }
            | Statement::ShowDatabases
            | Statement::ShowCurrentDatabase
            | Statement::ShowTables => Err(RuseDbError::Parse(
                "statement not expected in this execution path".to_string(),
            )),
        }
    }

    fn exec_create_table(
        &self,
        catalog_base: &Path,
        name: &str,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    ) -> Result<QueryResult> {
        let (schema_columns, table_constraints) =
            build_table_definition(name, columns, constraints)?;
        let mut catalog = Catalog::open(catalog_base)?;
        validate_foreign_key_definitions(&catalog, &table_constraints)?;
        let table = catalog.create_table(name, schema_columns, table_constraints)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        heap.sync()?;
        Ok(QueryResult::Message(format!(
            "created table '{}' (id={})",
            table.name, table.table_id
        )))
    }

    fn exec_drop_table(&self, catalog_base: &Path, table_name: &str) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        ensure_drop_table_allowed_by_fk(catalog_base, &catalog, &table.name)?;
        let indexes = catalog.list_indexes(table_name)?;
        catalog.drop_table(table_name)?;

        let heap_path = table_heap_path(catalog_base, table.table_id);
        if heap_path.exists() {
            fs::remove_file(heap_path)?;
        }
        for index in indexes {
            let index_path = index_file_path(catalog_base, index.index_id);
            if index_path.exists() {
                fs::remove_file(index_path)?;
            }
        }
        Ok(QueryResult::Message(format!(
            "dropped table '{}'",
            table_name
        )))
    }

    fn exec_alter_table_add_column(
        &self,
        catalog_base: &Path,
        table_name: &str,
        column: ColumnDef,
    ) -> Result<QueryResult> {
        if column.primary_key || column.unique || column.references.is_some() {
            return Err(RuseDbError::Parse(
                "ALTER TABLE ADD COLUMN currently does not support PRIMARY KEY/UNIQUE/REFERENCES"
                    .to_string(),
            ));
        }

        let mut catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let old_schema = catalog.describe_table(&table.name)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        let has_rows = !heap.scan_records()?.is_empty();
        if has_rows && !column.nullable {
            return Err(RuseDbError::Parse(format!(
                "cannot add NOT NULL column '{}' to non-empty table '{}'",
                column.name, table_name
            )));
        }

        catalog.add_column(
            &table.name,
            Column::new(column.name, column.data_type, column.nullable),
        )?;
        let new_schema = catalog.describe_table(&table.name)?;

        rewrite_table_rows(
            catalog_base,
            table.table_id,
            &old_schema,
            &new_schema,
            |row| {
                let mut values = row.values.clone();
                values.push(Value::Null);
                Ok(values)
            },
        )?;
        rebuild_table_indexes(catalog_base, &catalog, &table.name, &new_schema)?;

        Ok(QueryResult::Message(format!(
            "altered table '{}': added column",
            table_name
        )))
    }

    fn exec_alter_table_drop_column(
        &self,
        catalog_base: &Path,
        table_name: &str,
        column_name: &str,
    ) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let old_schema = catalog.describe_table(&table.name)?;
        if old_schema.columns.len() <= 1 {
            return Err(RuseDbError::InvalidSchema(format!(
                "cannot drop last column from table '{}'",
                table_name
            )));
        }
        let (drop_index, _) = old_schema
            .find_column(column_name)
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: column_name.to_string(),
            })?;

        ensure_drop_column_allowed_by_metadata(&catalog, &table.name, column_name)?;
        catalog.drop_column(&table.name, column_name)?;
        let new_schema = catalog.describe_table(&table.name)?;

        rewrite_table_rows(
            catalog_base,
            table.table_id,
            &old_schema,
            &new_schema,
            |row| {
                let mut values = row.values.clone();
                values.remove(drop_index);
                Ok(values)
            },
        )?;
        rebuild_table_indexes(catalog_base, &catalog, &table.name, &new_schema)?;

        Ok(QueryResult::Message(format!(
            "altered table '{}': dropped column '{}'",
            table_name, column_name
        )))
    }

    fn exec_alter_table_alter_column(
        &self,
        catalog_base: &Path,
        table_name: &str,
        column_name: &str,
        action: AlterColumnAction,
    ) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let old_schema = catalog.describe_table(&table.name)?;
        let (column_index, column) =
            old_schema
                .find_column(column_name)
                .ok_or(RuseDbError::NotFound {
                    object: "column".to_string(),
                    name: column_name.to_string(),
                })?;
        let normalized_column = column.name.to_ascii_lowercase();

        match action {
            AlterColumnAction::SetDataType(new_type) => {
                if column.data_type == new_type {
                    return Ok(QueryResult::Message(format!(
                        "altered table '{}': column '{}' type unchanged",
                        table_name, column.name
                    )));
                }
                ensure_alter_column_type_allowed_by_metadata(
                    &catalog,
                    &table.name,
                    &normalized_column,
                    new_type,
                )?;

                let mut new_columns = old_schema.columns.clone();
                new_columns[column_index].data_type = new_type;
                let new_schema = Schema::new(new_columns)?;

                rewrite_table_rows(
                    catalog_base,
                    table.table_id,
                    &old_schema,
                    &new_schema,
                    |row| {
                        let mut values = row.values.clone();
                        values[column_index] = cast_value_to_type(
                            &values[column_index],
                            new_type,
                            &old_schema.columns[column_index].name,
                        )?;
                        Ok(values)
                    },
                )?;

                catalog.alter_column(&table.name, column_name, Some(new_type), None)?;
                rebuild_table_indexes(catalog_base, &catalog, &table.name, &new_schema)?;
                Ok(QueryResult::Message(format!(
                    "altered table '{}': column '{}' type changed to {}",
                    table_name,
                    column.name,
                    new_type.as_str()
                )))
            }
            AlterColumnAction::SetNotNull => {
                if !column.nullable {
                    return Ok(QueryResult::Message(format!(
                        "altered table '{}': column '{}' already NOT NULL",
                        table_name, column.name
                    )));
                }
                let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
                for (_rid, raw) in heap.scan_records()? {
                    let row = Row::decode(&old_schema, &raw)?;
                    if matches!(row.values[column_index], Value::Null) {
                        return Err(RuseDbError::Parse(format!(
                            "cannot set NOT NULL on '{}.{}': existing rows contain NULL values",
                            table_name, column.name
                        )));
                    }
                }
                catalog.alter_column(&table.name, column_name, None, Some(false))?;
                Ok(QueryResult::Message(format!(
                    "altered table '{}': column '{}' set NOT NULL",
                    table_name, column.name
                )))
            }
            AlterColumnAction::DropNotNull => {
                ensure_drop_not_null_allowed_by_metadata(
                    &catalog,
                    &table.name,
                    &normalized_column,
                )?;
                catalog.alter_column(&table.name, column_name, None, Some(true))?;
                Ok(QueryResult::Message(format!(
                    "altered table '{}': column '{}' dropped NOT NULL",
                    table_name, column.name
                )))
            }
        }
    }

    fn exec_rename_table(
        &self,
        catalog_base: &Path,
        old_name: &str,
        new_name: &str,
    ) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let renamed = catalog.rename_table(old_name, new_name)?;
        Ok(QueryResult::Message(format!(
            "renamed table '{}' to '{}'",
            old_name, renamed.name
        )))
    }

    fn exec_rename_column(
        &self,
        catalog_base: &Path,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let _ = find_table(&catalog, table_name)?;
        let renamed = catalog.rename_column(table_name, old_name, new_name)?;
        Ok(QueryResult::Message(format!(
            "renamed column '{}.{}' to '{}'",
            table_name, old_name, renamed.name
        )))
    }

    fn exec_create_index(
        &self,
        catalog_base: &Path,
        index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<QueryResult> {
        let mut catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let schema = catalog.describe_table(table_name)?;
        let (column_index, column) =
            schema
                .find_column(column_name)
                .ok_or(RuseDbError::NotFound {
                    object: "column".to_string(),
                    name: column_name.to_string(),
                })?;
        let key_kind = IndexKeyKind::from_data_type(column.data_type)?;

        let info = catalog.create_index(table_name, index_name, column_name)?;
        let mut index = OrderedIndex::open(index_file_path(catalog_base, info.index_id), key_kind)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        for (rid, raw) in heap.scan_records()? {
            let row = Row::decode(&schema, &raw)?;
            if let Some(key) = index.key_from_value(&row.values[column_index])? {
                index.insert(key, rid)?;
            }
        }
        Ok(QueryResult::Message(format!(
            "created index '{}' on {}({})",
            info.name, table_name, column_name
        )))
    }

    fn exec_insert(
        &self,
        catalog_base: &Path,
        table_name: &str,
        column_names: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    ) -> Result<QueryResult> {
        let catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let schema = catalog.describe_table(table_name)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        let mut indexes = load_table_indexes(&catalog, table_name, &schema, catalog_base)?;
        let constraints = load_constraint_checks(&catalog, table_name, &schema)?;

        let mut prepared_rows = Vec::with_capacity(rows.len());
        for exprs in rows {
            let mut values = build_insert_values(&schema, column_names.as_deref(), &exprs)?;
            normalize_values_for_schema(&schema, &mut values)?;
            prepared_rows.push(values);
        }
        validate_constraints_on_insert(
            catalog_base,
            &catalog,
            &mut heap,
            &schema,
            &constraints,
            &prepared_rows,
        )?;

        let mut inserted = 0usize;
        for values in prepared_rows {
            let row = Row::new(values.clone());
            let bytes = row.encode(&schema)?;
            let rid = heap.insert_record(&bytes)?;
            for table_index in &mut indexes {
                if let Some(key) = table_index
                    .index
                    .key_from_value(&values[table_index.column_index])?
                {
                    table_index.index.insert(key, rid)?;
                }
            }
            inserted += 1;
        }
        heap.sync()?;
        Ok(QueryResult::AffectedRows(inserted))
    }

    fn exec_select(&self, catalog_base: &Path, plan: SelectPlan) -> Result<QueryResult> {
        let has_aggregate = plan
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Aggregate { .. }));
        let having_has_aggregate = plan
            .having
            .as_ref()
            .map(expr_has_aggregate)
            .unwrap_or(false);
        if !plan.joins.is_empty()
            || has_aggregate
            || !plan.group_by.is_empty()
            || plan.having.is_some()
            || having_has_aggregate
        {
            return self.exec_select_generic(catalog_base, plan);
        }

        let catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, &plan.table)?;
        let schema = catalog.describe_table(&plan.table)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        let mut indexes = load_table_indexes(&catalog, &plan.table, &schema, catalog_base)?;

        let (projection_indexes, projection_names) = resolve_projection(&schema, &plan.projection)?;
        let order_by_indexes = resolve_order_by(&schema, &plan.order_by)?;
        let candidate_rids = choose_index_candidates(plan.selection.as_ref(), &mut indexes)?;
        let candidate_rows = collect_rows(&mut heap, &schema, candidate_rids)?;
        let mut subquery_cache: HashMap<String, Vec<Value>> = HashMap::new();
        let mut resolve_subquery = |subquery: &Statement| {
            self.resolve_subquery_values_cached(catalog_base, subquery, &mut subquery_cache)
        };

        let mut matched_rows = Vec::new();
        for (_, row) in candidate_rows {
            if !matches_selection_with_subquery(
                &schema,
                &row,
                plan.selection.as_ref(),
                &mut resolve_subquery,
            )? {
                continue;
            }
            matched_rows.push(row);
        }

        if !order_by_indexes.is_empty() {
            matched_rows
                .sort_by(|left, right| compare_rows_by_order(left, right, &order_by_indexes));
        }
        if let Some(n) = plan.limit {
            matched_rows.truncate(n);
        }

        let mut result_rows = Vec::new();
        for row in matched_rows {
            let mut out = Vec::new();
            for index in &projection_indexes {
                out.push(row.values[*index].clone());
            }
            result_rows.push(out);
        }

        Ok(QueryResult::Rows {
            columns: projection_names,
            rows: result_rows,
        })
    }

    fn exec_select_generic(&self, catalog_base: &Path, plan: SelectPlan) -> Result<QueryResult> {
        let mut table_names = vec![plan.table.clone()];
        for join in &plan.joins {
            if table_names
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&join.table))
            {
                return Err(RuseDbError::Parse(format!(
                    "joining the same table '{}' more than once requires alias support",
                    join.table
                )));
            }
            table_names.push(join.table.clone());
        }

        let mut rows = load_table_context_rows(catalog_base, &plan.table)?;
        let mut subquery_cache: HashMap<String, Vec<Value>> = HashMap::new();
        let mut resolve_subquery = |subquery: &Statement| {
            self.resolve_subquery_values_cached(catalog_base, subquery, &mut subquery_cache)
        };
        for join in plan.joins {
            let right_rows = load_table_context_rows(catalog_base, &join.table)?;
            let right_null_row = null_table_context_row(catalog_base, &join.table)?;
            let mut joined = Vec::new();
            for left in &rows {
                let mut matched = false;
                for right in &right_rows {
                    let merged = left.merge(right);
                    if matches_selection_ctx_with_subquery(
                        &merged,
                        Some(&join.on),
                        &mut resolve_subquery,
                    )? {
                        joined.push(merged);
                        matched = true;
                    }
                }
                if !matched && matches!(join.kind, rusedb_sql::JoinType::Left) {
                    joined.push(left.merge(&right_null_row));
                }
            }
            rows = joined;
        }

        let mut filtered_rows = Vec::new();
        for row in rows {
            if matches_selection_ctx_with_subquery(
                &row,
                plan.selection.as_ref(),
                &mut resolve_subquery,
            )? {
                filtered_rows.push(row);
            }
        }

        let has_aggregate = plan
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Aggregate { .. }));
        let having_has_aggregate = plan
            .having
            .as_ref()
            .map(expr_has_aggregate)
            .unwrap_or(false);
        if has_aggregate || !plan.group_by.is_empty() || having_has_aggregate {
            let columns = projection_labels(&plan.projection)?;
            let mut output_rows = project_aggregate_rows(
                &filtered_rows,
                &plan.projection,
                &plan.group_by,
                plan.having.as_ref(),
                has_aggregate || having_has_aggregate,
                &mut resolve_subquery,
            )?;
            if !plan.order_by.is_empty() {
                let order_indexes = resolve_output_order_by(&columns, &plan.order_by)?;
                output_rows.sort_by(|left, right| compare_output_rows(left, right, &order_indexes));
            }
            if let Some(n) = plan.limit {
                output_rows.truncate(n);
            }
            Ok(QueryResult::Rows {
                columns,
                rows: output_rows,
            })
        } else {
            let mut rows_for_projection = filtered_rows;
            if let Some(having_expr) = plan.having.as_ref() {
                let mut having_filtered = Vec::new();
                for row in rows_for_projection {
                    if matches_selection_ctx_with_subquery(
                        &row,
                        Some(having_expr),
                        &mut resolve_subquery,
                    )? {
                        having_filtered.push(row);
                    }
                }
                rows_for_projection = having_filtered;
            }
            if !plan.order_by.is_empty() {
                validate_order_by_ctx(&rows_for_projection, &plan.order_by)?;
                rows_for_projection
                    .sort_by(|left, right| compare_ctx_by_order(left, right, &plan.order_by));
            }
            let (columns, mut output_rows) = project_rows(&rows_for_projection, &plan.projection)?;
            if let Some(n) = plan.limit {
                output_rows.truncate(n);
            }
            Ok(QueryResult::Rows {
                columns,
                rows: output_rows,
            })
        }
    }

    fn exec_delete(
        &self,
        catalog_base: &Path,
        table_name: &str,
        selection: Option<Expr>,
    ) -> Result<QueryResult> {
        let catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let schema = catalog.describe_table(table_name)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        let mut indexes = load_table_indexes(&catalog, table_name, &schema, catalog_base)?;

        let candidate_rids = choose_index_candidates(selection.as_ref(), &mut indexes)?;
        let candidate_rows = collect_rows(&mut heap, &schema, candidate_rids)?;
        let mut subquery_cache: HashMap<String, Vec<Value>> = HashMap::new();
        let mut resolve_subquery = |subquery: &Statement| {
            self.resolve_subquery_values_cached(catalog_base, subquery, &mut subquery_cache)
        };

        let mut rows_to_delete = Vec::new();
        for (rid, row) in candidate_rows {
            if !matches_selection_with_subquery(
                &schema,
                &row,
                selection.as_ref(),
                &mut resolve_subquery,
            )? {
                continue;
            }
            rows_to_delete.push((rid, row));
        }

        ensure_parent_delete_restrict(
            catalog_base,
            &catalog,
            &table.name,
            &schema,
            &rows_to_delete,
        )?;

        let mut deleted = 0usize;
        for (rid, row) in rows_to_delete {
            heap.delete_record(rid)?;
            for table_index in &mut indexes {
                if let Some(key) = table_index
                    .index
                    .key_from_value(&row.values[table_index.column_index])?
                {
                    table_index.index.remove(&key, rid)?;
                }
            }
            deleted += 1;
        }

        heap.sync()?;
        Ok(QueryResult::AffectedRows(deleted))
    }

    fn exec_update(
        &self,
        catalog_base: &Path,
        table_name: &str,
        assignments: Vec<rusedb_sql::Assignment>,
        selection: Option<Expr>,
    ) -> Result<QueryResult> {
        if assignments.is_empty() {
            return Ok(QueryResult::AffectedRows(0));
        }

        let catalog = Catalog::open(catalog_base)?;
        let table = find_table(&catalog, table_name)?;
        let schema = catalog.describe_table(table_name)?;
        let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
        let mut indexes = load_table_indexes(&catalog, table_name, &schema, catalog_base)?;
        let constraints = load_constraint_checks(&catalog, table_name, &schema)?;

        let resolved_assignments = resolve_assignments(&schema, assignments)?;
        let candidate_rids = choose_index_candidates(selection.as_ref(), &mut indexes)?;
        let candidate_rows = collect_rows(&mut heap, &schema, candidate_rids)?;
        let mut subquery_cache: HashMap<String, Vec<Value>> = HashMap::new();
        let mut resolve_subquery = |subquery: &Statement| {
            self.resolve_subquery_values_cached(catalog_base, subquery, &mut subquery_cache)
        };

        let mut updates = Vec::new();
        for (rid, row) in candidate_rows {
            if !matches_selection_with_subquery(
                &schema,
                &row,
                selection.as_ref(),
                &mut resolve_subquery,
            )? {
                continue;
            }

            let mut new_values = row.values.clone();
            for (index, expr) in &resolved_assignments {
                let value = eval_expr(expr, &schema, &row)?;
                new_values[*index] = value;
            }
            normalize_values_for_schema(&schema, &mut new_values)?;
            updates.push((rid, row, new_values));
        }

        ensure_parent_update_restrict(catalog_base, &catalog, &table.name, &schema, &updates)?;
        validate_constraints_on_update(
            catalog_base,
            &catalog,
            &mut heap,
            &schema,
            &constraints,
            &updates,
        )?;

        let mut updated = 0usize;
        for (rid, row, new_values) in updates {
            let encoded = Row::new(new_values.clone()).encode(&schema)?;
            let new_rid = heap.update_record(rid, &encoded)?;

            for table_index in &mut indexes {
                if let Some(old_key) = table_index
                    .index
                    .key_from_value(&row.values[table_index.column_index])?
                {
                    table_index.index.remove(&old_key, rid)?;
                }
                if let Some(new_key) = table_index
                    .index
                    .key_from_value(&new_values[table_index.column_index])?
                {
                    table_index.index.insert(new_key, new_rid)?;
                }
            }
            updated += 1;
        }

        heap.sync()?;
        Ok(QueryResult::AffectedRows(updated))
    }

    fn resolve_subquery_values_cached(
        &self,
        catalog_base: &Path,
        subquery: &Statement,
        cache: &mut HashMap<String, Vec<Value>>,
    ) -> Result<Vec<Value>> {
        let cache_key = format!("{subquery:?}");
        if let Some(values) = cache.get(&cache_key) {
            return Ok(values.clone());
        }
        let values = self.execute_in_subquery_values(catalog_base, subquery)?;
        cache.insert(cache_key, values.clone());
        Ok(values)
    }

    fn execute_in_subquery_values(
        &self,
        catalog_base: &Path,
        subquery: &Statement,
    ) -> Result<Vec<Value>> {
        let Statement::Select { .. } = subquery else {
            return Err(RuseDbError::Parse(
                "IN subquery requires SELECT statement".to_string(),
            ));
        };
        let result = self.execute_statement_on_base(catalog_base, subquery.clone())?;
        let QueryResult::Rows { columns, rows } = result else {
            return Err(RuseDbError::Parse(
                "IN subquery must produce row results".to_string(),
            ));
        };
        if columns.len() != 1 {
            return Err(RuseDbError::Parse(format!(
                "IN subquery must return exactly one column, got {}",
                columns.len()
            )));
        }

        let mut out = Vec::with_capacity(rows.len());
        for mut row in rows {
            if row.len() != 1 {
                return Err(RuseDbError::Parse(
                    "IN subquery returned malformed row width".to_string(),
                ));
            }
            out.push(row.remove(0));
        }
        Ok(out)
    }

    fn create_database(&self, db_name: &str) -> Result<QueryResult> {
        let normalized = normalize_database_name(db_name)?;
        let mut databases = self.load_database_registry()?;
        if databases.iter().any(|name| name == &normalized) {
            return Err(RuseDbError::AlreadyExists {
                object: "database".to_string(),
                name: db_name.to_string(),
            });
        }
        databases.push(normalized.clone());
        databases.sort();
        persist_database_registry(&self.catalog_base, &databases)?;

        let db_base = self.database_base_for_name(&normalized)?;
        let _ = Catalog::open(&db_base)?;
        clear_wal(&db_base)?;
        Ok(QueryResult::Message(format!(
            "created database '{}'",
            normalized
        )))
    }

    fn drop_database(&self, db_name: &str) -> Result<QueryResult> {
        let normalized = normalize_database_name(db_name)?;
        if normalized == DEFAULT_DATABASE_NAME {
            return Err(RuseDbError::Parse(
                "cannot drop default database".to_string(),
            ));
        }
        {
            let state = self.lock_state()?;
            if state.tx.is_some() || state.tx_starting {
                return Err(RuseDbError::Parse(
                    "cannot drop database while a transaction is active".to_string(),
                ));
            }
            if state.current_db == normalized {
                return Err(RuseDbError::Parse(
                    "cannot drop database currently in use; switch to another database first"
                        .to_string(),
                ));
            }
        }

        let mut databases = self.load_database_registry()?;
        if !databases.iter().any(|name| name == &normalized) {
            return Err(RuseDbError::NotFound {
                object: "database".to_string(),
                name: db_name.to_string(),
            });
        }

        let db_base = self.database_base_for_name(&normalized)?;
        cleanup_family_files_for_base(&db_base)?;
        databases.retain(|name| name != &normalized);
        persist_database_registry(&self.catalog_base, &databases)?;
        Ok(QueryResult::Message(format!(
            "dropped database '{}'",
            normalized
        )))
    }

    fn use_database(&self, db_name: &str) -> Result<QueryResult> {
        let normalized = normalize_database_name(db_name)?;
        {
            let state = self.lock_state()?;
            if state.tx.is_some() || state.tx_starting {
                return Err(RuseDbError::Parse(
                    "cannot switch database while a transaction is active".to_string(),
                ));
            }
        }

        let databases = self.load_database_registry()?;
        if !databases.iter().any(|name| name == &normalized) {
            return Err(RuseDbError::NotFound {
                object: "database".to_string(),
                name: db_name.to_string(),
            });
        }

        let mut state = self.lock_state()?;
        state.current_db = normalized.clone();
        Ok(QueryResult::Message(format!(
            "using database '{}'",
            normalized
        )))
    }

    fn show_databases(&self) -> Result<QueryResult> {
        let databases = self.load_database_registry()?;
        let rows = databases
            .into_iter()
            .map(|name| vec![Value::Varchar(name)])
            .collect();
        Ok(QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows,
        })
    }

    fn show_current_database(&self) -> Result<QueryResult> {
        let current_db = {
            let state = self.lock_state()?;
            state.current_db.clone()
        };
        Ok(QueryResult::Rows {
            columns: vec!["database".to_string()],
            rows: vec![vec![Value::Varchar(current_db)]],
        })
    }

    fn show_tables(&self) -> Result<QueryResult> {
        let live_base = self.current_live_base()?;
        let catalog = Catalog::open(live_base)?;
        let rows = catalog
            .list_tables()
            .into_iter()
            .map(|table| {
                vec![
                    Value::BigInt(i64::from(table.table_id)),
                    Value::Varchar(table.name),
                ]
            })
            .collect();
        Ok(QueryResult::Rows {
            columns: vec!["table_id".to_string(), "name".to_string()],
            rows,
        })
    }

    fn exec_analyze_table(&self, catalog_base: &Path, table_name: &str) -> Result<QueryResult> {
        let stats = refresh_single_table_statistics(catalog_base, table_name)?;
        Ok(QueryResult::Message(format!(
            "analyzed table '{}' (rows={}, columns={})",
            stats.table,
            stats.row_count,
            stats.columns.len()
        )))
    }

    fn exec_explain(
        &self,
        catalog_base: &Path,
        analyze: bool,
        statement: Statement,
    ) -> Result<QueryResult> {
        let Statement::Select {
            table,
            joins,
            projection,
            selection,
            group_by,
            having,
            order_by,
            limit,
        } = statement
        else {
            return Err(RuseDbError::Parse(
                "EXPLAIN currently supports SELECT statements only".to_string(),
            ));
        };
        let plan = SelectPlan {
            table,
            joins,
            projection,
            selection,
            group_by,
            having,
            order_by,
            limit,
        };
        self.explain_select(catalog_base, plan, analyze)
    }

    fn explain_select(
        &self,
        catalog_base: &Path,
        plan: SelectPlan,
        analyze: bool,
    ) -> Result<QueryResult> {
        let table_stats = ensure_table_statistics(catalog_base, &plan.table)?;
        let catalog = Catalog::open(catalog_base)?;
        let schema = catalog.describe_table(&plan.table)?;
        let mut indexes = load_table_indexes(&catalog, &plan.table, &schema, catalog_base)?;

        let index_predicate = if let Some(selection) = plan.selection.as_ref() {
            extract_index_predicate(selection)?
        } else {
            None
        };
        let chosen_index = index_predicate.as_ref().and_then(|predicate| {
            indexes
                .iter()
                .find(|idx| idx.info.key_columns.eq_ignore_ascii_case(&predicate.column))
                .map(|idx| idx.info.name.clone())
        });
        let candidate_rids = choose_index_candidates(plan.selection.as_ref(), &mut indexes)?;
        let estimated_rows = candidate_rids
            .as_ref()
            .map(|rids| rids.len())
            .unwrap_or(table_stats.row_count);

        let access_path = if let (Some(index_name), Some(predicate)) =
            (chosen_index.as_ref(), index_predicate.as_ref())
        {
            format!(
                "INDEX {} on {} {} {}",
                index_name,
                predicate.column,
                binary_op_name(predicate.op),
                render_value_for_plan(&predicate.value)
            )
        } else {
            "FULL TABLE SCAN".to_string()
        };

        let mut rows = Vec::new();
        push_explain_row(&mut rows, "table", plan.table.clone());
        push_explain_row(&mut rows, "access_path", access_path);
        push_explain_row(&mut rows, "table_rows", table_stats.row_count.to_string());
        push_explain_row(&mut rows, "estimated_rows", estimated_rows.to_string());
        push_explain_row(
            &mut rows,
            "joins",
            if plan.joins.is_empty() {
                "0".to_string()
            } else {
                plan.joins
                    .iter()
                    .map(|join| format!("{:?} {}", join.kind, join.table))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        );
        push_explain_row(&mut rows, "group_by_count", plan.group_by.len().to_string());
        push_explain_row(
            &mut rows,
            "has_having",
            if plan.having.is_some() {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        push_explain_row(&mut rows, "order_by_count", plan.order_by.len().to_string());
        push_explain_row(
            &mut rows,
            "limit",
            plan.limit
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        );

        if let Some(predicate) = index_predicate.as_ref() {
            if let Some(column_stats) = find_column_statistics(&table_stats, &predicate.column) {
                let null_rate = if table_stats.row_count == 0 {
                    0.0
                } else {
                    column_stats.null_count as f64 / table_stats.row_count as f64
                };
                push_explain_row(
                    &mut rows,
                    "predicate_column_stats",
                    format!(
                        "{}: distinct={}, null_rate={:.4}",
                        column_stats.name, column_stats.distinct_count, null_rate
                    ),
                );
            }
        }

        if let Some(selection) = plan.selection.as_ref() {
            let indexable_count = count_indexable_predicates(selection);
            if indexable_count > 1 {
                push_explain_row(
                    &mut rows,
                    "index_strategy",
                    "multiple indexable predicates found; current strategy picks the first usable predicate".to_string(),
                );
            }
        }

        if analyze {
            let start = Instant::now();
            let executed = self.exec_select(catalog_base, plan.clone())?;
            let elapsed = start.elapsed();
            let output_rows = match executed {
                QueryResult::Rows { rows, .. } => rows.len(),
                QueryResult::AffectedRows(value) => value,
                QueryResult::Message(_) => 0,
            };
            let scanned_rows = estimate_scanned_rows_for_select(catalog_base, &plan)?;
            push_explain_row(
                &mut rows,
                "analyze_elapsed_ms",
                elapsed.as_millis().to_string(),
            );
            push_explain_row(&mut rows, "analyze_scanned_rows", scanned_rows.to_string());
            push_explain_row(&mut rows, "analyze_output_rows", output_rows.to_string());
        }

        Ok(QueryResult::Rows {
            columns: vec!["item".to_string(), "value".to_string()],
            rows,
        })
    }

    fn begin_transaction(&self) -> Result<QueryResult> {
        self.begin_transaction_internal()?;
        Ok(QueryResult::Message(format!(
            "transaction started (isolation: {ISOLATION_LEVEL})"
        )))
    }

    fn commit_transaction(&self) -> Result<QueryResult> {
        self.commit_transaction_internal()?;
        Ok(QueryResult::Message("transaction committed".to_string()))
    }

    fn rollback_transaction(&self) -> Result<QueryResult> {
        self.rollback_transaction_internal()?;
        Ok(QueryResult::Message("transaction rolled back".to_string()))
    }

    fn begin_transaction_internal(&self) -> Result<()> {
        let current_thread = std::thread::current().id();
        let start_wait = Instant::now();

        let (tx_id, current_db) = {
            let mut state = self.lock_state()?;
            loop {
                if let Some(tx) = &state.tx {
                    if tx.owner_thread == current_thread {
                        return Err(RuseDbError::Parse(
                            "transaction already active; commit or rollback first".to_string(),
                        ));
                    }
                }
                if state.tx.is_none() && !state.tx_starting {
                    break;
                }

                let elapsed = start_wait.elapsed();
                if elapsed >= self.lock_wait_timeout {
                    return Err(lock_wait_timeout_error(self.lock_wait_timeout));
                }
                let remaining = self.lock_wait_timeout.saturating_sub(elapsed);
                let (next_state, wait_result) = self
                    .tx_condvar
                    .wait_timeout(state, remaining)
                    .map_err(|_| {
                        RuseDbError::Corruption("engine transaction lock poisoned".to_string())
                    })?;
                state = next_state;
                if wait_result.timed_out() && (state.tx.is_some() || state.tx_starting) {
                    return Err(lock_wait_timeout_error(self.lock_wait_timeout));
                }
            }

            state.tx_starting = true;
            state.next_tx_id = state.next_tx_id.saturating_add(1);
            (state.next_tx_id, state.current_db.clone())
        };

        let live_base = self.database_base_for_name(&current_db)?;
        let tx_base = tx_base_path(&live_base, tx_id);
        let setup = (|| -> Result<()> {
            cleanup_family_files_for_base(&tx_base)?;
            copy_live_files_to_tx(&live_base, &tx_base)?;
            write_wal_reset(&live_base, WalEventKind::Begin, tx_id)?;
            Ok(())
        })();

        let mut state = self.lock_state()?;
        state.tx_starting = false;
        match setup {
            Ok(()) => {
                state.tx = Some(TransactionContext {
                    tx_id,
                    live_base,
                    working_base: tx_base,
                    owner_thread: current_thread,
                });
                self.tx_condvar.notify_all();
                Ok(())
            }
            Err(err) => {
                state.tx = None;
                self.tx_condvar.notify_all();
                Err(err)
            }
        }
    }

    fn commit_transaction_internal(&self) -> Result<()> {
        let current_thread = std::thread::current().id();
        let tx = {
            let state = self.lock_state()?;
            let tx = state
                .tx
                .clone()
                .ok_or(RuseDbError::Parse("no active transaction".to_string()))?;
            if tx.owner_thread != current_thread {
                return Err(RuseDbError::Parse(
                    "active transaction is owned by another session/thread".to_string(),
                ));
            }
            tx
        };

        write_wal_append(&tx.live_base, WalEventKind::CommitStart, tx.tx_id)?;
        apply_tx_files_to_live(&tx.live_base, &tx.working_base)?;
        write_wal_append(&tx.live_base, WalEventKind::CommitDone, tx.tx_id)?;
        cleanup_family_files_for_base(&tx.working_base)?;
        write_wal_checkpoint(&tx.live_base, tx.tx_id)?;

        let mut state = self.lock_state()?;
        state.tx = None;
        self.tx_condvar.notify_all();
        drop(state);
        let _ = refresh_all_table_statistics(&tx.live_base);
        Ok(())
    }

    fn rollback_transaction_internal(&self) -> Result<()> {
        let current_thread = std::thread::current().id();
        let tx = {
            let state = self.lock_state()?;
            let tx = state
                .tx
                .clone()
                .ok_or(RuseDbError::Parse("no active transaction".to_string()))?;
            if tx.owner_thread != current_thread {
                return Err(RuseDbError::Parse(
                    "active transaction is owned by another session/thread".to_string(),
                ));
            }
            tx
        };
        cleanup_family_files_for_base(&tx.working_base)?;
        clear_wal(&tx.live_base)?;
        let mut state = self.lock_state()?;
        state.tx = None;
        self.tx_condvar.notify_all();
        Ok(())
    }

    fn ensure_recovered(&self) -> Result<()> {
        {
            let state = self.lock_state()?;
            if state.wal_recovered {
                return Ok(());
            }
        }
        let databases = self.load_database_registry()?;
        for name in databases {
            let base = self.database_base_for_name(&name)?;
            recover_from_wal(&base)?;
        }
        let mut state = self.lock_state()?;
        state.wal_recovered = true;
        Ok(())
    }

    fn current_tx_base(&self) -> Result<PathBuf> {
        let current_thread = std::thread::current().id();
        let state = self.lock_state()?;
        state
            .tx
            .as_ref()
            .and_then(|tx| {
                if tx.owner_thread == current_thread {
                    Some(tx.working_base.clone())
                } else {
                    None
                }
            })
            .ok_or(RuseDbError::Parse("no active transaction".to_string()))
    }

    fn current_live_base(&self) -> Result<PathBuf> {
        let current_db = {
            let state = self.lock_state()?;
            state.current_db.clone()
        };
        self.database_base_for_name(&current_db)
    }

    fn wait_for_backup_lock_release_for_statement(&self, statement: &Statement) -> Result<()> {
        let lock_base = match statement {
            Statement::CreateDatabase { .. } | Statement::DropDatabase { .. } => {
                self.catalog_base.clone()
            }
            _ => self.current_live_base()?,
        };
        wait_for_backup_lock_release(&lock_base, self.lock_wait_timeout)
    }

    fn database_base_for_name(&self, db_name: &str) -> Result<PathBuf> {
        if db_name == DEFAULT_DATABASE_NAME {
            return Ok(self.catalog_base.clone());
        }
        let (dir, prefix) = family_dir_and_prefix(&self.catalog_base)?;
        Ok(dir.join(format!("{prefix}-db-{db_name}")))
    }

    fn load_database_registry(&self) -> Result<Vec<String>> {
        load_or_init_database_registry(&self.catalog_base)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, EngineState>> {
        self.state
            .lock()
            .map_err(|_| RuseDbError::Corruption("engine state lock poisoned".to_string()))
    }

    fn base_for_profile_log(&self) -> Result<PathBuf> {
        let (tx_live_base, current_db) = {
            let state = self.lock_state()?;
            (
                state.tx.as_ref().map(|tx| tx.live_base.clone()),
                state.current_db.clone(),
            )
        };
        if let Some(base) = tx_live_base {
            return Ok(base);
        }
        self.database_base_for_name(&current_db)
    }

    fn record_query_profile(
        &self,
        sql: &str,
        elapsed: Duration,
        result: &QueryResult,
    ) -> Result<()> {
        if elapsed < self.slow_query_threshold {
            return Ok(());
        }
        let base = self.base_for_profile_log()?;
        let path = slow_log_path(&base);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let result_metric = match result {
            QueryResult::Rows { rows, .. } => format!("rows={}", rows.len()),
            QueryResult::AffectedRows(value) => format!("affected={value}"),
            QueryResult::Message(message) => {
                let first = message.lines().next().unwrap_or_default();
                format!("message={first}")
            }
        };
        let compact_sql = sql.replace(['\n', '\r'], " ");
        file.write_all(
            format!(
                "{ts_ms}\t{}\t{result_metric}\t{compact_sql}\n",
                elapsed.as_millis()
            )
            .as_bytes(),
        )?;
        file.flush()?;
        Ok(())
    }
}

impl Executor for Engine {
    fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        let started = Instant::now();
        let statement = parse_sql(sql)?;
        let result = self.execute_statement(statement);
        if let Ok(ref value) = result {
            let _ = self.record_query_profile(sql, started.elapsed(), value);
        }
        result
    }
}

fn statement_requires_backup_lock_wait(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Begin
            | Statement::AnalyzeTable { .. }
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
    ) || is_mutating_statement(statement)
}

fn is_mutating_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::AlterTableAddColumn { .. }
            | Statement::AlterTableDropColumn { .. }
            | Statement::AlterTableAlterColumn { .. }
            | Statement::RenameTable { .. }
            | Statement::RenameColumn { .. }
            | Statement::CreateIndex { .. }
            | Statement::Insert { .. }
            | Statement::Delete { .. }
            | Statement::Update { .. }
    )
}

fn lock_wait_timeout_error(timeout: Duration) -> RuseDbError {
    RuseDbError::Parse(format!(
        "lock wait timeout after {} ms while waiting for transaction lock",
        timeout.as_millis()
    ))
}

fn backup_lock_wait_timeout_error(timeout: Duration) -> RuseDbError {
    RuseDbError::Parse(format!(
        "backup lock wait timeout after {} ms while waiting for online backup to finish",
        timeout.as_millis()
    ))
}

fn backup_lock_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".backup.lock");
    PathBuf::from(os)
}

fn wait_for_backup_lock_release(base: &Path, timeout: Duration) -> Result<()> {
    let lock_path = backup_lock_path(base);
    let start = Instant::now();
    while lock_path.exists() {
        if start.elapsed() >= timeout {
            return Err(backup_lock_wait_timeout_error(timeout));
        }
        std::thread::sleep(BACKUP_LOCK_WAIT_POLL);
    }
    Ok(())
}

fn normalize_database_name(name: &str) -> Result<String> {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(RuseDbError::Parse(
            "database name cannot be empty".to_string(),
        ));
    }
    let mut chars = normalized.chars();
    let Some(first) = chars.next() else {
        return Err(RuseDbError::Parse(
            "database name cannot be empty".to_string(),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(RuseDbError::Parse(format!(
            "invalid database name '{}': must start with a letter or underscore",
            name
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(RuseDbError::Parse(format!(
            "invalid database name '{}': only letters, digits and underscore are allowed",
            name
        )));
    }
    Ok(normalized)
}

fn databases_manifest_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".databases");
    PathBuf::from(os)
}

fn load_or_init_database_registry(base: &Path) -> Result<Vec<String>> {
    let manifest = databases_manifest_path(base);
    let mut databases = Vec::new();
    if manifest.exists() {
        let content = fs::read_to_string(&manifest)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let normalized = normalize_database_name(trimmed)?;
            if !databases.iter().any(|name| name == &normalized) {
                databases.push(normalized);
            }
        }
    }

    if !databases.iter().any(|name| name == DEFAULT_DATABASE_NAME) {
        databases.push(DEFAULT_DATABASE_NAME.to_string());
    }
    databases.sort();
    persist_database_registry(base, &databases)?;
    Ok(databases)
}

fn persist_database_registry(base: &Path, databases: &[String]) -> Result<()> {
    let manifest = databases_manifest_path(base);
    let content = if databases.is_empty() {
        String::new()
    } else {
        format!("{}\n", databases.join("\n"))
    };
    fs::write(manifest, content)?;
    Ok(())
}

fn stats_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".stats");
    PathBuf::from(os)
}

fn slow_log_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".slowlog");
    PathBuf::from(os)
}

fn parse_stats_header(line: &str) -> Result<u32> {
    let mut parts = line.split_whitespace();
    let magic = parts
        .next()
        .ok_or_else(|| RuseDbError::Corruption("stats header missing magic".to_string()))?;
    let version_raw = parts
        .next()
        .ok_or_else(|| RuseDbError::Corruption("stats header missing version".to_string()))?;
    if parts.next().is_some() {
        return Err(RuseDbError::Corruption(
            "stats header contains unexpected extra fields".to_string(),
        ));
    }
    if magic != STATS_MAGIC {
        return Err(RuseDbError::Corruption(format!(
            "unknown stats header magic '{magic}'"
        )));
    }
    let version = version_raw.parse::<u32>().map_err(|_| {
        RuseDbError::Corruption(format!("invalid stats header version '{version_raw}'"))
    })?;
    Ok(version)
}

fn load_statistics(base: &Path) -> Result<HashMap<String, TableStatistics>> {
    let path = stats_path(base);
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let content = fs::read_to_string(path)?;
    let mut lines = content.lines();
    let Some(header_line) = lines.next() else {
        return Ok(HashMap::new());
    };
    let version = parse_stats_header(header_line)?;
    if version != STATS_VERSION {
        return Err(RuseDbError::Corruption(format!(
            "unsupported stats version {version}, expected {STATS_VERSION}",
        )));
    }

    let mut out: HashMap<String, TableStatistics> = HashMap::new();
    let mut current: Option<TableStatistics> = None;
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let tag = parts
            .next()
            .ok_or_else(|| RuseDbError::Corruption("malformed stats record".to_string()))?;
        match tag {
            "TABLE" => {
                if current.is_some() {
                    return Err(RuseDbError::Corruption(
                        "stats TABLE section not closed with END".to_string(),
                    ));
                }
                let table = parts
                    .next()
                    .ok_or_else(|| RuseDbError::Corruption("stats TABLE missing name".to_string()))?
                    .to_string();
                let row_count = parts
                    .next()
                    .ok_or_else(|| {
                        RuseDbError::Corruption("stats TABLE missing row count".to_string())
                    })?
                    .parse::<usize>()
                    .map_err(|_| {
                        RuseDbError::Corruption("stats TABLE row count is invalid".to_string())
                    })?;
                let updated_unix_ms = parts
                    .next()
                    .ok_or_else(|| {
                        RuseDbError::Corruption("stats TABLE missing updated timestamp".to_string())
                    })?
                    .parse::<u64>()
                    .map_err(|_| {
                        RuseDbError::Corruption(
                            "stats TABLE updated timestamp is invalid".to_string(),
                        )
                    })?;
                if parts.next().is_some() {
                    return Err(RuseDbError::Corruption(
                        "stats TABLE contains unexpected extra fields".to_string(),
                    ));
                }
                current = Some(TableStatistics {
                    table: table.clone(),
                    row_count,
                    updated_unix_ms,
                    columns: Vec::new(),
                });
            }
            "COLUMN" => {
                let table = current.as_mut().ok_or_else(|| {
                    RuseDbError::Corruption(
                        "stats COLUMN appears outside TABLE section".to_string(),
                    )
                })?;
                let name = parts
                    .next()
                    .ok_or_else(|| {
                        RuseDbError::Corruption("stats COLUMN missing name".to_string())
                    })?
                    .to_string();
                let distinct_count = parts
                    .next()
                    .ok_or_else(|| {
                        RuseDbError::Corruption("stats COLUMN missing distinct count".to_string())
                    })?
                    .parse::<usize>()
                    .map_err(|_| {
                        RuseDbError::Corruption(
                            "stats COLUMN distinct count is invalid".to_string(),
                        )
                    })?;
                let null_count = parts
                    .next()
                    .ok_or_else(|| {
                        RuseDbError::Corruption("stats COLUMN missing null count".to_string())
                    })?
                    .parse::<usize>()
                    .map_err(|_| {
                        RuseDbError::Corruption("stats COLUMN null count is invalid".to_string())
                    })?;
                if parts.next().is_some() {
                    return Err(RuseDbError::Corruption(
                        "stats COLUMN contains unexpected extra fields".to_string(),
                    ));
                }
                table.columns.push(ColumnStatistics {
                    name,
                    distinct_count,
                    null_count,
                });
            }
            "END" => {
                let table = current.take().ok_or_else(|| {
                    RuseDbError::Corruption("stats END appears without TABLE".to_string())
                })?;
                out.insert(table.table.to_ascii_lowercase(), table);
            }
            other => {
                return Err(RuseDbError::Corruption(format!(
                    "unknown stats record '{other}'"
                )));
            }
        }
    }
    if current.is_some() {
        return Err(RuseDbError::Corruption(
            "stats TABLE section not closed with END at EOF".to_string(),
        ));
    }
    Ok(out)
}

fn persist_statistics(base: &Path, tables: &HashMap<String, TableStatistics>) -> Result<()> {
    let mut keys = tables.keys().cloned().collect::<Vec<_>>();
    keys.sort();

    let mut content = String::new();
    content.push_str(&format!("{STATS_MAGIC} {STATS_VERSION}\n"));
    for key in keys {
        let Some(table) = tables.get(&key) else {
            continue;
        };
        content.push_str(&format!(
            "TABLE {} {} {}\n",
            table.table, table.row_count, table.updated_unix_ms
        ));
        for column in &table.columns {
            content.push_str(&format!(
                "COLUMN {} {} {}\n",
                column.name, column.distinct_count, column.null_count
            ));
        }
        content.push_str("END\n");
    }

    fs::write(stats_path(base), content)?;
    Ok(())
}

fn value_stats_key(value: &Value) -> String {
    match value {
        Value::Int(v) => format!("i:{v}"),
        Value::BigInt(v) => format!("I:{v}"),
        Value::Bool(v) => format!("b:{v}"),
        Value::Double(v) => format!("d:{v}"),
        Value::Varchar(v) => format!("s:{}:{v}", v.len()),
        Value::Null => "n".to_string(),
    }
}

fn compute_table_statistics(catalog_base: &Path, table_name: &str) -> Result<TableStatistics> {
    let catalog = Catalog::open(catalog_base)?;
    let table = find_table(&catalog, table_name)?;
    let schema = catalog.describe_table(table_name)?;
    let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
    let mut distinct_sets = vec![HashSet::new(); schema.columns.len()];
    let mut null_counts = vec![0usize; schema.columns.len()];
    let mut row_count = 0usize;

    for (_, raw) in heap.scan_records()? {
        let row = Row::decode(&schema, &raw)?;
        row_count += 1;
        for (index, value) in row.values.iter().enumerate() {
            if matches!(value, Value::Null) {
                null_counts[index] = null_counts[index].saturating_add(1);
            } else {
                distinct_sets[index].insert(value_stats_key(value));
            }
        }
    }

    let updated_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let columns = schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| ColumnStatistics {
            name: column.name.clone(),
            distinct_count: distinct_sets[index].len(),
            null_count: null_counts[index],
        })
        .collect();
    Ok(TableStatistics {
        table: table.name,
        row_count,
        updated_unix_ms,
        columns,
    })
}

fn refresh_single_table_statistics(
    catalog_base: &Path,
    table_name: &str,
) -> Result<TableStatistics> {
    let mut all = load_statistics(catalog_base)?;
    let stats = compute_table_statistics(catalog_base, table_name)?;
    all.insert(stats.table.to_ascii_lowercase(), stats.clone());
    persist_statistics(catalog_base, &all)?;
    Ok(stats)
}

fn refresh_all_table_statistics(catalog_base: &Path) -> Result<()> {
    let catalog = Catalog::open(catalog_base)?;
    let tables = catalog.list_tables();
    let mut all = HashMap::new();
    for table in tables {
        let stats = compute_table_statistics(catalog_base, &table.name)?;
        all.insert(table.name.to_ascii_lowercase(), stats);
    }
    persist_statistics(catalog_base, &all)
}

fn ensure_table_statistics(catalog_base: &Path, table_name: &str) -> Result<TableStatistics> {
    let all = load_statistics(catalog_base)?;
    let key = table_name.to_ascii_lowercase();
    if let Some(stats) = all.get(&key) {
        return Ok(stats.clone());
    }
    refresh_single_table_statistics(catalog_base, table_name)
}

fn find_column_statistics<'a>(
    table: &'a TableStatistics,
    column_name: &str,
) -> Option<&'a ColumnStatistics> {
    table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
}

fn wal_path(base: &Path) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(".wal");
    PathBuf::from(os)
}

fn tx_base_path(base: &Path, tx_id: u64) -> PathBuf {
    let mut os = base.as_os_str().to_os_string();
    os.push(format!(".tx-{tx_id}"));
    PathBuf::from(os)
}

fn wal_header_line() -> String {
    format!("{WAL_MAGIC} {WAL_VERSION}\n")
}

fn parse_wal_header(line: &str) -> Result<Option<u32>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let mut parts = trimmed.split_whitespace();
    let Some(magic) = parts.next() else {
        return Ok(None);
    };
    if magic != WAL_MAGIC {
        return Ok(None);
    }
    let version_raw = parts.next().ok_or(RuseDbError::Corruption(
        "malformed WAL header: missing version".to_string(),
    ))?;
    if parts.next().is_some() {
        return Err(RuseDbError::Corruption(
            "malformed WAL header: unexpected extra fields".to_string(),
        ));
    }
    let version = version_raw.parse::<u32>().map_err(|_| {
        RuseDbError::Corruption(format!("malformed WAL header version '{version_raw}'"))
    })?;
    Ok(Some(version))
}

fn write_wal_reset(base: &Path, event: WalEventKind, tx_id: u64) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(wal_path(base))?;
    file.write_all(wal_header_line().as_bytes())?;
    file.write_all(format!("{} {tx_id}\n", wal_event_name(event)).as_bytes())?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn write_wal_append(base: &Path, event: WalEventKind, tx_id: u64) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path(base))?;
    if file.metadata()?.len() == 0 {
        file.write_all(wal_header_line().as_bytes())?;
    }
    file.write_all(format!("{} {tx_id}\n", wal_event_name(event)).as_bytes())?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn write_wal_checkpoint(base: &Path, tx_id: u64) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(wal_path(base))?;
    file.write_all(wal_header_line().as_bytes())?;
    file.write_all(format!("{} {tx_id}\n", wal_event_name(WalEventKind::Checkpoint)).as_bytes())?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn clear_wal(base: &Path) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(wal_path(base))?;
    file.write_all(wal_header_line().as_bytes())?;
    file.flush()?;
    file.sync_data()?;
    Ok(())
}

fn wal_event_name(event: WalEventKind) -> &'static str {
    match event {
        WalEventKind::Begin => "BEGIN",
        WalEventKind::CommitStart => "COMMIT_START",
        WalEventKind::CommitDone => "COMMIT_DONE",
        WalEventKind::Checkpoint => "CHECKPOINT",
    }
}

fn recover_from_wal(base: &Path) -> Result<()> {
    let wal = wal_path(base);
    if !wal.exists() {
        return Ok(());
    }
    let mut content = String::new();
    OpenOptions::new()
        .read(true)
        .open(&wal)?
        .read_to_string(&mut content)?;
    if content.trim().is_empty() {
        clear_wal(base)?;
        return Ok(());
    }

    let mut header_parsed = false;
    let mut begin_tx = None;
    let mut commit_started = false;
    let mut commit_done = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !header_parsed {
            if let Some(version) = parse_wal_header(trimmed)? {
                if version != WAL_VERSION {
                    return Err(RuseDbError::Corruption(format!(
                        "unsupported WAL version {version}, expected {WAL_VERSION}",
                    )));
                }
                header_parsed = true;
                continue;
            }
            header_parsed = true;
        }
        let mut parts = trimmed.split_whitespace();
        let event = parts.next().ok_or(RuseDbError::Corruption(
            "malformed WAL record: missing event kind".to_string(),
        ))?;
        let tx_id_raw = parts.next().ok_or(RuseDbError::Corruption(
            "malformed WAL record: missing tx id".to_string(),
        ))?;
        let tx_id = tx_id_raw.parse::<u64>().map_err(|_| {
            RuseDbError::Corruption(format!(
                "malformed WAL tx id '{tx_id_raw}' in line '{line}'"
            ))
        })?;
        match event {
            "BEGIN" => {
                begin_tx = Some(tx_id);
                commit_started = false;
                commit_done = false;
            }
            "COMMIT_START" => {
                if begin_tx == Some(tx_id) {
                    commit_started = true;
                }
            }
            "COMMIT_DONE" => {
                if begin_tx == Some(tx_id) {
                    commit_done = true;
                }
            }
            "CHECKPOINT" => {
                begin_tx = None;
                commit_started = false;
                commit_done = false;
            }
            _ => {
                return Err(RuseDbError::Corruption(format!(
                    "unknown WAL event '{event}' in line '{line}'"
                )));
            }
        }
    }

    if let Some(tx_id) = begin_tx {
        let tx_base = tx_base_path(base, tx_id);
        if commit_started && !commit_done {
            apply_tx_files_to_live(base, &tx_base)?;
            cleanup_family_files_for_base(&tx_base)?;
            write_wal_checkpoint(base, tx_id)?;
            return Ok(());
        }
        cleanup_family_files_for_base(&tx_base)?;
    }
    clear_wal(base)?;
    Ok(())
}

fn copy_live_files_to_tx(base: &Path, tx_base: &Path) -> Result<()> {
    let live_files = list_live_db_files(base)?;
    let (dir, live_prefix) = family_dir_and_prefix(base)?;
    let (_, tx_prefix) = family_dir_and_prefix(tx_base)?;
    for src in live_files {
        let file_name = src
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or(RuseDbError::Corruption(format!(
                "invalid source file path '{}'",
                src.display()
            )))?;
        let target_name = remap_family_name(&file_name, &live_prefix, &tx_prefix)?;
        fs::copy(&src, dir.join(target_name))?;
    }
    Ok(())
}

fn apply_tx_files_to_live(base: &Path, tx_base: &Path) -> Result<()> {
    let tx_files = list_tx_db_files(tx_base)?;
    let live_files_before = list_live_db_files(base)?;
    if tx_files.is_empty() && !live_files_before.is_empty() {
        return Err(RuseDbError::Corruption(
            "commit recovery found empty tx files with non-empty live database".to_string(),
        ));
    }

    let (dir, tx_prefix) = family_dir_and_prefix(tx_base)?;
    let (_, live_prefix) = family_dir_and_prefix(base)?;

    let mut target_names = HashSet::new();
    let mut copy_map = Vec::new();
    for tx_file in tx_files {
        let tx_name = tx_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or(RuseDbError::Corruption(format!(
                "invalid tx file path '{}'",
                tx_file.display()
            )))?;
        let live_name = remap_family_name(&tx_name, &tx_prefix, &live_prefix)?;
        target_names.insert(live_name.clone());
        copy_map.push((tx_file, dir.join(live_name)));
    }

    for (src, dst) in copy_map {
        fs::copy(src, dst)?;
    }

    let live_files_after_copy = list_live_db_files(base)?;
    for live_file in live_files_after_copy {
        let live_name = live_file
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or(RuseDbError::Corruption(format!(
                "invalid live file path '{}'",
                live_file.display()
            )))?;
        if !target_names.contains(&live_name) {
            fs::remove_file(live_file)?;
        }
    }
    Ok(())
}

fn family_dir_and_prefix(base: &Path) -> Result<(PathBuf, String)> {
    let dir = base
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let prefix = base
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .ok_or(RuseDbError::Parse(format!(
            "invalid database base path '{}'",
            base.display()
        )))?;
    Ok((dir, prefix))
}

fn cleanup_family_files_for_base(base: &Path) -> Result<()> {
    for file in list_family_files(base, false, false)? {
        fs::remove_file(file)?;
    }
    Ok(())
}

fn list_live_db_files(base: &Path) -> Result<Vec<PathBuf>> {
    list_family_files(base, true, true)
}

fn list_tx_db_files(tx_base: &Path) -> Result<Vec<PathBuf>> {
    list_family_files(tx_base, false, false)
}

fn list_family_files(
    base: &Path,
    exclude_nested_txs: bool,
    exclude_wal: bool,
) -> Result<Vec<PathBuf>> {
    let (dir, prefix) = family_dir_and_prefix(base)?;
    let starts_with = format!("{prefix}.");
    let nested_tx_prefix = format!("{prefix}.tx-");
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with(&starts_with) {
            continue;
        }
        if exclude_nested_txs && file_name.starts_with(&nested_tx_prefix) {
            continue;
        }
        if exclude_wal && file_name.ends_with(".wal") {
            continue;
        }
        if is_non_transactional_family_file(&file_name) {
            continue;
        }
        out.push(entry.path());
    }
    Ok(out)
}

fn is_non_transactional_family_file(file_name: &str) -> bool {
    file_name.ends_with(".stats") || file_name.ends_with(".slowlog")
}

fn remap_family_name(name: &str, from_prefix: &str, to_prefix: &str) -> Result<String> {
    let suffix = name
        .strip_prefix(from_prefix)
        .ok_or(RuseDbError::Corruption(format!(
            "file '{name}' does not match expected prefix '{from_prefix}'"
        )))?;
    Ok(format!("{to_prefix}{suffix}"))
}

fn find_table(catalog: &Catalog, table_name: &str) -> Result<TableInfo> {
    catalog.get_table(table_name)
}

fn table_heap_path(base: &Path, table_id: u32) -> PathBuf {
    let mut os: OsString = base.as_os_str().to_os_string();
    os.push(format!(".table-{table_id}.heap"));
    PathBuf::from(os)
}

fn index_file_path(base: &Path, index_id: u32) -> PathBuf {
    let mut os: OsString = base.as_os_str().to_os_string();
    os.push(format!(".index-{index_id}.idx"));
    PathBuf::from(os)
}

fn load_table_indexes(
    catalog: &Catalog,
    table_name: &str,
    schema: &Schema,
    catalog_base: &Path,
) -> Result<Vec<TableIndex>> {
    let mut out = Vec::new();
    for info in catalog.list_indexes(table_name)? {
        let (column_index, column) =
            schema
                .find_column(&info.key_columns)
                .ok_or(RuseDbError::Corruption(format!(
                    "index '{}' refers to missing column '{}'",
                    info.name, info.key_columns
                )))?;
        let key_kind = IndexKeyKind::from_data_type(column.data_type)?;
        let index = OrderedIndex::open(index_file_path(catalog_base, info.index_id), key_kind)?;
        out.push(TableIndex {
            info,
            column_index,
            index,
        });
    }
    Ok(out)
}

fn rewrite_table_rows<F>(
    catalog_base: &Path,
    table_id: u32,
    old_schema: &Schema,
    new_schema: &Schema,
    mut transform: F,
) -> Result<()>
where
    F: FnMut(&Row) -> Result<Vec<Value>>,
{
    let heap_path = table_heap_path(catalog_base, table_id);
    let mut old_heap = HeapFile::open(&heap_path)?;
    let mut rewritten_rows = Vec::new();
    for (_rid, raw) in old_heap.scan_records()? {
        let row = Row::decode(old_schema, &raw)?;
        let mut values = transform(&row)?;
        normalize_values_for_schema(new_schema, &mut values)?;
        rewritten_rows.push(Row::new(values).encode(new_schema)?);
    }
    drop(old_heap);

    if heap_path.exists() {
        fs::remove_file(&heap_path)?;
    }
    let mut new_heap = HeapFile::open(&heap_path)?;
    for raw in rewritten_rows {
        new_heap.insert_record(&raw)?;
    }
    new_heap.sync()?;
    Ok(())
}

fn rebuild_table_indexes(
    catalog_base: &Path,
    catalog: &Catalog,
    table_name: &str,
    schema: &Schema,
) -> Result<()> {
    let table = find_table(catalog, table_name)?;
    let indexes = catalog.list_indexes(table_name)?;
    if indexes.is_empty() {
        return Ok(());
    }

    let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
    let rows = heap.scan_records()?;
    let mut opened_indexes = Vec::new();
    for info in indexes {
        let (column_index, column) =
            schema
                .find_column(&info.key_columns)
                .ok_or(RuseDbError::Corruption(format!(
                    "index '{}' refers to missing column '{}'",
                    info.name, info.key_columns
                )))?;
        let key_kind = IndexKeyKind::from_data_type(column.data_type)?;
        let path = index_file_path(catalog_base, info.index_id);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        opened_indexes.push((info, column_index, OrderedIndex::open(path, key_kind)?));
    }

    for (rid, raw) in rows {
        let row = Row::decode(schema, &raw)?;
        for (_, column_index, index) in &mut opened_indexes {
            if let Some(key) = index.key_from_value(&row.values[*column_index])? {
                index.insert(key, rid)?;
            }
        }
    }
    Ok(())
}

fn build_table_definition(
    table_name: &str,
    column_defs: Vec<ColumnDef>,
    table_constraints: Vec<TableConstraint>,
) -> Result<(Vec<Column>, Vec<ConstraintDef>)> {
    if column_defs.is_empty() {
        return Err(RuseDbError::InvalidSchema(
            "table must define at least one column".to_string(),
        ));
    }

    let mut columns = column_defs
        .iter()
        .map(|column| Column::new(column.name.clone(), column.data_type, column.nullable))
        .collect::<Vec<_>>();
    let mut column_indexes = HashMap::new();
    for (idx, column) in columns.iter().enumerate() {
        let key = column.name.to_ascii_lowercase();
        if column_indexes.insert(key, idx).is_some() {
            return Err(RuseDbError::InvalidSchema(format!(
                "duplicate column name '{}'",
                column.name
            )));
        }
    }

    let mut raw_constraints = Vec::new();
    for column in &column_defs {
        if column.primary_key {
            raw_constraints.push(TableConstraint {
                name: None,
                kind: TableConstraintKind::PrimaryKey(vec![column.name.clone()]),
            });
        }
        if column.unique {
            raw_constraints.push(TableConstraint {
                name: None,
                kind: TableConstraintKind::Unique(vec![column.name.clone()]),
            });
        }
        if let Some(reference) = &column.references {
            raw_constraints.push(TableConstraint {
                name: None,
                kind: TableConstraintKind::ForeignKey {
                    columns: vec![column.name.clone()],
                    referenced_table: reference.table.clone(),
                    referenced_columns: reference.columns.clone(),
                },
            });
        }
    }
    raw_constraints.extend(table_constraints);

    let mut out = Vec::with_capacity(raw_constraints.len());
    let mut has_primary_key = false;
    let mut signatures = HashMap::new();
    for constraint in raw_constraints {
        let (kind, keys, referenced_table, mut referenced_columns) = match constraint.kind {
            TableConstraintKind::PrimaryKey(keys) => {
                (ConstraintKind::PrimaryKey, keys, None, Vec::new())
            }
            TableConstraintKind::Unique(keys) => (ConstraintKind::Unique, keys, None, Vec::new()),
            TableConstraintKind::ForeignKey {
                columns,
                referenced_table,
                referenced_columns,
            } => (
                ConstraintKind::ForeignKey,
                columns,
                Some(referenced_table),
                referenced_columns,
            ),
        };
        if keys.is_empty() {
            return Err(RuseDbError::InvalidSchema(
                "constraint key columns cannot be empty".to_string(),
            ));
        }
        if matches!(kind, ConstraintKind::PrimaryKey) {
            if has_primary_key {
                return Err(RuseDbError::InvalidSchema(format!(
                    "table '{}' can have only one PRIMARY KEY",
                    table_name
                )));
            }
            has_primary_key = true;
        }

        let mut key_names = HashSet::new();
        let mut normalized_keys = Vec::with_capacity(keys.len());
        for key in keys {
            let key_trimmed = key.trim();
            if key_trimmed.is_empty() {
                return Err(RuseDbError::InvalidSchema(
                    "constraint contains empty column name".to_string(),
                ));
            }
            let key_normalized = key_trimmed.to_ascii_lowercase();
            if !key_names.insert(key_normalized.clone()) {
                return Err(RuseDbError::InvalidSchema(format!(
                    "constraint has duplicate key column '{}'",
                    key_trimmed
                )));
            }
            let index =
                column_indexes
                    .get(&key_normalized)
                    .copied()
                    .ok_or(RuseDbError::NotFound {
                        object: "column".to_string(),
                        name: key_trimmed.to_string(),
                    })?;
            if matches!(kind, ConstraintKind::PrimaryKey) {
                columns[index].nullable = false;
            }
            normalized_keys.push(columns[index].name.clone());
        }
        if matches!(kind, ConstraintKind::ForeignKey) {
            if referenced_columns.is_empty() {
                return Err(RuseDbError::InvalidSchema(
                    "FOREIGN KEY referenced columns cannot be empty".to_string(),
                ));
            }
            if referenced_columns.len() != normalized_keys.len() {
                return Err(RuseDbError::InvalidSchema(
                    "FOREIGN KEY requires same number of source and referenced columns".to_string(),
                ));
            }
            referenced_columns = referenced_columns
                .into_iter()
                .map(|col| col.trim().to_string())
                .collect();
            if referenced_columns.iter().any(|col| col.is_empty()) {
                return Err(RuseDbError::InvalidSchema(
                    "FOREIGN KEY referenced column name cannot be empty".to_string(),
                ));
            }
        }

        let name = constraint.name.unwrap_or_else(|| {
            default_constraint_name(
                table_name,
                kind,
                &normalized_keys,
                referenced_table.as_deref(),
                &referenced_columns,
            )
        });
        let signature = constraint_signature(
            kind,
            &normalized_keys,
            referenced_table.as_deref(),
            &referenced_columns,
        );
        if let Some(existing_name) = signatures.insert(signature, name.clone()) {
            return Err(RuseDbError::InvalidSchema(format!(
                "constraint '{}' duplicates '{}'",
                name, existing_name
            )));
        }

        out.push(ConstraintDef {
            name,
            kind,
            key_columns: normalized_keys,
            referenced_table,
            referenced_columns,
        });
    }

    Ok((columns, out))
}

fn load_constraint_checks(
    catalog: &Catalog,
    table_name: &str,
    schema: &Schema,
) -> Result<Vec<ConstraintCheck>> {
    let mut out = Vec::new();
    for info in catalog.list_constraints(table_name)? {
        out.push(constraint_check_from_info(catalog, schema, info)?);
    }
    Ok(out)
}

fn constraint_check_from_info(
    catalog: &Catalog,
    schema: &Schema,
    info: ConstraintInfo,
) -> Result<ConstraintCheck> {
    let mut indexes = Vec::with_capacity(info.key_columns.len());
    for key in &info.key_columns {
        let (index, _) = schema
            .find_column(key)
            .ok_or(RuseDbError::Corruption(format!(
                "constraint '{}' refers to missing column '{}'",
                info.name, key
            )))?;
        indexes.push(index);
    }
    let mut referenced_column_indexes = Vec::new();
    if matches!(info.kind, ConstraintKind::ForeignKey) {
        let referenced_table = info
            .referenced_table
            .as_ref()
            .ok_or(RuseDbError::Corruption(format!(
                "FOREIGN KEY constraint '{}' missing referenced table",
                info.name
            )))?;
        let referenced_schema = catalog.describe_table(referenced_table)?;
        for col in &info.referenced_columns {
            let (index, _) = referenced_schema
                .find_column(col)
                .ok_or(RuseDbError::Corruption(format!(
                    "FOREIGN KEY constraint '{}' refers to missing referenced column '{}.{}'",
                    info.name, referenced_table, col
                )))?;
            referenced_column_indexes.push(index);
        }
    }
    Ok(ConstraintCheck {
        info,
        column_indexes: indexes,
        referenced_column_indexes,
    })
}

fn validate_constraints_on_insert(
    catalog_base: &Path,
    catalog: &Catalog,
    heap: &mut HeapFile,
    schema: &Schema,
    constraints: &[ConstraintCheck],
    new_rows: &[Vec<Value>],
) -> Result<()> {
    if constraints.is_empty() || new_rows.is_empty() {
        return Ok(());
    }

    for constraint in constraints {
        if !matches!(
            constraint.info.kind,
            ConstraintKind::PrimaryKey | ConstraintKind::Unique
        ) {
            continue;
        }
        let mut seen = HashSet::new();
        for (_rid, raw) in heap.scan_records()? {
            let row = Row::decode(schema, &raw)?;
            if let Some(key) = constraint_key_for_values(&row.values, constraint)? {
                if !seen.insert(key) {
                    return Err(duplicate_constraint_error(constraint));
                }
            }
        }
        for values in new_rows {
            if let Some(key) = constraint_key_for_values(values, constraint)? {
                if !seen.insert(key) {
                    return Err(duplicate_constraint_error(constraint));
                }
            }
        }
    }
    for constraint in constraints {
        if !matches!(constraint.info.kind, ConstraintKind::ForeignKey) {
            continue;
        }
        let parent_key_set = load_parent_key_set(catalog_base, catalog, constraint)?;
        for values in new_rows {
            if let Some(key) = constraint_key_for_values(values, constraint)?
                && !parent_key_set.contains(&key)
            {
                return Err(foreign_key_violation_error(constraint));
            }
        }
    }
    Ok(())
}

fn validate_constraints_on_update(
    catalog_base: &Path,
    catalog: &Catalog,
    heap: &mut HeapFile,
    schema: &Schema,
    constraints: &[ConstraintCheck],
    updates: &[(Rid, Row, Vec<Value>)],
) -> Result<()> {
    if constraints.is_empty() || updates.is_empty() {
        return Ok(());
    }
    let updates_by_rid = updates
        .iter()
        .map(|(rid, _, values)| (*rid, values.clone()))
        .collect::<HashMap<_, _>>();

    for constraint in constraints {
        if !matches!(
            constraint.info.kind,
            ConstraintKind::PrimaryKey | ConstraintKind::Unique
        ) {
            continue;
        }
        let mut seen = HashSet::new();
        for (rid, raw) in heap.scan_records()? {
            let row = Row::decode(schema, &raw)?;
            let values = updates_by_rid
                .get(&rid)
                .map(Vec::as_slice)
                .unwrap_or(row.values.as_slice());
            if let Some(key) = constraint_key_for_values(values, constraint)? {
                if !seen.insert(key) {
                    return Err(duplicate_constraint_error(constraint));
                }
            }
        }
    }
    for constraint in constraints {
        if !matches!(constraint.info.kind, ConstraintKind::ForeignKey) {
            continue;
        }
        let parent_key_set = load_parent_key_set(catalog_base, catalog, constraint)?;
        for (_, _, new_values) in updates {
            if let Some(key) = constraint_key_for_values(new_values, constraint)?
                && !parent_key_set.contains(&key)
            {
                return Err(foreign_key_violation_error(constraint));
            }
        }
    }
    Ok(())
}

fn constraint_key_for_values(
    values: &[Value],
    constraint: &ConstraintCheck,
) -> Result<Option<String>> {
    key_for_indexes(
        values,
        &constraint.column_indexes,
        matches!(constraint.info.kind, ConstraintKind::PrimaryKey),
        &constraint.info,
    )
}

fn key_for_indexes(
    values: &[Value],
    indexes: &[usize],
    reject_null: bool,
    info: &ConstraintInfo,
) -> Result<Option<String>> {
    let mut key_values = Vec::with_capacity(indexes.len());
    for index in indexes {
        let value = values.get(*index).ok_or(RuseDbError::Corruption(format!(
            "constraint '{}' references invalid column index {}",
            info.name, index
        )))?;
        if matches!(value, Value::Null) {
            if reject_null {
                let kind = match info.kind {
                    ConstraintKind::PrimaryKey => "PRIMARY KEY",
                    ConstraintKind::Unique => "UNIQUE",
                    ConstraintKind::ForeignKey => "FOREIGN KEY",
                };
                return Err(RuseDbError::Parse(format!(
                    "{} constraint '{}' does not allow NULL",
                    kind, info.name
                )));
            }
            return Ok(None);
        }
        key_values.push(value.clone());
    }
    Ok(Some(group_key(&key_values)))
}

fn duplicate_constraint_error(constraint: &ConstraintCheck) -> RuseDbError {
    let kind = match constraint.info.kind {
        ConstraintKind::PrimaryKey => "PRIMARY KEY",
        ConstraintKind::Unique => "UNIQUE",
        ConstraintKind::ForeignKey => "FOREIGN KEY",
    };
    RuseDbError::Parse(format!(
        "duplicate key violates {} constraint '{}'",
        kind, constraint.info.name
    ))
}

fn default_constraint_name(
    table_name: &str,
    kind: ConstraintKind,
    keys: &[String],
    referenced_table: Option<&str>,
    referenced_columns: &[String],
) -> String {
    let normalized_table = normalize_identifier_for_name(table_name);
    let normalized_keys = keys
        .iter()
        .map(|key| normalize_identifier_for_name(key))
        .collect::<Vec<_>>()
        .join("_");
    match kind {
        ConstraintKind::PrimaryKey => format!("pk_{}", normalized_table),
        ConstraintKind::Unique => format!("uq_{}_{}", normalized_table, normalized_keys),
        ConstraintKind::ForeignKey => {
            let parent = normalize_identifier_for_name(referenced_table.unwrap_or("parent"));
            let parent_cols = referenced_columns
                .iter()
                .map(|key| normalize_identifier_for_name(key))
                .collect::<Vec<_>>()
                .join("_");
            format!(
                "fk_{}_{}_{}_{}",
                normalized_table, normalized_keys, parent, parent_cols
            )
        }
    }
}

fn normalize_identifier_for_name(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unnamed".to_string()
    } else {
        out
    }
}

fn constraint_signature(
    kind: ConstraintKind,
    keys: &[String],
    referenced_table: Option<&str>,
    referenced_columns: &[String],
) -> String {
    let kind_label = match kind {
        ConstraintKind::PrimaryKey => "pk",
        ConstraintKind::Unique => "uq",
        ConstraintKind::ForeignKey => "fk",
    };
    let cols = keys
        .iter()
        .map(|key| key.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(",");
    match kind {
        ConstraintKind::ForeignKey => {
            let parent = referenced_table.unwrap_or_default().to_ascii_lowercase();
            let parent_cols = referenced_columns
                .iter()
                .map(|key| key.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(",");
            format!("{kind_label}:{cols}->{parent}({parent_cols})")
        }
        _ => format!("{kind_label}:{cols}"),
    }
}

fn validate_foreign_key_definitions(
    catalog: &Catalog,
    constraints: &[ConstraintDef],
) -> Result<()> {
    for constraint in constraints {
        if !matches!(constraint.kind, ConstraintKind::ForeignKey) {
            continue;
        }
        let referenced_table =
            constraint
                .referenced_table
                .as_ref()
                .ok_or(RuseDbError::InvalidSchema(format!(
                    "FOREIGN KEY constraint '{}' missing referenced table",
                    constraint.name
                )))?;
        let _ = catalog.get_table(referenced_table)?;
        let parent_schema = catalog.describe_table(referenced_table)?;
        for referenced_column in &constraint.referenced_columns {
            if parent_schema.find_column(referenced_column).is_none() {
                return Err(RuseDbError::NotFound {
                    object: "column".to_string(),
                    name: format!("{}.{}", referenced_table, referenced_column),
                });
            }
        }
        if constraint.key_columns.len() != constraint.referenced_columns.len() {
            return Err(RuseDbError::InvalidSchema(format!(
                "FOREIGN KEY constraint '{}' requires same number of source and referenced columns",
                constraint.name
            )));
        }
        let parent_constraints = catalog.list_constraints(referenced_table)?;
        let has_parent_key = parent_constraints.iter().any(|item| {
            matches!(
                item.kind,
                ConstraintKind::PrimaryKey | ConstraintKind::Unique
            ) && same_identifier_list(&item.key_columns, &constraint.referenced_columns)
        });
        if !has_parent_key {
            return Err(RuseDbError::InvalidSchema(format!(
                "FOREIGN KEY constraint '{}' references '{}({})' without PRIMARY KEY/UNIQUE",
                constraint.name,
                referenced_table,
                constraint.referenced_columns.join(", ")
            )));
        }
    }
    Ok(())
}

fn ensure_drop_table_allowed_by_fk(
    _catalog_base: &Path,
    catalog: &Catalog,
    table_name: &str,
) -> Result<()> {
    let refs = load_referencing_foreign_keys(catalog, table_name)?;
    if let Some((child_table_name, fk)) = refs.first() {
        return Err(RuseDbError::Parse(format!(
            "cannot drop table '{}': referenced by FOREIGN KEY '{}' on table '{}'",
            table_name, fk.info.name, child_table_name
        )));
    }
    Ok(())
}

fn ensure_drop_column_allowed_by_metadata(
    catalog: &Catalog,
    table_name: &str,
    column_name: &str,
) -> Result<()> {
    let normalized_table = table_name.to_ascii_lowercase();
    let normalized_column = column_name.to_ascii_lowercase();

    for index in catalog.list_indexes(table_name)? {
        if index.key_columns.eq_ignore_ascii_case(column_name) {
            return Err(RuseDbError::Parse(format!(
                "cannot drop column '{}.{}': used by index '{}'",
                table_name, column_name, index.name
            )));
        }
    }

    for constraint in catalog.list_constraints(table_name)? {
        if constraint
            .key_columns
            .iter()
            .any(|col| col.eq_ignore_ascii_case(column_name))
        {
            return Err(RuseDbError::Parse(format!(
                "cannot drop column '{}.{}': used by constraint '{}'",
                table_name, column_name, constraint.name
            )));
        }
        if constraint
            .referenced_table
            .as_ref()
            .map(|table| table.to_ascii_lowercase() == normalized_table)
            .unwrap_or(false)
            && constraint
                .referenced_columns
                .iter()
                .any(|col| col.eq_ignore_ascii_case(column_name))
        {
            return Err(RuseDbError::Parse(format!(
                "cannot drop column '{}.{}': referenced by constraint '{}'",
                table_name, column_name, constraint.name
            )));
        }
    }

    for (child_table, fk) in load_referencing_foreign_keys(catalog, table_name)? {
        if fk
            .info
            .referenced_columns
            .iter()
            .any(|col| col.to_ascii_lowercase() == normalized_column)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot drop column '{}.{}': referenced by FOREIGN KEY '{}' on table '{}'",
                table_name, column_name, fk.info.name, child_table
            )));
        }
    }

    Ok(())
}

fn ensure_alter_column_type_allowed_by_metadata(
    catalog: &Catalog,
    table_name: &str,
    normalized_column: &str,
    new_type: DataType,
) -> Result<()> {
    for constraint in catalog.list_constraints(table_name)? {
        if constraint
            .key_columns
            .iter()
            .any(|col| col.to_ascii_lowercase() == normalized_column)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot alter type of '{}.{}': used by constraint '{}'",
                table_name, normalized_column, constraint.name
            )));
        }
        if constraint
            .referenced_table
            .as_ref()
            .map(|table| table.eq_ignore_ascii_case(table_name))
            .unwrap_or(false)
            && constraint
                .referenced_columns
                .iter()
                .any(|col| col.to_ascii_lowercase() == normalized_column)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot alter type of '{}.{}': referenced by constraint '{}'",
                table_name, normalized_column, constraint.name
            )));
        }
    }

    for (child_table, fk) in load_referencing_foreign_keys(catalog, table_name)? {
        if fk
            .info
            .referenced_columns
            .iter()
            .any(|col| col.to_ascii_lowercase() == normalized_column)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot alter type of '{}.{}': referenced by FOREIGN KEY '{}' on table '{}'",
                table_name, normalized_column, fk.info.name, child_table
            )));
        }
    }

    for index in catalog.list_indexes(table_name)? {
        if index.key_columns.to_ascii_lowercase() == normalized_column
            && let Err(err) = IndexKeyKind::from_data_type(new_type)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot alter type of indexed column '{}.{}' to {}: {}",
                table_name,
                normalized_column,
                new_type.as_str(),
                err
            )));
        }
    }

    Ok(())
}

fn ensure_drop_not_null_allowed_by_metadata(
    catalog: &Catalog,
    table_name: &str,
    normalized_column: &str,
) -> Result<()> {
    for constraint in catalog.list_constraints(table_name)? {
        if matches!(constraint.kind, ConstraintKind::PrimaryKey)
            && constraint
                .key_columns
                .iter()
                .any(|col| col.to_ascii_lowercase() == normalized_column)
        {
            return Err(RuseDbError::Parse(format!(
                "cannot drop NOT NULL from '{}.{}': part of PRIMARY KEY '{}'",
                table_name, normalized_column, constraint.name
            )));
        }
    }
    Ok(())
}

fn cast_value_to_type(value: &Value, target: DataType, column_name: &str) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let out = match (value, target) {
        (Value::Int(v), DataType::Int) => Value::Int(*v),
        (Value::Int(v), DataType::BigInt) => Value::BigInt(i64::from(*v)),
        (Value::Int(v), DataType::Double) => Value::Double(*v as f64),
        (Value::BigInt(v), DataType::BigInt) => Value::BigInt(*v),
        (Value::BigInt(v), DataType::Int) => {
            let casted = i32::try_from(*v).map_err(|_| RuseDbError::TypeMismatch {
                column: column_name.to_string(),
                expected: "INT".to_string(),
                actual: "BIGINT(out-of-range)".to_string(),
            })?;
            Value::Int(casted)
        }
        (Value::BigInt(v), DataType::Double) => Value::Double(*v as f64),
        (Value::Double(v), DataType::Double) => Value::Double(*v),
        (Value::Double(v), DataType::BigInt) => {
            if !v.is_finite() || v.fract() != 0.0 {
                return Err(RuseDbError::TypeMismatch {
                    column: column_name.to_string(),
                    expected: "BIGINT".to_string(),
                    actual: "DOUBLE(non-integer)".to_string(),
                });
            }
            if *v < i64::MIN as f64 || *v > i64::MAX as f64 {
                return Err(RuseDbError::TypeMismatch {
                    column: column_name.to_string(),
                    expected: "BIGINT".to_string(),
                    actual: "DOUBLE(out-of-range)".to_string(),
                });
            }
            Value::BigInt(*v as i64)
        }
        (Value::Double(v), DataType::Int) => {
            if !v.is_finite() || v.fract() != 0.0 {
                return Err(RuseDbError::TypeMismatch {
                    column: column_name.to_string(),
                    expected: "INT".to_string(),
                    actual: "DOUBLE(non-integer)".to_string(),
                });
            }
            if *v < i32::MIN as f64 || *v > i32::MAX as f64 {
                return Err(RuseDbError::TypeMismatch {
                    column: column_name.to_string(),
                    expected: "INT".to_string(),
                    actual: "DOUBLE(out-of-range)".to_string(),
                });
            }
            Value::Int(*v as i32)
        }
        (Value::Bool(v), DataType::Bool) => Value::Bool(*v),
        (Value::Varchar(v), DataType::Varchar) => Value::Varchar(v.clone()),
        (_, target_type) => {
            return Err(RuseDbError::TypeMismatch {
                column: column_name.to_string(),
                expected: target_type.as_str().to_string(),
                actual: value.type_name().to_string(),
            });
        }
    };
    Ok(out)
}

fn ensure_parent_delete_restrict(
    catalog_base: &Path,
    catalog: &Catalog,
    parent_table_name: &str,
    _parent_schema: &Schema,
    rows_to_delete: &[(Rid, Row)],
) -> Result<()> {
    if rows_to_delete.is_empty() {
        return Ok(());
    }
    let refs = load_referencing_foreign_keys(catalog, parent_table_name)?;
    if refs.is_empty() {
        return Ok(());
    }

    for (child_table_name, fk) in refs {
        let child_table = catalog.get_table(&child_table_name)?;
        let child_schema = catalog.describe_table(&child_table_name)?;
        let mut child_heap = HeapFile::open(table_heap_path(catalog_base, child_table.table_id))?;
        let mut child_keys = HashSet::new();
        for (_rid, raw) in child_heap.scan_records()? {
            let row = Row::decode(&child_schema, &raw)?;
            if let Some(key) = constraint_key_for_values(&row.values, &fk)? {
                child_keys.insert(key);
            }
        }

        for (_, row) in rows_to_delete {
            if let Some(parent_key) =
                key_for_indexes(&row.values, &fk.referenced_column_indexes, false, &fk.info)?
                && child_keys.contains(&parent_key)
            {
                return Err(RuseDbError::Parse(format!(
                    "cannot delete from '{}': FOREIGN KEY '{}' on table '{}' has dependent rows",
                    parent_table_name, fk.info.name, child_table_name
                )));
            }
        }
    }
    Ok(())
}

fn ensure_parent_update_restrict(
    catalog_base: &Path,
    catalog: &Catalog,
    parent_table_name: &str,
    _parent_schema: &Schema,
    updates: &[(Rid, Row, Vec<Value>)],
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let refs = load_referencing_foreign_keys(catalog, parent_table_name)?;
    if refs.is_empty() {
        return Ok(());
    }

    for (child_table_name, fk) in refs {
        let child_table = catalog.get_table(&child_table_name)?;
        let child_schema = catalog.describe_table(&child_table_name)?;
        let mut child_heap = HeapFile::open(table_heap_path(catalog_base, child_table.table_id))?;
        let mut child_keys = HashSet::new();
        for (_rid, raw) in child_heap.scan_records()? {
            let row = Row::decode(&child_schema, &raw)?;
            if let Some(key) = constraint_key_for_values(&row.values, &fk)? {
                child_keys.insert(key);
            }
        }

        for (_, old_row, new_values) in updates {
            let old_key = key_for_indexes(
                &old_row.values,
                &fk.referenced_column_indexes,
                false,
                &fk.info,
            )?;
            let new_key =
                key_for_indexes(new_values, &fk.referenced_column_indexes, false, &fk.info)?;
            if old_key != new_key
                && let Some(old_key) = old_key
                && child_keys.contains(&old_key)
            {
                return Err(RuseDbError::Parse(format!(
                    "cannot update '{}': FOREIGN KEY '{}' on table '{}' requires RESTRICT",
                    parent_table_name, fk.info.name, child_table_name
                )));
            }
        }
    }
    Ok(())
}

fn load_referencing_foreign_keys(
    catalog: &Catalog,
    parent_table_name: &str,
) -> Result<Vec<(String, ConstraintCheck)>> {
    let parent_normalized = parent_table_name.to_ascii_lowercase();
    let mut table_name_by_id = HashMap::new();
    for table in catalog.list_tables() {
        table_name_by_id.insert(table.table_id, table.name);
    }

    let mut out = Vec::new();
    for info in catalog.list_all_constraints() {
        if !matches!(info.kind, ConstraintKind::ForeignKey) {
            continue;
        }
        let Some(referenced_table) = info.referenced_table.as_ref() else {
            continue;
        };
        if referenced_table.to_ascii_lowercase() != parent_normalized {
            continue;
        }
        let child_table_name =
            table_name_by_id
                .get(&info.table_id)
                .cloned()
                .ok_or(RuseDbError::Corruption(format!(
                    "constraint '{}' references missing child table_id {}",
                    info.name, info.table_id
                )))?;
        let child_schema = catalog.describe_table(&child_table_name)?;
        let check = constraint_check_from_info(catalog, &child_schema, info)?;
        out.push((child_table_name, check));
    }
    Ok(out)
}

fn load_parent_key_set(
    catalog_base: &Path,
    catalog: &Catalog,
    constraint: &ConstraintCheck,
) -> Result<HashSet<String>> {
    let referenced_table =
        constraint
            .info
            .referenced_table
            .as_ref()
            .ok_or(RuseDbError::Corruption(format!(
                "FOREIGN KEY constraint '{}' missing referenced table",
                constraint.info.name
            )))?;
    let parent_table = catalog.get_table(referenced_table)?;
    let parent_schema = catalog.describe_table(referenced_table)?;
    let mut parent_heap = HeapFile::open(table_heap_path(catalog_base, parent_table.table_id))?;
    let mut parent_key_set = HashSet::new();
    for (_rid, raw) in parent_heap.scan_records()? {
        let row = Row::decode(&parent_schema, &raw)?;
        if let Some(key) = key_for_indexes(
            &row.values,
            &constraint.referenced_column_indexes,
            false,
            &constraint.info,
        )? {
            parent_key_set.insert(key);
        }
    }
    Ok(parent_key_set)
}

fn foreign_key_violation_error(constraint: &ConstraintCheck) -> RuseDbError {
    let referenced_table = constraint
        .info
        .referenced_table
        .as_deref()
        .unwrap_or("<unknown>");
    RuseDbError::Parse(format!(
        "foreign key constraint '{}' fails: referenced key not found in '{}'",
        constraint.info.name, referenced_table
    ))
}

fn same_identifier_list(left: &[String], right: &[String]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .all(|(l, r)| l.eq_ignore_ascii_case(r))
}

fn choose_index_candidates(
    selection: Option<&Expr>,
    indexes: &mut [TableIndex],
) -> Result<Option<Vec<Rid>>> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let Some(predicate) = extract_index_predicate(selection)? else {
        return Ok(None);
    };

    let normalized_column = predicate.column.to_ascii_lowercase();
    let Some(table_index) = indexes
        .iter_mut()
        .find(|idx| idx.info.key_columns.to_ascii_lowercase() == normalized_column)
    else {
        return Ok(None);
    };

    let Some(key) = table_index.index.key_from_value(&predicate.value)? else {
        return Ok(Some(Vec::new()));
    };

    let rids = match predicate.op {
        BinaryOp::Eq => table_index.index.search_eq(&key)?,
        BinaryOp::Lt => table_index.index.search_range(None, Some((key, false)))?,
        BinaryOp::Lte => table_index.index.search_range(None, Some((key, true)))?,
        BinaryOp::Gt => table_index.index.search_range(Some((key, false)), None)?,
        BinaryOp::Gte => table_index.index.search_range(Some((key, true)), None)?,
        _ => return Ok(None),
    };
    Ok(Some(rids))
}

fn push_explain_row(rows: &mut Vec<Vec<Value>>, item: &str, value: String) {
    rows.push(vec![
        Value::Varchar(item.to_string()),
        Value::Varchar(value),
    ]);
}

fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

fn render_value_for_plan(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Varchar(v) => format!("'{}'", v.replace('\'', "''")),
        Value::Null => "NULL".to_string(),
    }
}

fn count_indexable_predicates(expr: &Expr) -> usize {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => count_indexable_predicates(left) + count_indexable_predicates(right),
        Expr::Binary { left, op, right } if is_range_or_eq(*op) => {
            if identifier_literal_pair(left, right)
                .ok()
                .flatten()
                .is_some()
                || identifier_literal_pair(right, left)
                    .ok()
                    .flatten()
                    .is_some()
            {
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn estimate_scanned_rows_for_select(catalog_base: &Path, plan: &SelectPlan) -> Result<usize> {
    if !plan.joins.is_empty() {
        let mut total = ensure_table_statistics(catalog_base, &plan.table)?.row_count;
        for join in &plan.joins {
            total =
                total.saturating_add(ensure_table_statistics(catalog_base, &join.table)?.row_count);
        }
        return Ok(total);
    }

    let table_stats = ensure_table_statistics(catalog_base, &plan.table)?;
    let catalog = Catalog::open(catalog_base)?;
    let schema = catalog.describe_table(&plan.table)?;
    let mut indexes = load_table_indexes(&catalog, &plan.table, &schema, catalog_base)?;
    let candidate_rids = choose_index_candidates(plan.selection.as_ref(), &mut indexes)?;
    Ok(candidate_rids
        .as_ref()
        .map(|rids| rids.len())
        .unwrap_or(table_stats.row_count))
}

fn extract_index_predicate(expr: &Expr) -> Result<Option<IndexPredicate>> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            if let Some(p) = extract_index_predicate(left)? {
                return Ok(Some(p));
            }
            extract_index_predicate(right)
        }
        Expr::Binary { left, op, right } if is_range_or_eq(*op) => {
            if let Some((column, value)) = identifier_literal_pair(left, right)? {
                return Ok(Some(IndexPredicate {
                    column,
                    op: *op,
                    value,
                }));
            }
            if let Some((column, value)) = identifier_literal_pair(right, left)? {
                return Ok(Some(IndexPredicate {
                    column,
                    op: invert_comparison(*op),
                    value,
                }));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn identifier_literal_pair(
    column_expr: &Expr,
    value_expr: &Expr,
) -> Result<Option<(String, Value)>> {
    let Expr::Identifier(column) = column_expr else {
        return Ok(None);
    };
    if expr_contains_subquery(value_expr) {
        return Ok(None);
    }
    let value = eval_constant_expr(value_expr)?;
    Ok(Some((column.clone(), value)))
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::Literal(_) | Expr::Aggregate { .. } => false,
        Expr::Unary { expr, .. } => expr_contains_subquery(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
        Expr::InSubquery { .. } | Expr::ScalarSubquery { .. } => true,
        Expr::Like { expr, pattern, .. } => {
            expr_contains_subquery(expr) || expr_contains_subquery(pattern)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        Expr::IsNull { expr, .. } => expr_contains_subquery(expr),
    }
}

fn is_range_or_eq(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    )
}

fn invert_comparison(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Lte => BinaryOp::Gte,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Gte => BinaryOp::Lte,
        other => other,
    }
}

fn collect_rows(
    heap: &mut HeapFile,
    schema: &Schema,
    candidate_rids: Option<Vec<Rid>>,
) -> Result<Vec<(Rid, Row)>> {
    match candidate_rids {
        Some(rids) => {
            let mut out = Vec::new();
            let mut dedup = HashSet::new();
            for rid in rids {
                if !dedup.insert(rid) {
                    continue;
                }
                let Ok(raw) = heap.get_record(rid) else {
                    continue;
                };
                let row = Row::decode(schema, &raw)?;
                out.push((rid, row));
            }
            Ok(out)
        }
        None => {
            let mut out = Vec::new();
            for (rid, raw) in heap.scan_records()? {
                let row = Row::decode(schema, &raw)?;
                out.push((rid, row));
            }
            Ok(out)
        }
    }
}

fn build_insert_values(
    schema: &Schema,
    column_names: Option<&[String]>,
    exprs: &[Expr],
) -> Result<Vec<Value>> {
    match column_names {
        Some(names) => {
            if names.len() != exprs.len() {
                return Err(RuseDbError::Parse(format!(
                    "insert value count {} does not match target column count {}",
                    exprs.len(),
                    names.len()
                )));
            }

            let mut values = vec![Value::Null; schema.columns.len()];
            for (name, expr) in names.iter().zip(exprs) {
                let (index, _) = schema.find_column(name).ok_or(RuseDbError::NotFound {
                    object: "column".to_string(),
                    name: name.clone(),
                })?;
                values[index] = eval_constant_expr(expr)?;
            }
            Ok(values)
        }
        None => {
            if exprs.len() != schema.columns.len() {
                return Err(RuseDbError::Parse(format!(
                    "insert value count {} does not match table column count {}",
                    exprs.len(),
                    schema.columns.len()
                )));
            }
            exprs.iter().map(eval_constant_expr).collect()
        }
    }
}

fn resolve_assignments(
    schema: &Schema,
    assignments: Vec<rusedb_sql::Assignment>,
) -> Result<Vec<(usize, Expr)>> {
    let mut out = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let (index, _) = schema
            .find_column(&assignment.column)
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: assignment.column,
            })?;
        out.push((index, assignment.value));
    }
    Ok(out)
}

fn normalize_values_for_schema(schema: &Schema, values: &mut [Value]) -> Result<()> {
    if values.len() != schema.columns.len() {
        return Err(RuseDbError::InvalidSchema(format!(
            "value count {} does not match schema columns {}",
            values.len(),
            schema.columns.len()
        )));
    }

    for (value, column) in values.iter_mut().zip(&schema.columns) {
        if matches!(value, Value::Null) {
            continue;
        }

        match column.data_type {
            DataType::Int => match value {
                Value::Int(_) => {}
                Value::BigInt(v) => {
                    let casted = i32::try_from(*v).map_err(|_| RuseDbError::TypeMismatch {
                        column: column.name.clone(),
                        expected: "INT".to_string(),
                        actual: "BIGINT(out-of-range)".to_string(),
                    })?;
                    *value = Value::Int(casted);
                }
                _ => {}
            },
            DataType::BigInt => {
                if let Value::Int(v) = value {
                    *value = Value::BigInt(i64::from(*v));
                }
            }
            DataType::Double => match value {
                Value::Int(v) => *value = Value::Double(*v as f64),
                Value::BigInt(v) => *value = Value::Double(*v as f64),
                _ => {}
            },
            DataType::Bool | DataType::Varchar => {}
        }
    }
    Ok(())
}

fn resolve_projection(
    schema: &Schema,
    projection: &[SelectItem],
) -> Result<(Vec<usize>, Vec<String>)> {
    if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard) {
        let indexes: Vec<usize> = (0..schema.columns.len()).collect();
        let names = schema.columns.iter().map(|c| c.name.clone()).collect();
        return Ok((indexes, names));
    }

    let mut indexes = Vec::new();
    let mut names = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                return Err(RuseDbError::Parse(
                    "wildcard cannot be mixed with explicit projection columns".to_string(),
                ));
            }
            SelectItem::Column(name) => {
                let (index, _) = schema.find_column(name).ok_or(RuseDbError::NotFound {
                    object: "column".to_string(),
                    name: name.clone(),
                })?;
                indexes.push(index);
                names.push(name.clone());
            }
            SelectItem::Aggregate { .. } => {
                return Err(RuseDbError::Parse(
                    "aggregate projection is only supported in grouped/joined select path"
                        .to_string(),
                ));
            }
        }
    }
    Ok((indexes, names))
}

fn resolve_order_by(schema: &Schema, order_by: &[OrderByItem]) -> Result<Vec<(usize, bool)>> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        let (index, _) = schema
            .find_column(&item.column)
            .ok_or(RuseDbError::NotFound {
                object: "column".to_string(),
                name: item.column.clone(),
            })?;
        out.push((index, item.descending));
    }
    Ok(out)
}

fn matches_selection_with_subquery<F>(
    schema: &Schema,
    row: &Row,
    selection: Option<&Expr>,
    resolve_subquery: &mut F,
) -> Result<bool>
where
    F: FnMut(&Statement) -> Result<Vec<Value>>,
{
    let Some(expr) = selection else {
        return Ok(true);
    };
    match eval_expr_with_subquery(expr, schema, row, resolve_subquery)? {
        Value::Bool(v) => Ok(v),
        Value::Null => Ok(false),
        other => Err(RuseDbError::Parse(format!(
            "WHERE expression must return BOOL, got {}",
            other.type_name()
        ))),
    }
}

fn eval_constant_expr(expr: &Expr) -> Result<Value> {
    let mut resolve_identifier = |name: &str| {
        Err(RuseDbError::Parse(format!(
            "column reference '{}' is not allowed in constant expression",
            name
        )))
    };
    let mut resolve_aggregate = |_: AggregateFunction, _: Option<&str>| {
        Err(RuseDbError::Parse(
            "aggregate function is not allowed in constant expression".to_string(),
        ))
    };
    let mut resolve_subquery = |_: &Statement| {
        Err(RuseDbError::Parse(
            "subquery is not allowed in constant expression".to_string(),
        ))
    };
    eval_expr_with(
        expr,
        &mut resolve_identifier,
        &mut resolve_aggregate,
        &mut resolve_subquery,
    )
}

fn eval_expr(expr: &Expr, schema: &Schema, row: &Row) -> Result<Value> {
    eval_expr_with_subquery(expr, schema, row, &mut |_| {
        Err(RuseDbError::Parse(
            "subquery is not available in this context".to_string(),
        ))
    })
}

fn eval_expr_with_subquery<F>(
    expr: &Expr,
    schema: &Schema,
    row: &Row,
    resolve_subquery: &mut F,
) -> Result<Value>
where
    F: FnMut(&Statement) -> Result<Vec<Value>>,
{
    let mut resolve_identifier = |name: &str| {
        let (index, _) = schema.find_column(name).ok_or(RuseDbError::NotFound {
            object: "column".to_string(),
            name: name.to_string(),
        })?;
        Ok(row.values[index].clone())
    };
    let mut resolve_aggregate = |_: AggregateFunction, _: Option<&str>| {
        Err(RuseDbError::Parse(
            "aggregate function is only allowed in grouped/HAVING context".to_string(),
        ))
    };
    eval_expr_with(
        expr,
        &mut resolve_identifier,
        &mut resolve_aggregate,
        resolve_subquery,
    )
}

fn eval_binary(op: &BinaryOp, left: Value, right: Value) -> Result<Value> {
    use BinaryOp::*;
    match op {
        And | Or => {
            let l = to_bool(&left)?;
            let r = to_bool(&right)?;
            let out = match op {
                And => l && r,
                Or => l || r,
                _ => unreachable!(),
            };
            Ok(Value::Bool(out))
        }
        Eq | NotEq | Lt | Lte | Gt | Gte => {
            let result = compare_values(&left, &right)?;
            let is_true = match op {
                Eq => result == Some(Ordering::Equal),
                NotEq => result != Some(Ordering::Equal),
                Lt => result == Some(Ordering::Less),
                Lte => matches!(result, Some(Ordering::Less | Ordering::Equal)),
                Gt => result == Some(Ordering::Greater),
                Gte => matches!(result, Some(Ordering::Greater | Ordering::Equal)),
                _ => unreachable!(),
            };
            Ok(Value::Bool(is_true))
        }
    }
}

fn eval_expr_with(
    expr: &Expr,
    resolve_identifier: &mut dyn FnMut(&str) -> Result<Value>,
    resolve_aggregate: &mut dyn FnMut(AggregateFunction, Option<&str>) -> Result<Value>,
    resolve_subquery: &mut dyn FnMut(&Statement) -> Result<Vec<Value>>,
) -> Result<Value> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(name),
        Expr::Aggregate { func, column } => resolve_aggregate(*func, column.as_deref()),
        Expr::Literal(literal) => Ok(literal_to_value(literal)),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => match eval_expr_with(
            expr,
            resolve_identifier,
            resolve_aggregate,
            resolve_subquery,
        )? {
            Value::Bool(v) => Ok(Value::Bool(!v)),
            Value::Null => Ok(Value::Null),
            other => Err(RuseDbError::Parse(format!(
                "NOT expects BOOL, got {}",
                other.type_name()
            ))),
        },
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match eval_expr_with(
            expr,
            resolve_identifier,
            resolve_aggregate,
            resolve_subquery,
        )? {
            Value::Int(v) => Ok(Value::Int(-v)),
            Value::BigInt(v) => Ok(Value::BigInt(-v)),
            Value::Double(v) => Ok(Value::Double(-v)),
            Value::Null => Ok(Value::Null),
            other => Err(RuseDbError::Parse(format!(
                "cannot negate value of type {}",
                other.type_name()
            ))),
        },
        Expr::Binary { left, op, right } => {
            let left = eval_expr_with(
                left,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            let right = eval_expr_with(
                right,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            eval_binary(op, left, right)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = eval_expr_with(
                expr,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            eval_in_list_with_values(
                target,
                list,
                *negated,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let target = eval_expr_with(
                expr,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            let values = resolve_subquery(subquery)?;
            eval_in_values(target, &values, *negated)
        }
        Expr::ScalarSubquery { subquery } => {
            let values = resolve_subquery(subquery)?;
            match values.len() {
                0 => Ok(Value::Null),
                1 => Ok(values[0].clone()),
                count => Err(RuseDbError::Parse(format!(
                    "scalar subquery must return at most one row, got {count}",
                ))),
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let value = eval_expr_with(
                expr,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            let pattern = eval_expr_with(
                pattern,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            eval_like_with_values(value, pattern, *negated)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr_with(
                expr,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            let low = eval_expr_with(low, resolve_identifier, resolve_aggregate, resolve_subquery)?;
            let high = eval_expr_with(
                high,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            eval_between_with_values(value, low, high, *negated)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval_expr_with(
                expr,
                resolve_identifier,
                resolve_aggregate,
                resolve_subquery,
            )?;
            let mut out = matches!(value, Value::Null);
            if *negated {
                out = !out;
            }
            Ok(Value::Bool(out))
        }
    }
}

fn eval_in_list_with_values(
    target: Value,
    list: &[Expr],
    negated: bool,
    resolve_identifier: &mut dyn FnMut(&str) -> Result<Value>,
    resolve_aggregate: &mut dyn FnMut(AggregateFunction, Option<&str>) -> Result<Value>,
    resolve_subquery: &mut dyn FnMut(&Statement) -> Result<Vec<Value>>,
) -> Result<Value> {
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        values.push(eval_expr_with(
            item,
            resolve_identifier,
            resolve_aggregate,
            resolve_subquery,
        )?);
    }
    eval_in_values(target, &values, negated)
}

fn eval_in_values(target: Value, values: &[Value], negated: bool) -> Result<Value> {
    if matches!(target, Value::Null) {
        return Ok(Value::Null);
    }

    let mut has_null = false;
    for candidate in values {
        if matches!(candidate, Value::Null) {
            has_null = true;
            continue;
        }
        if compare_values(&target, candidate)? == Some(Ordering::Equal) {
            return Ok(Value::Bool(!negated));
        }
    }

    if has_null {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(negated))
    }
}

fn eval_like_with_values(value: Value, pattern: Value, negated: bool) -> Result<Value> {
    if matches!(value, Value::Null) || matches!(pattern, Value::Null) {
        return Ok(Value::Null);
    }

    let Value::Varchar(input) = value else {
        return Err(RuseDbError::TypeMismatch {
            column: "LIKE-left".to_string(),
            expected: "VARCHAR".to_string(),
            actual: value.type_name().to_string(),
        });
    };
    let Value::Varchar(pattern_text) = pattern else {
        return Err(RuseDbError::TypeMismatch {
            column: "LIKE-pattern".to_string(),
            expected: "VARCHAR".to_string(),
            actual: pattern.type_name().to_string(),
        });
    };
    let mut out = like_matches(&input, &pattern_text);
    if negated {
        out = !out;
    }
    Ok(Value::Bool(out))
}

fn eval_between_with_values(value: Value, low: Value, high: Value, negated: bool) -> Result<Value> {
    if matches!(value, Value::Null) || matches!(low, Value::Null) || matches!(high, Value::Null) {
        return Ok(Value::Null);
    }

    let ge_low = matches!(
        compare_values(&value, &low)?,
        Some(Ordering::Greater | Ordering::Equal)
    );
    let le_high = matches!(
        compare_values(&value, &high)?,
        Some(Ordering::Less | Ordering::Equal)
    );
    let mut out = ge_low && le_high;
    if negated {
        out = !out;
    }
    Ok(Value::Bool(out))
}

fn like_matches(input: &str, pattern: &str) -> bool {
    let s: Vec<char> = input.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; p.len() + 1]; s.len() + 1];
    dp[0][0] = true;

    for j in 1..=p.len() {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=s.len() {
        for j in 1..=p.len() {
            match p[j - 1] {
                '%' => {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                '_' => {
                    dp[i][j] = dp[i - 1][j - 1];
                }
                ch => {
                    dp[i][j] = dp[i - 1][j - 1] && s[i - 1] == ch;
                }
            }
        }
    }
    dp[s.len()][p.len()]
}

fn literal_to_value(literal: &Literal) -> Value {
    match literal {
        Literal::Integer(v) => Value::BigInt(*v),
        Literal::String(v) => Value::Varchar(v.clone()),
        Literal::Bool(v) => Value::Bool(*v),
        Literal::Null => Value::Null,
    }
}

fn to_bool(value: &Value) -> Result<bool> {
    match value {
        Value::Bool(v) => Ok(*v),
        Value::Null => Ok(false),
        other => Err(RuseDbError::Parse(format!(
            "expected BOOL expression, got {}",
            other.type_name()
        ))),
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<Option<Ordering>> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(None);
    }

    if let (Some(l), Some(r)) = (as_f64(left), as_f64(right)) {
        return Ok(l.partial_cmp(&r));
    }

    match (left, right) {
        (Value::Bool(l), Value::Bool(r)) => Ok(Some(l.cmp(r))),
        (Value::Varchar(l), Value::Varchar(r)) => Ok(Some(l.cmp(r))),
        _ => Err(RuseDbError::Parse(format!(
            "cannot compare {} with {}",
            left.type_name(),
            right.type_name()
        ))),
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int(v) => Some(*v as f64),
        Value::BigInt(v) => Some(*v as f64),
        Value::Double(v) => Some(*v),
        _ => None,
    }
}

fn compare_rows_by_order(left: &Row, right: &Row, order_by: &[(usize, bool)]) -> Ordering {
    for (column_index, descending) in order_by {
        let ord =
            compare_values_for_order(&left.values[*column_index], &right.values[*column_index]);
        if ord != Ordering::Equal {
            return if *descending { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn compare_values_for_order(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        (Value::BigInt(l), Value::BigInt(r)) => l.cmp(r),
        (Value::Double(l), Value::Double(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::Varchar(l), Value::Varchar(r)) => l.cmp(r),
        (l, r) if as_f64(l).is_some() && as_f64(r).is_some() => as_f64(l)
            .zip(as_f64(r))
            .and_then(|(lv, rv)| lv.partial_cmp(&rv))
            .unwrap_or(Ordering::Equal),
        (l, r) => value_type_order(l).cmp(&value_type_order(r)),
    }
}

fn value_type_order(value: &Value) -> u8 {
    match value {
        Value::Int(_) | Value::BigInt(_) | Value::Double(_) => 0,
        Value::Bool(_) => 1,
        Value::Varchar(_) => 2,
        Value::Null => 3,
    }
}

fn normalize_identifier(name: &str) -> String {
    name.to_ascii_lowercase()
}

impl RowContext {
    fn from_table_row(table_name: &str, schema: &Schema, row: &Row) -> Self {
        let mut ctx = Self {
            values: HashMap::new(),
            ambiguous: HashSet::new(),
            wildcard_columns: Vec::new(),
        };
        for (column, value) in schema.columns.iter().zip(&row.values) {
            let qualified = format!("{table_name}.{}", column.name);
            let qualified_key = normalize_identifier(&qualified);
            ctx.values.insert(qualified_key, value.clone());
            ctx.wildcard_columns.push(qualified);
            ctx.insert_unqualified(&column.name, value.clone());
        }
        ctx
    }

    fn null_for_table(table_name: &str, schema: &Schema) -> Self {
        let mut ctx = Self {
            values: HashMap::new(),
            ambiguous: HashSet::new(),
            wildcard_columns: Vec::new(),
        };
        for column in &schema.columns {
            let qualified = format!("{table_name}.{}", column.name);
            let qualified_key = normalize_identifier(&qualified);
            ctx.values.insert(qualified_key, Value::Null);
            ctx.wildcard_columns.push(qualified);
            ctx.insert_unqualified(&column.name, Value::Null);
        }
        ctx
    }

    fn merge(&self, other: &RowContext) -> Self {
        let mut out = self.clone();
        for label in &other.wildcard_columns {
            let key = normalize_identifier(label);
            if let Some(value) = other.values.get(&key) {
                out.values.insert(key, value.clone());
            }
            out.wildcard_columns.push(label.clone());
        }

        for (key, value) in &other.values {
            if key.contains('.') {
                continue;
            }
            if out.ambiguous.contains(key) {
                continue;
            }
            if out.values.contains_key(key) {
                out.values.remove(key);
                out.ambiguous.insert(key.clone());
            } else {
                out.values.insert(key.clone(), value.clone());
            }
        }

        for ambiguous_key in &other.ambiguous {
            out.ambiguous.insert(ambiguous_key.clone());
            out.values.remove(ambiguous_key);
        }
        out
    }

    fn get(&self, identifier: &str) -> Result<Value> {
        let key = normalize_identifier(identifier);
        if self.ambiguous.contains(&key) {
            return Err(RuseDbError::Parse(format!(
                "ambiguous column reference '{}'",
                identifier
            )));
        }
        self.values.get(&key).cloned().ok_or(RuseDbError::NotFound {
            object: "column".to_string(),
            name: identifier.to_string(),
        })
    }

    fn insert_unqualified(&mut self, column_name: &str, value: Value) {
        use std::collections::hash_map::Entry;

        let key = normalize_identifier(column_name);
        if self.ambiguous.contains(&key) {
            return;
        }
        match self.values.entry(key) {
            Entry::Occupied(entry) => {
                let ambiguous_key = entry.key().clone();
                entry.remove();
                self.ambiguous.insert(ambiguous_key);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
    }
}

fn load_table_context_rows(catalog_base: &Path, table_name: &str) -> Result<Vec<RowContext>> {
    let catalog = Catalog::open(catalog_base)?;
    let table = find_table(&catalog, table_name)?;
    let schema = catalog.describe_table(table_name)?;
    let mut heap = HeapFile::open(table_heap_path(catalog_base, table.table_id))?;
    let mut out = Vec::new();
    for (_, raw) in heap.scan_records()? {
        let row = Row::decode(&schema, &raw)?;
        out.push(RowContext::from_table_row(table_name, &schema, &row));
    }
    Ok(out)
}

fn null_table_context_row(catalog_base: &Path, table_name: &str) -> Result<RowContext> {
    let catalog = Catalog::open(catalog_base)?;
    let schema = catalog.describe_table(table_name)?;
    Ok(RowContext::null_for_table(table_name, &schema))
}

fn matches_selection_ctx_with_subquery<F>(
    row: &RowContext,
    selection: Option<&Expr>,
    resolve_subquery: &mut F,
) -> Result<bool>
where
    F: FnMut(&Statement) -> Result<Vec<Value>>,
{
    let Some(expr) = selection else {
        return Ok(true);
    };
    match eval_expr_ctx_with_subquery(expr, row, resolve_subquery)? {
        Value::Bool(v) => Ok(v),
        Value::Null => Ok(false),
        other => Err(RuseDbError::Parse(format!(
            "WHERE/ON expression must return BOOL, got {}",
            other.type_name()
        ))),
    }
}

fn eval_expr_ctx_with_subquery<F>(
    expr: &Expr,
    row: &RowContext,
    resolve_subquery: &mut F,
) -> Result<Value>
where
    F: FnMut(&Statement) -> Result<Vec<Value>>,
{
    let mut resolve_identifier = |name: &str| row.get(name);
    let mut resolve_aggregate = |_: AggregateFunction, _: Option<&str>| {
        Err(RuseDbError::Parse(
            "aggregate function is only allowed in grouped/HAVING context".to_string(),
        ))
    };
    eval_expr_with(
        expr,
        &mut resolve_identifier,
        &mut resolve_aggregate,
        resolve_subquery,
    )
}

fn compare_ctx_by_order(
    left: &RowContext,
    right: &RowContext,
    order_by: &[OrderByItem],
) -> Ordering {
    for item in order_by {
        let left_val = left.get(&item.column).ok();
        let right_val = right.get(&item.column).ok();
        let ord = match (left_val, right_val) {
            (Some(lv), Some(rv)) => compare_values_for_order(&lv, &rv),
            _ => Ordering::Equal,
        };
        if ord != Ordering::Equal {
            return if item.descending { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn validate_order_by_ctx(rows: &[RowContext], order_by: &[OrderByItem]) -> Result<()> {
    let Some(sample) = rows.first() else {
        return Ok(());
    };
    for item in order_by {
        let _ = sample.get(&item.column)?;
    }
    Ok(())
}

fn resolve_output_order_by(
    columns: &[String],
    order_by: &[OrderByItem],
) -> Result<Vec<(usize, bool)>> {
    let mut out = Vec::with_capacity(order_by.len());
    for item in order_by {
        let target = normalize_identifier(&item.column);
        let idx = columns
            .iter()
            .position(|column| normalize_identifier(column) == target)
            .ok_or(RuseDbError::NotFound {
                object: "order-by-column".to_string(),
                name: item.column.clone(),
            })?;
        out.push((idx, item.descending));
    }
    Ok(out)
}

fn compare_output_rows(left: &[Value], right: &[Value], order_by: &[(usize, bool)]) -> Ordering {
    for (index, descending) in order_by {
        if *index >= left.len() || *index >= right.len() {
            continue;
        }
        let ord = compare_values_for_order(&left[*index], &right[*index]);
        if ord != Ordering::Equal {
            return if *descending { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn eval_group_expr(
    expr: &Expr,
    group_rows: &[RowContext],
    normalized_group_by: &HashSet<String>,
    resolve_subquery: &mut dyn FnMut(&Statement) -> Result<Vec<Value>>,
) -> Result<Value> {
    let mut resolve_identifier = |name: &str| {
        let normalized = normalize_identifier(name);
        if !normalized_group_by.is_empty() && !normalized_group_by.contains(&normalized) {
            return Err(RuseDbError::Parse(format!(
                "column '{}' must appear in GROUP BY",
                name
            )));
        }
        let first_row = group_rows.first().ok_or(RuseDbError::Parse(
            "group row missing for grouped expression".to_string(),
        ))?;
        first_row.get(name)
    };
    let mut resolve_aggregate = |func: AggregateFunction, column: Option<&str>| {
        evaluate_aggregate(func, column, group_rows)
    };
    eval_expr_with(
        expr,
        &mut resolve_identifier,
        &mut resolve_aggregate,
        resolve_subquery,
    )
}

fn projection_labels(projection: &[SelectItem]) -> Result<Vec<String>> {
    let mut labels = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::Wildcard => {
                return Err(RuseDbError::Parse(
                    "wildcard is not allowed in aggregate/grouped select".to_string(),
                ));
            }
            SelectItem::Column(name) => labels.push(name.clone()),
            SelectItem::Aggregate { func, column } => {
                labels.push(aggregate_label(*func, column.as_deref()))
            }
        }
    }
    Ok(labels)
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Identifier(_) | Expr::Literal(_) => false,
        Expr::Unary { expr, .. } => expr_has_aggregate(expr),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::InList { expr, list, .. } => {
            expr_has_aggregate(expr) || list.iter().any(expr_has_aggregate)
        }
        Expr::InSubquery { expr, .. } => expr_has_aggregate(expr),
        Expr::ScalarSubquery { .. } => false,
        Expr::Like { expr, pattern, .. } => expr_has_aggregate(expr) || expr_has_aggregate(pattern),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high),
        Expr::IsNull { expr, .. } => expr_has_aggregate(expr),
    }
}

fn project_rows(
    rows: &[RowContext],
    projection: &[SelectItem],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard) {
        let columns = rows
            .first()
            .map(|row| row.wildcard_columns.clone())
            .unwrap_or_default();
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out_row = Vec::with_capacity(columns.len());
            for column in &columns {
                out_row.push(row.get(column)?);
            }
            output.push(out_row);
        }
        return Ok((columns, output));
    }

    let mut columns = Vec::with_capacity(projection.len());
    let mut output = Vec::with_capacity(rows.len());
    for item in projection {
        match item {
            SelectItem::Column(name) => columns.push(name.clone()),
            SelectItem::Wildcard => {
                return Err(RuseDbError::Parse(
                    "wildcard cannot be mixed with explicit projection columns".to_string(),
                ));
            }
            SelectItem::Aggregate { .. } => {
                return Err(RuseDbError::Parse(
                    "aggregate projection requires grouped select path".to_string(),
                ));
            }
        }
    }

    for row in rows {
        let mut out_row = Vec::with_capacity(columns.len());
        for column in &columns {
            out_row.push(row.get(column)?);
        }
        output.push(out_row);
    }
    Ok((columns, output))
}

fn project_aggregate_rows(
    rows: &[RowContext],
    projection: &[SelectItem],
    group_by: &[String],
    having: Option<&Expr>,
    has_aggregate: bool,
    resolve_subquery: &mut dyn FnMut(&Statement) -> Result<Vec<Value>>,
) -> Result<Vec<Vec<Value>>> {
    if projection.is_empty() {
        return Ok(Vec::new());
    }
    if projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard))
    {
        return Err(RuseDbError::Parse(
            "wildcard cannot be used with aggregate/grouped select".to_string(),
        ));
    }

    let normalized_group_by: HashSet<String> = group_by
        .iter()
        .map(|column| normalize_identifier(column))
        .collect();

    let mut groups: BTreeMap<String, Vec<RowContext>> = BTreeMap::new();
    if group_by.is_empty() {
        groups.insert("__all__".to_string(), rows.to_vec());
    } else {
        for row in rows {
            let mut group_values = Vec::with_capacity(group_by.len());
            for column in group_by {
                group_values.push(row.get(column)?);
            }
            groups
                .entry(group_key(&group_values))
                .or_default()
                .push(row.clone());
        }
    }
    if groups.is_empty() {
        if has_aggregate && group_by.is_empty() {
            groups.insert("__all__".to_string(), Vec::new());
        } else {
            return Ok(Vec::new());
        }
    }

    let mut output = Vec::new();
    for group_rows in groups.into_values() {
        if let Some(having_expr) = having {
            match eval_group_expr(
                having_expr,
                &group_rows,
                &normalized_group_by,
                resolve_subquery,
            )? {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => continue,
                other => {
                    return Err(RuseDbError::Parse(format!(
                        "HAVING expression must return BOOL, got {}",
                        other.type_name()
                    )));
                }
            }
        }

        let mut out_row = Vec::with_capacity(projection.len());
        for item in projection {
            match item {
                SelectItem::Column(name) => {
                    let normalized = normalize_identifier(name);
                    if !normalized_group_by.contains(&normalized) {
                        return Err(RuseDbError::Parse(format!(
                            "column '{}' must appear in GROUP BY",
                            name
                        )));
                    }
                    let first_row = group_rows.first().ok_or(RuseDbError::Parse(
                        "group row missing for grouped projection".to_string(),
                    ))?;
                    out_row.push(first_row.get(name)?);
                }
                SelectItem::Aggregate { func, column } => {
                    out_row.push(evaluate_aggregate(*func, column.as_deref(), &group_rows)?);
                }
                SelectItem::Wildcard => unreachable!(),
            }
        }
        output.push(out_row);
    }
    Ok(output)
}

fn aggregate_label(func: AggregateFunction, column: Option<&str>) -> String {
    let name = match func {
        AggregateFunction::Count => "count",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    };
    match column {
        Some(column) => format!("{name}({column})"),
        None => format!("{name}(*)"),
    }
}

fn evaluate_aggregate(
    func: AggregateFunction,
    column: Option<&str>,
    rows: &[RowContext],
) -> Result<Value> {
    match func {
        AggregateFunction::Count => match column {
            Some(column_name) => {
                let mut count = 0i64;
                for row in rows {
                    if !matches!(row.get(column_name)?, Value::Null) {
                        count += 1;
                    }
                }
                Ok(Value::BigInt(count))
            }
            None => Ok(Value::BigInt(rows.len() as i64)),
        },
        AggregateFunction::Sum => {
            let column_name = column.ok_or(RuseDbError::Parse(
                "SUM requires a target column".to_string(),
            ))?;
            let mut sum = 0f64;
            let mut seen = false;
            for row in rows {
                let value = row.get(column_name)?;
                if matches!(value, Value::Null) {
                    continue;
                }
                let numeric = as_f64(&value).ok_or(RuseDbError::Parse(format!(
                    "SUM expects numeric column, got {}",
                    value.type_name()
                )))?;
                sum += numeric;
                seen = true;
            }
            if seen {
                Ok(Value::Double(sum))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let column_name = column.ok_or(RuseDbError::Parse(format!(
                "{} requires a target column",
                aggregate_label(func, None)
            )))?;
            let mut best: Option<Value> = None;
            for row in rows {
                let value = row.get(column_name)?;
                if matches!(value, Value::Null) {
                    continue;
                }
                match &best {
                    None => best = Some(value),
                    Some(current) => {
                        let ord = compare_values_for_order(&value, current);
                        if (matches!(func, AggregateFunction::Min) && ord == Ordering::Less)
                            || (matches!(func, AggregateFunction::Max) && ord == Ordering::Greater)
                        {
                            best = Some(value);
                        }
                    }
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
    }
}

fn group_key(values: &[Value]) -> String {
    let mut out = String::new();
    for value in values {
        let segment = match value {
            Value::Int(v) => format!("i:{v};"),
            Value::BigInt(v) => format!("I:{v};"),
            Value::Bool(v) => format!("b:{v};"),
            Value::Double(v) => format!("d:{v};"),
            Value::Varchar(v) => format!("s:{}:{};", v.len(), v),
            Value::Null => "n:;".to_string(),
        };
        out.push_str(&segment);
    }
    out
}

#[cfg(test)]
mod tests {
    use rusedb_core::Value;

    use super::{Engine, Executor, QueryResult};

    #[test]
    fn engine_update_path_works() {
        let base = std::env::temp_dir().join(format!("rusedb-exec-test-{}", std::process::id()));
        let _ = std::fs::remove_file(base.with_extension("tables"));
        let _ = std::fs::remove_file(base.with_extension("columns"));
        let _ = std::fs::remove_file(base.with_extension("indexes"));
        let _ = std::fs::remove_file(base.with_extension("table-1.heap"));
        let _ = std::fs::remove_file(base.with_extension("index-1.idx"));
        let engine = Engine::new(base);
        engine
            .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
            .unwrap();
        engine
            .execute_sql("INSERT INTO t (id, name) VALUES (1, 'x')")
            .unwrap();
        let result = engine
            .execute_sql("UPDATE t SET name = 'y' WHERE id = 1")
            .unwrap();
        assert_eq!(result, QueryResult::AffectedRows(1));

        let selected = engine.execute_sql("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(
            selected,
            QueryResult::Rows {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![vec![Value::BigInt(1), Value::Varchar("y".to_string())]]
            }
        );
    }

    #[test]
    fn parse_and_execute_create_table_and_index() {
        let dir = std::env::temp_dir().join(format!("rusedb-exec-test2-{}", std::process::id()));
        let _ = std::fs::remove_file(dir.with_extension("tables"));
        let _ = std::fs::remove_file(dir.with_extension("columns"));
        let _ = std::fs::remove_file(dir.with_extension("indexes"));
        let _ = std::fs::remove_file(dir.with_extension("table-1.heap"));
        let _ = std::fs::remove_file(dir.with_extension("index-1.idx"));
        let engine = Engine::new(&dir);
        let result = engine
            .execute_sql("CREATE TABLE t (id BIGINT NOT NULL, name VARCHAR)")
            .unwrap();
        assert!(matches!(result, QueryResult::Message(_)));

        let result = engine
            .execute_sql("CREATE INDEX idx_t_id ON t (id)")
            .unwrap();
        assert!(matches!(result, QueryResult::Message(_)));
    }
}
