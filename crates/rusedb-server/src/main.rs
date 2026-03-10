use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::hash::Hasher;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use rusedb_core::{Column, DataType, Result, RuseDbError, Value};
use rusedb_exec::{Engine, Executor, QueryResult};
use rusedb_sql::{Expr, JoinClause, Statement, parse_sql};
use rusedb_storage::Catalog;
use serde::{Deserialize, Serialize};

const DEFAULT_INSTANCE_DIR: &str = "rusedb-instance";
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 15432;
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:18080";
const DEFAULT_ADMIN: &str = "admin";

#[derive(Debug, Clone)]
struct ServerConfig {
    host: String,
    port: u16,
    catalog_base: String,
    admin_user: String,
    password_salt: String,
    password_hash: String,
}

#[derive(Clone)]
struct ServerRuntime {
    config: ServerConfig,
    engine: Arc<Mutex<Engine>>,
    running: Arc<AtomicBool>,
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct HttpApiRuntime {
    engine: Arc<Mutex<Engine>>,
    catalog_base: String,
    bearer_token: Option<String>,
    allow_origin: String,
}

#[derive(Debug, Deserialize)]
struct HttpSqlRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct HttpErrorResponse {
    ok: bool,
    error: String,
}

#[derive(Debug, Serialize)]
struct HttpSqlResponse {
    ok: bool,
    results: Vec<HttpQueryResult>,
}

#[derive(Debug, Serialize)]
struct HttpAdminLinesResponse {
    ok: bool,
    source: String,
    lines: Vec<String>,
}

#[derive(Debug, Serialize)]
struct HttpAdminStatsResponse {
    ok: bool,
    source: String,
    stats: serde_json::Value,
}

#[derive(Debug, Clone)]
struct MigrationEntry {
    version: u64,
    name: String,
    up_path: Option<PathBuf>,
    down_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RbacStore {
    users: BTreeMap<String, RbacUser>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RbacUser {
    username: String,
    token: String,
    grants: Vec<RbacGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RbacGrant {
    scope: RbacScope,
    actions: HashSet<RbacAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RbacScope {
    Database { database: String },
    Table { database: String, table: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RbacAction {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Admin,
}

#[derive(Debug, Clone)]
struct AuthenticatedUser {
    username: String,
    grants: Vec<RbacGrant>,
}

#[derive(Debug, Clone)]
enum RbacAuthOutcome {
    Disabled,
    Authorized(AuthenticatedUser),
    Unauthorized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PermissionRequirement {
    action: RbacAction,
    database: String,
    table: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HttpQueryResult {
    AffectedRows {
        count: usize,
    },
    Message {
        message: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<HttpValue>>,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum HttpValue {
    Int(i32),
    BigInt(i64),
    Bool(bool),
    Double(f64),
    String(String),
    Null,
}

impl ServerConfig {
    fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("--version") | Some("-V") => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        Some("help") => print_usage(),
        Some("init") => {
            let rest: Vec<String> = args.collect();
            cmd_init(&rest)?;
        }
        Some("start") => {
            let rest: Vec<String> = args.collect();
            cmd_start(&rest)?;
        }
        Some("status") => {
            let rest: Vec<String> = args.collect();
            cmd_status(&rest)?;
        }
        Some("stop") => {
            let rest: Vec<String> = args.collect();
            cmd_stop(&rest)?;
        }
        Some("connect") => {
            let rest: Vec<String> = args.collect();
            cmd_connect(&rest)?;
        }
        Some("shell") | None => {
            let rest: Vec<String> = args.collect();
            cmd_shell(&rest)?;
        }
        Some("create-table") => {
            let rest: Vec<String> = args.collect();
            cmd_create_table(&rest)?;
        }
        Some("drop-table") => {
            let rest: Vec<String> = args.collect();
            cmd_drop_table(&rest)?;
        }
        Some("show-tables") => {
            let rest: Vec<String> = args.collect();
            cmd_show_tables(&rest)?;
        }
        Some("describe") => {
            let rest: Vec<String> = args.collect();
            cmd_describe(&rest)?;
        }
        Some("parse-sql") => {
            let rest: Vec<String> = args.collect();
            cmd_parse_sql(&rest)?;
        }
        Some("sql") => {
            let rest: Vec<String> = args.collect();
            cmd_sql(&rest)?;
        }
        Some("backup") => {
            let rest: Vec<String> = args.collect();
            cmd_backup(&rest)?;
        }
        Some("restore") => {
            let rest: Vec<String> = args.collect();
            cmd_restore(&rest)?;
        }
        Some("migrate") => {
            let rest: Vec<String> = args.collect();
            cmd_migrate(&rest)?;
        }
        Some("user-add") => {
            let rest: Vec<String> = args.collect();
            cmd_user_add(&rest)?;
        }
        Some("user-list") => {
            let rest: Vec<String> = args.collect();
            cmd_user_list(&rest)?;
        }
        Some("grant") => {
            let rest: Vec<String> = args.collect();
            cmd_grant(&rest)?;
        }
        Some("http") => {
            let rest: Vec<String> = args.collect();
            cmd_http(&rest)?;
        }
        Some(other) => {
            return Err(RuseDbError::Parse(format!(
                "unknown command '{other}', run `rusedb help`"
            )));
        }
    }
    Ok(())
}

fn cmd_init(args: &[String]) -> Result<()> {
    let instance_dir = parse_instance_dir(args, "rusedb init [instance_dir]")?;
    fs::create_dir_all(&instance_dir)?;
    let config_path = config_path(&instance_dir);
    if config_path.exists() {
        return Err(RuseDbError::AlreadyExists {
            object: "config".to_string(),
            name: config_path.display().to_string(),
        });
    }

    let default_catalog_path = absolute_path(instance_dir.join("data").join("rusedb"))?;
    let host = prompt_with_default("Host", DEFAULT_HOST)?;
    let port_text = prompt_with_default("Port", &DEFAULT_PORT.to_string())?;
    let port = parse_port(&port_text)?;
    let catalog_base = prompt_with_default(
        "Catalog base path",
        default_catalog_path.to_string_lossy().as_ref(),
    )?;
    let admin_user = prompt_with_default("Admin username", DEFAULT_ADMIN)?;
    let password = prompt_required("Admin password")?;
    let password_confirm = prompt_required("Confirm password")?;
    if password != password_confirm {
        return Err(RuseDbError::Parse(
            "password and confirmation do not match".to_string(),
        ));
    }
    if password.is_empty() {
        return Err(RuseDbError::Parse("password cannot be empty".to_string()));
    }

    let salt = generate_salt();
    let password_hash = derive_password_hash(&password, &salt);
    let config = ServerConfig {
        host,
        port,
        catalog_base,
        admin_user,
        password_salt: salt,
        password_hash,
    };
    if let Some(parent) = Path::new(&config.catalog_base).parent() {
        fs::create_dir_all(parent)?;
    }
    save_config(&config_path, &config)?;

    println!("ok: initialized instance at '{}'", instance_dir.display());
    println!("next:");
    println!("  rusedb start {}", instance_dir.display());
    println!("  rusedb connect {}", instance_dir.display());
    Ok(())
}

fn cmd_start(args: &[String]) -> Result<()> {
    let instance_dir = parse_instance_dir(args, "rusedb start [instance_dir]")?;
    let config = load_config(&instance_dir)?;
    if let Some(parent) = Path::new(&config.catalog_base).parent() {
        fs::create_dir_all(parent)?;
    }

    let addr = config.addr();
    let listener = TcpListener::bind(&addr)?;
    listener.set_nonblocking(true)?;
    let runtime = ServerRuntime {
        config: config.clone(),
        engine: Arc::new(Mutex::new(Engine::new(&config.catalog_base))),
        running: Arc::new(AtomicBool::new(true)),
    };
    println!("RuseDB server listening on {addr}");
    println!(
        "instance config: '{}'",
        config_path(&instance_dir).to_string_lossy()
    );
    println!("press Ctrl+C to stop");

    while runtime.running.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, peer)) => {
                let _ = stream.set_nonblocking(false);
                let runtime_cloned = runtime.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, &runtime_cloned) {
                        eprintln!("client {peer} error: {err}");
                    }
                });
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(err) => return Err(RuseDbError::Io(err)),
        }
    }
    println!("server stopped");
    Ok(())
}

fn cmd_status(args: &[String]) -> Result<()> {
    let instance_dir = parse_instance_dir(args, "rusedb status [instance_dir]")?;
    let config = load_config(&instance_dir)?;
    let addr = config.addr();
    match TcpStream::connect(&addr) {
        Ok(stream) => {
            let mut reader = BufReader::new(stream);
            let greeting = read_protocol_line(&mut reader)?.unwrap_or_default();
            if !greeting.is_empty() {
                let _ = parse_protocol_response(&greeting)?;
            }
            println!("ok: server is running at {addr}");
            Ok(())
        }
        Err(err) => Err(RuseDbError::Parse(format!(
            "server not reachable at {addr}: {err}"
        ))),
    }
}

fn cmd_stop(args: &[String]) -> Result<()> {
    let instance_dir = parse_instance_dir(args, "rusedb stop [instance_dir]")?;
    let config = load_config(&instance_dir)?;
    let addr = config.addr();

    let mut stream = TcpStream::connect(&addr).map_err(|err| {
        RuseDbError::Parse(format!(
            "cannot connect to server at {addr} for shutdown: {err}"
        ))
    })?;
    let mut reader = BufReader::new(stream.try_clone()?);
    if let Some(greeting) = read_protocol_line(&mut reader)? {
        let _ = parse_protocol_response(&greeting)?;
    }
    authenticate_remote_session(&config, &mut stream, &mut reader)?;
    write_protocol_line(&mut stream, "SHUTDOWN")?;
    let response = read_protocol_line(&mut reader)?.ok_or(RuseDbError::Parse(
        "server closed connection before shutdown ack".to_string(),
    ))?;
    let (ok, payload) = parse_protocol_response(&response)?;
    if !ok {
        return Err(RuseDbError::Parse(format!("shutdown failed: {payload}")));
    }
    if payload.is_empty() {
        println!("ok: shutdown requested");
    } else {
        println!("{payload}");
    }
    Ok(())
}

fn cmd_connect(args: &[String]) -> Result<()> {
    let instance_dir = parse_instance_dir(args, "rusedb connect [instance_dir]")?;
    let config = load_config(&instance_dir)?;
    let addr = config.addr();

    let mut stream = TcpStream::connect(&addr)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    if let Some(greeting) = read_protocol_line(&mut reader)? {
        let (_, payload) = parse_protocol_response(&greeting)?;
        if !payload.is_empty() {
            println!("{payload}");
        }
    }
    let auth_payload = authenticate_remote_session(&config, &mut stream, &mut reader)?;

    println!("connected to {addr}");
    if !auth_payload.is_empty() {
        println!("{auth_payload}");
    }
    let mut line = String::new();
    loop {
        print!("rusedb@{}> ", addr);
        io::stdout().flush()?;
        line.clear();
        if io::stdin().read_line(&mut line)? == 0 {
            let _ = write_protocol_line(&mut stream, "QUIT");
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case(".exit") || input.eq_ignore_ascii_case(".quit") {
            let _ = write_protocol_line(&mut stream, "QUIT");
            break;
        }

        let statements = split_sql_statements(input)?;
        for statement in statements {
            write_protocol_line(&mut stream, &format!("SQL {statement}"))?;
            let response = read_protocol_line(&mut reader)?.ok_or(RuseDbError::Parse(
                "server closed connection unexpectedly".to_string(),
            ))?;
            let (ok, payload) = parse_protocol_response(&response)?;
            if ok {
                if !payload.is_empty() {
                    println!("{payload}");
                }
            } else {
                eprintln!("error: {payload}");
            }
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, runtime: &ServerRuntime) -> Result<()> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    write_protocol_line(
        &mut writer,
        &format!(
            "OK {}",
            encode_payload("RuseDB ready, send AUTH <user> <password>")
        ),
    )?;

    let Some(auth_line) = read_protocol_line(&mut reader)? else {
        return Ok(());
    };
    let (user, password) = parse_auth_line(&auth_line)?;
    if user != runtime.config.admin_user
        || !verify_password(
            &password,
            &runtime.config.password_salt,
            &runtime.config.password_hash,
        )
    {
        write_protocol_line(&mut writer, "ERR authentication failed")?;
        return Ok(());
    }
    write_protocol_line(&mut writer, "OK authenticated")?;

    while let Some(line) = read_protocol_line(&mut reader)? {
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case("QUIT") {
            write_protocol_line(&mut writer, "OK bye")?;
            break;
        }
        if input.eq_ignore_ascii_case("PING") {
            write_protocol_line(&mut writer, "OK pong")?;
            continue;
        }
        if input.eq_ignore_ascii_case("SHUTDOWN") {
            runtime.running.store(false, Ordering::SeqCst);
            write_protocol_line(
                &mut writer,
                &format!("OK {}", encode_payload("server shutting down")),
            )?;
            break;
        }

        let Some(sql) = strip_prefix_ascii_case(input, "SQL ") else {
            write_protocol_line(&mut writer, "ERR unknown command")?;
            continue;
        };
        match execute_sql_batch(&runtime.engine, sql.trim()) {
            Ok(payload) => {
                write_protocol_line(&mut writer, &format!("OK {}", encode_payload(&payload)))?
            }
            Err(err) => write_protocol_line(
                &mut writer,
                &format!("ERR {}", encode_payload(&err.to_string())),
            )?,
        }
    }
    Ok(())
}

fn execute_sql_batch(engine: &Arc<Mutex<Engine>>, sql: &str) -> Result<String> {
    let statements = split_sql_statements(sql)?;
    if statements.is_empty() {
        return Err(RuseDbError::Parse("no SQL statement provided".to_string()));
    }
    let guard = engine
        .lock()
        .map_err(|_| RuseDbError::Corruption("engine lock poisoned".to_string()))?;
    let mut out = Vec::with_capacity(statements.len());
    for statement in statements {
        let result = guard.execute_sql(&statement)?;
        out.push(render_query_result(&result));
    }
    Ok(out.join("\n"))
}

fn parse_auth_line(line: &str) -> Result<(String, String)> {
    let Some(rest) = strip_prefix_ascii_case(line.trim(), "AUTH ") else {
        return Err(RuseDbError::Parse(
            "first command must be AUTH <user> <password>".to_string(),
        ));
    };
    let mut parts = rest.splitn(2, ' ');
    let user = parts.next().unwrap_or_default().trim().to_string();
    let password = parts.next().unwrap_or_default().to_string();
    if user.is_empty() || password.is_empty() {
        return Err(RuseDbError::Parse(
            "AUTH requires non-empty user and password".to_string(),
        ));
    }
    Ok((user, password))
}

fn authenticate_remote_session(
    config: &ServerConfig,
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
) -> Result<String> {
    let user = prompt_with_default("Username", &config.admin_user)?;
    let password = prompt_required("Password")?;
    write_protocol_line(stream, &format!("AUTH {user} {password}"))?;
    let auth_resp = read_protocol_line(reader)?.ok_or(RuseDbError::Parse(
        "server closed connection before auth response".to_string(),
    ))?;
    let (auth_ok, auth_payload) = parse_protocol_response(&auth_resp)?;
    if !auth_ok {
        return Err(RuseDbError::Parse(format!(
            "authentication failed: {auth_payload}"
        )));
    }
    Ok(auth_payload)
}

fn strip_prefix_ascii_case<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.len() < prefix.len() {
        return None;
    }
    let (head, tail) = input.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail)
    } else {
        None
    }
}

fn write_protocol_line(stream: &mut TcpStream, line: &str) -> Result<()> {
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_protocol_line(reader: &mut BufReader<TcpStream>) -> Result<Option<String>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    while matches!(line.as_bytes().last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    Ok(Some(line))
}

fn parse_protocol_response(line: &str) -> Result<(bool, String)> {
    if line.eq_ignore_ascii_case("OK") {
        return Ok((true, String::new()));
    }
    if line.eq_ignore_ascii_case("ERR") {
        return Ok((false, String::new()));
    }
    if let Some(payload) = line.strip_prefix("OK ") {
        return Ok((true, decode_payload(payload)?));
    }
    if let Some(payload) = line.strip_prefix("ERR ") {
        return Ok((false, decode_payload(payload)?));
    }
    Err(RuseDbError::Parse(format!(
        "invalid server response '{line}'"
    )))
}

fn encode_payload(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}

fn decode_payload(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let escaped = chars.next().ok_or(RuseDbError::Parse(
            "invalid escape in protocol payload".to_string(),
        ))?;
        match escaped {
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            '\\' => out.push('\\'),
            other => {
                return Err(RuseDbError::Parse(format!(
                    "unknown escape sequence '\\{other}'"
                )));
            }
        }
    }
    Ok(out)
}

fn parse_instance_dir(args: &[String], usage: &str) -> Result<PathBuf> {
    if args.len() > 1 {
        return Err(RuseDbError::Parse(format!("usage: {usage}")));
    }
    Ok(PathBuf::from(
        args.first()
            .map(String::as_str)
            .unwrap_or(DEFAULT_INSTANCE_DIR),
    ))
}

fn config_path(instance_dir: &Path) -> PathBuf {
    instance_dir.join("rusedb.conf")
}

fn save_config(path: &Path, config: &ServerConfig) -> Result<()> {
    let content = format!(
        "host={}\nport={}\ncatalog_base={}\nadmin_user={}\npassword_salt={}\npassword_hash={}\n",
        config.host,
        config.port,
        config.catalog_base,
        config.admin_user,
        config.password_salt,
        config.password_hash
    );
    fs::write(path, content)?;
    Ok(())
}

fn load_config(instance_dir: &Path) -> Result<ServerConfig> {
    let path = config_path(instance_dir);
    let content = fs::read_to_string(&path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            RuseDbError::NotFound {
                object: "config".to_string(),
                name: path.display().to_string(),
            }
        } else {
            RuseDbError::Io(err)
        }
    })?;
    let pairs = parse_kv_config(&content)?;

    let host = required_kv(&pairs, "host")?;
    let port = parse_port(&required_kv(&pairs, "port")?)?;
    let catalog_base = required_kv(&pairs, "catalog_base")?;
    let admin_user = required_kv(&pairs, "admin_user")?;
    let password_salt = required_kv(&pairs, "password_salt")?;
    let password_hash = required_kv(&pairs, "password_hash")?;
    Ok(ServerConfig {
        host,
        port,
        catalog_base,
        admin_user,
        password_salt,
        password_hash,
    })
}

fn parse_kv_config(content: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(RuseDbError::Parse(format!(
                "invalid config line {}: '{}'",
                idx + 1,
                line
            )));
        };
        out.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(out)
}

fn required_kv(map: &HashMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .cloned()
        .ok_or(RuseDbError::Parse(format!("missing config key '{}'", key)))
}

fn parse_port(raw: &str) -> Result<u16> {
    let port = raw.trim().parse::<u16>().map_err(|_| {
        RuseDbError::Parse(format!("invalid port '{}': expected integer 1..65535", raw))
    })?;
    if port == 0 {
        return Err(RuseDbError::Parse(
            "invalid port '0': expected integer 1..65535".to_string(),
        ));
    }
    Ok(port)
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    print!("{label} [{default}]: ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let value = line.trim();
    if value.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(value.to_string())
    }
}

fn prompt_required(label: &str) -> Result<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let value = line.trim().to_string();
    if value.is_empty() {
        return Err(RuseDbError::Parse(format!("{label} cannot be empty")));
    }
    Ok(value)
}

fn absolute_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn generate_salt() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{now:x}-{:x}", process::id())
}

fn derive_password_hash(password: &str, salt: &str) -> String {
    let mut state = format!("{password}:{salt}");
    for _ in 0..2048 {
        state = siphash_hex(&state);
    }
    state
}

fn siphash_hex(input: &str) -> String {
    let mut hasher = DefaultHasher::new();
    hasher.write(input.as_bytes());
    format!("{:016x}", hasher.finish())
}

fn verify_password(password: &str, salt: &str, expected_hash: &str) -> bool {
    derive_password_hash(password, salt) == expected_hash
}

fn cmd_create_table(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        return Err(RuseDbError::Parse(
            "usage: rusedb create-table <catalog_base> <table_name> <col:type[?]>...".to_string(),
        ));
    }

    let base = &args[0];
    let table_name = &args[1];
    let columns: Result<Vec<Column>> = args[2..].iter().map(|arg| parse_column_spec(arg)).collect();
    let mut catalog = Catalog::open(base)?;
    let table = catalog.create_table(table_name, columns?, Vec::new())?;
    println!("ok: created table '{}' (id={})", table.name, table.table_id);
    Ok(())
}

fn cmd_drop_table(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb drop-table <catalog_base> <table_name>".to_string(),
        ));
    }
    let mut catalog = Catalog::open(&args[0])?;
    catalog.drop_table(&args[1])?;
    println!("ok: dropped table '{}'", args[1]);
    Ok(())
}

fn cmd_show_tables(args: &[String]) -> Result<()> {
    if args.len() != 1 {
        return Err(RuseDbError::Parse(
            "usage: rusedb show-tables <catalog_base>".to_string(),
        ));
    }
    let catalog = Catalog::open(&args[0])?;
    let tables = catalog.list_tables();
    println!("table_id\tname");
    for table in tables {
        println!("{}\t{}", table.table_id, table.name);
    }
    Ok(())
}

fn cmd_describe(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb describe <catalog_base> <table_name>".to_string(),
        ));
    }
    let catalog = Catalog::open(&args[0])?;
    let schema = catalog.describe_table(&args[1])?;
    println!("column_id\tname\ttype\tnullable");
    for column in schema.columns {
        println!(
            "{}\t{}\t{}\t{}",
            column.id, column.name, column.data_type, column.nullable
        );
    }
    Ok(())
}

fn cmd_parse_sql(args: &[String]) -> Result<()> {
    if args.is_empty() {
        return Err(RuseDbError::Parse(
            "usage: rusedb parse-sql <sql>".to_string(),
        ));
    }
    let sql = args.join(" ");
    let statement = parse_sql(&sql)?;
    println!("{statement:#?}");
    Ok(())
}

fn cmd_sql(args: &[String]) -> Result<()> {
    if args.len() < 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb sql <catalog_base> <sql>".to_string(),
        ));
    }

    let base = &args[0];
    let sql = args[1..].join(" ");
    let engine = Engine::new(base);
    let statements = split_sql_statements(&sql)?;
    if statements.is_empty() {
        return Err(RuseDbError::Parse("no SQL statement provided".to_string()));
    }
    for statement in statements {
        let result = engine.execute_sql(&statement)?;
        print_query_result(result);
    }
    Ok(())
}

fn cmd_backup(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb backup <catalog_base> <backup_dir>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let backup_dir = absolute_path(PathBuf::from(&args[1]))?;
    if backup_dir.exists() && fs::read_dir(&backup_dir)?.next().is_some() {
        return Err(RuseDbError::Parse(format!(
            "backup target '{}' must be empty",
            backup_dir.display()
        )));
    }
    fs::create_dir_all(&backup_dir)?;

    let (_, source_prefix) = family_dir_and_prefix_path(&catalog_base)?;
    let family_files = list_catalog_family_files(&catalog_base)?;
    if family_files.is_empty() {
        return Err(RuseDbError::NotFound {
            object: "catalog-files".to_string(),
            name: catalog_base.display().to_string(),
        });
    }

    let mut copied = 0usize;
    for src in family_files {
        let file_name = src.file_name().ok_or(RuseDbError::Corruption(format!(
            "invalid source file '{}'",
            src.display()
        )))?;
        fs::copy(&src, backup_dir.join(file_name))?;
        copied += 1;
    }

    let created_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let manifest = format!(
        "version=1\nsource_prefix={source_prefix}\nsource_base={}\ncreated_unix_ms={created_unix_ms}\nfiles={copied}\n",
        catalog_base.display()
    );
    fs::write(backup_manifest_path(&backup_dir), manifest)?;

    println!(
        "ok: backup created at '{}' (files={copied})",
        backup_dir.display()
    );
    Ok(())
}

fn cmd_restore(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb restore <backup_dir> <catalog_base>".to_string(),
        ));
    }
    let backup_dir = absolute_path(PathBuf::from(&args[0]))?;
    let target_base = absolute_path(PathBuf::from(&args[1]))?;
    let manifest_path = backup_manifest_path(&backup_dir);
    if !manifest_path.exists() {
        return Err(RuseDbError::NotFound {
            object: "backup-manifest".to_string(),
            name: manifest_path.display().to_string(),
        });
    }
    let manifest_text = fs::read_to_string(&manifest_path)?;
    let manifest_pairs = parse_kv_config(&manifest_text)?;
    let source_prefix = required_kv(&manifest_pairs, "source_prefix")?;

    let (target_dir, target_prefix) = family_dir_and_prefix_path(&target_base)?;
    fs::create_dir_all(&target_dir)?;

    let mut backup_files = Vec::new();
    for entry in fs::read_dir(&backup_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "backup.manifest" {
            continue;
        }
        if is_catalog_family_file_name(&name, &source_prefix) {
            backup_files.push(entry.path());
        }
    }
    if backup_files.is_empty() {
        return Err(RuseDbError::NotFound {
            object: "backup-family-files".to_string(),
            name: backup_dir.display().to_string(),
        });
    }

    for old in list_catalog_family_files(&target_base)? {
        fs::remove_file(old)?;
    }

    let mut restored = 0usize;
    for src in backup_files {
        let name = src
            .file_name()
            .map(|value| value.to_string_lossy().to_string())
            .ok_or(RuseDbError::Corruption(format!(
                "invalid backup file '{}'",
                src.display()
            )))?;
        let mapped = remap_family_file_prefix(&name, &source_prefix, &target_prefix)?;
        fs::copy(&src, target_dir.join(mapped))?;
        restored += 1;
    }
    println!(
        "ok: restore completed from '{}' to '{}' (files={restored})",
        backup_dir.display(),
        target_base.display()
    );
    Ok(())
}

fn cmd_migrate(args: &[String]) -> Result<()> {
    if args.is_empty() {
        return Err(RuseDbError::Parse(
            "usage: rusedb migrate <up|down> <catalog_base> <migrations_dir> [steps]".to_string(),
        ));
    }
    match args[0].as_str() {
        "up" => cmd_migrate_up(&args[1..]),
        "down" => cmd_migrate_down(&args[1..]),
        other => Err(RuseDbError::Parse(format!(
            "unknown migrate action '{other}', expected up or down",
        ))),
    }
}

fn cmd_migrate_up(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(RuseDbError::Parse(
            "usage: rusedb migrate up <catalog_base> <migrations_dir>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let migrations_dir = absolute_path(PathBuf::from(&args[1]))?;
    let engine = Engine::new(&catalog_base);
    let migrations = load_migration_entries(&migrations_dir)?;
    let mut applied = load_applied_migrations(&catalog_base)?;
    let mut applied_map = applied
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut executed = 0usize;

    for migration in migrations {
        let id = migration_id(&migration);
        if applied_map.contains(&id) {
            continue;
        }
        let up_path = migration.up_path.ok_or(RuseDbError::Parse(format!(
            "migration '{id}' missing .up.sql file"
        )))?;
        let sql = fs::read_to_string(&up_path)?;
        execute_migration_script(&engine, &id, &sql)?;
        applied.push(id.clone());
        applied_map.insert(id.clone());
        executed += 1;
    }
    persist_applied_migrations(&catalog_base, &applied)?;
    println!("ok: migrate up completed (applied={executed})");
    Ok(())
}

fn cmd_migrate_down(args: &[String]) -> Result<()> {
    if args.len() != 3 {
        return Err(RuseDbError::Parse(
            "usage: rusedb migrate down <catalog_base> <migrations_dir> <steps>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let migrations_dir = absolute_path(PathBuf::from(&args[1]))?;
    let steps = args[2].trim().parse::<usize>().map_err(|_| {
        RuseDbError::Parse(format!(
            "invalid steps '{}': expected positive integer",
            args[2]
        ))
    })?;
    if steps == 0 {
        return Err(RuseDbError::Parse(
            "steps must be greater than 0".to_string(),
        ));
    }
    let engine = Engine::new(&catalog_base);
    let migrations = load_migration_entries(&migrations_dir)?;
    let migration_map = migrations
        .into_iter()
        .map(|migration| (migration_id(&migration), migration))
        .collect::<HashMap<_, _>>();

    let mut applied = load_applied_migrations(&catalog_base)?;
    if steps > applied.len() {
        return Err(RuseDbError::Parse(format!(
            "cannot migrate down {} step(s): only {} applied migration(s)",
            steps,
            applied.len()
        )));
    }

    let mut executed = 0usize;
    for _ in 0..steps {
        let id = applied.pop().ok_or(RuseDbError::Parse(
            "no applied migrations to rollback".to_string(),
        ))?;
        let migration = migration_map.get(&id).ok_or(RuseDbError::Parse(format!(
            "applied migration '{id}' not found in migrations directory",
        )))?;
        let down_path = migration
            .down_path
            .as_ref()
            .ok_or(RuseDbError::Parse(format!(
                "migration '{id}' missing .down.sql file"
            )))?;
        let sql = fs::read_to_string(down_path)?;
        execute_migration_script(&engine, &id, &sql)?;
        executed += 1;
    }
    persist_applied_migrations(&catalog_base, &applied)?;
    println!("ok: migrate down completed (rolled_back={executed})");
    Ok(())
}

fn cmd_user_add(args: &[String]) -> Result<()> {
    if args.len() != 3 {
        return Err(RuseDbError::Parse(
            "usage: rusedb user-add <catalog_base> <username> <token>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let username = normalize_user_name(&args[1])?;
    let token = args[2].trim();
    if token.is_empty() {
        return Err(RuseDbError::Parse("token cannot be empty".to_string()));
    }

    let mut store = load_rbac_store(&catalog_base)?;
    if store.users.contains_key(&username) {
        return Err(RuseDbError::AlreadyExists {
            object: "rbac-user".to_string(),
            name: username,
        });
    }
    store.users.insert(
        username.clone(),
        RbacUser {
            username: username.clone(),
            token: token.to_string(),
            grants: Vec::new(),
        },
    );
    persist_rbac_store(&catalog_base, &store)?;
    println!("ok: created user '{}'", username);
    Ok(())
}

fn cmd_user_list(args: &[String]) -> Result<()> {
    if args.len() != 1 {
        return Err(RuseDbError::Parse(
            "usage: rusedb user-list <catalog_base>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let store = load_rbac_store(&catalog_base)?;
    println!("username\tgrants");
    for user in store.users.values() {
        println!("{}\t{}", user.username, user.grants.len());
    }
    Ok(())
}

fn cmd_grant(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        return Err(RuseDbError::Parse(
            "usage: rusedb grant <catalog_base> <username> <scope> <actions_csv>".to_string(),
        ));
    }
    let catalog_base = absolute_path(PathBuf::from(&args[0]))?;
    let username = normalize_user_name(&args[1])?;
    let scope = parse_rbac_scope_token(&args[2])?;
    let actions = parse_actions_csv(&args[3])?;

    let mut store = load_rbac_store(&catalog_base)?;
    let user = store
        .users
        .get_mut(&username)
        .ok_or(RuseDbError::NotFound {
            object: "rbac-user".to_string(),
            name: username.clone(),
        })?;
    upsert_user_grant(user, scope, actions);
    persist_rbac_store(&catalog_base, &store)?;
    println!("ok: grant updated for '{}'", username);
    Ok(())
}

fn cmd_http(args: &[String]) -> Result<()> {
    if args.is_empty() {
        return Err(RuseDbError::Parse(
            "usage: rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]".to_string(),
        ));
    }

    let mut cursor = 0usize;
    let base = args
        .get(cursor)
        .ok_or(RuseDbError::Parse(
            "usage: rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]".to_string(),
        ))?
        .clone();
    cursor += 1;

    let mut addr = DEFAULT_HTTP_ADDR.to_string();
    if let Some(next) = args.get(cursor) {
        if !next.starts_with("--") {
            addr = next.clone();
            cursor += 1;
        }
    }

    let mut bearer_token: Option<String> = None;
    let mut allow_origin = "*".to_string();
    while cursor < args.len() {
        match args[cursor].as_str() {
            "--token" => {
                cursor += 1;
                let token = args.get(cursor).ok_or(RuseDbError::Parse(
                    "option --token requires a value".to_string(),
                ))?;
                if token.trim().is_empty() {
                    return Err(RuseDbError::Parse("token cannot be empty".to_string()));
                }
                bearer_token = Some(token.clone());
                cursor += 1;
            }
            "--allow-origin" => {
                cursor += 1;
                let origin = args.get(cursor).ok_or(RuseDbError::Parse(
                    "option --allow-origin requires a value".to_string(),
                ))?;
                if origin.trim().is_empty() {
                    return Err(RuseDbError::Parse(
                        "allow-origin cannot be empty".to_string(),
                    ));
                }
                allow_origin = origin.clone();
                cursor += 1;
            }
            other => {
                return Err(RuseDbError::Parse(format!(
                    "unknown option '{other}' for http command"
                )));
            }
        }
    }

    let listener = TcpListener::bind(&addr)?;
    let runtime = Arc::new(HttpApiRuntime {
        engine: Arc::new(Mutex::new(Engine::new(&base))),
        catalog_base: base.clone(),
        bearer_token,
        allow_origin,
    });

    println!("RuseDB HTTP API listening on http://{addr}");
    println!("catalog_base: '{base}'");
    println!(
        "auth: {}",
        if runtime.bearer_token.is_some() {
            "bearer token enabled"
        } else {
            "disabled"
        }
    );
    println!("cors allow-origin: {}", runtime.allow_origin);
    println!("routes:");
    println!("  GET  /health");
    println!("  POST /sql  {{\"sql\":\"SELECT ...\"}}");
    println!("  GET  /admin/databases");
    println!("  GET  /admin/stats?db=default");
    println!("  GET  /admin/slowlog?db=default&limit=200");
    println!("  GET  /openapi.json");
    println!("  GET  /docs");
    println!("rbac headers when users exist: X-RuseDB-User, X-RuseDB-Token");
    println!("press Ctrl+C to stop");

    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                let runtime_cloned = Arc::clone(&runtime);
                thread::spawn(move || {
                    if let Err(err) = handle_http_client(stream, &runtime_cloned) {
                        eprintln!("http client {peer} error: {err}");
                    }
                });
            }
            Err(err) => return Err(RuseDbError::Io(err)),
        }
    }
}

fn cmd_shell(args: &[String]) -> Result<()> {
    if args.len() > 1 {
        return Err(RuseDbError::Parse(
            "usage: rusedb shell [catalog_base]".to_string(),
        ));
    }
    let base = if args.is_empty() { "rusedb" } else { &args[0] };
    let engine = Engine::new(base);
    println!(
        "RuseDB {} interactive shell, base='{}'. type .exit to quit.",
        env!("CARGO_PKG_VERSION"),
        base
    );
    let mut line = String::new();
    loop {
        print!("rusedb> ");
        io::stdout().flush()?;
        line.clear();
        if io::stdin().read_line(&mut line)? == 0 {
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case(".exit") || input.eq_ignore_ascii_case(".quit") {
            break;
        }
        let statements = split_sql_statements(input)?;
        for statement in statements {
            match engine.execute_sql(&statement) {
                Ok(result) => print_query_result(result),
                Err(err) => eprintln!("error: {err}"),
            }
        }
    }
    Ok(())
}

fn handle_http_client(mut stream: TcpStream, runtime: &Arc<HttpApiRuntime>) -> Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let Some(request) = read_http_request(&mut reader)? else {
        return Ok(());
    };
    let (path, query_params) = split_request_path_and_query(&request.path);
    let cors = cors_headers(&runtime.allow_origin);
    if request.method == "OPTIONS" {
        return write_http_response_with_headers(
            &mut stream,
            204,
            "text/plain; charset=utf-8",
            &[],
            &cors,
        );
    }

    match path.as_str() {
        "/health" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            let body = serde_json::json!({
                "ok": true,
                "service": "rusedb-http",
                "version": env!("CARGO_PKG_VERSION"),
            });
            write_http_json_response_with_headers(&mut stream, 200, &body, &cors)
        }
        "/openapi.json" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            let spec = openapi_spec_json();
            write_http_json_response_with_headers(&mut stream, 200, &spec, &cors)
        }
        "/docs" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            let html = openapi_docs_html();
            write_http_response_with_headers(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                html.as_bytes(),
                &cors,
            )
        }
        "/sql" => {
            if request.method != "POST" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            if !authorize_http_request(&request, runtime, &mut stream, &cors)? {
                return Ok(());
            }
            let rbac_auth = match authenticate_rbac_request(&request, runtime, &mut stream, &cors)?
            {
                RbacAuthOutcome::Unauthorized => return Ok(()),
                other => other,
            };
            let parsed: HttpSqlRequest = match serde_json::from_slice(&request.body) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: format!("invalid JSON payload: {err}"),
                        },
                        &cors,
                    );
                }
            };
            let sql = parsed.sql.trim();
            if sql.is_empty() {
                return write_http_json_response_with_headers(
                    &mut stream,
                    400,
                    &HttpErrorResponse {
                        ok: false,
                        error: "field `sql` cannot be empty".to_string(),
                    },
                    &cors,
                );
            }
            let statements = match split_sql_statements(sql) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            if statements.is_empty() {
                return write_http_json_response_with_headers(
                    &mut stream,
                    400,
                    &HttpErrorResponse {
                        ok: false,
                        error: "no SQL statement provided".to_string(),
                    },
                    &cors,
                );
            }
            let parsed_statements = match parse_sql_batch(&statements) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };

            let mut results = Vec::with_capacity(statements.len());
            let guard = runtime
                .engine
                .lock()
                .map_err(|_| RuseDbError::Corruption("engine lock poisoned".to_string()))?;
            if let RbacAuthOutcome::Authorized(user) = &rbac_auth {
                let current_db = match current_database_from_engine(&guard) {
                    Ok(value) => value,
                    Err(err) => {
                        return write_http_json_response_with_headers(
                            &mut stream,
                            500,
                            &HttpErrorResponse {
                                ok: false,
                                error: format!("failed to resolve current database: {err}"),
                            },
                            &cors,
                        );
                    }
                };
                let requirements =
                    match permission_requirements_for_statements(&parsed_statements, &current_db) {
                        Ok(value) => value,
                        Err(err) => {
                            return write_http_json_response_with_headers(
                                &mut stream,
                                400,
                                &HttpErrorResponse {
                                    ok: false,
                                    error: format!("unable to evaluate RBAC rules: {err}"),
                                },
                                &cors,
                            );
                        }
                    };
                if let Some(missing) = first_missing_requirement(user, &requirements) {
                    return write_http_forbidden(
                        &mut stream,
                        &cors,
                        &permission_denied_message(user, &missing),
                    );
                }
            }
            for statement in statements {
                let result = match guard.execute_sql(&statement) {
                    Ok(value) => value,
                    Err(err) => {
                        return write_http_json_response_with_headers(
                            &mut stream,
                            400,
                            &HttpErrorResponse {
                                ok: false,
                                error: format!("statement `{statement}` failed: {err}"),
                            },
                            &cors,
                        );
                    }
                };
                results.push(http_query_result(result));
            }
            write_http_json_response_with_headers(
                &mut stream,
                200,
                &HttpSqlResponse { ok: true, results },
                &cors,
            )
        }
        "/admin/databases" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            if !authorize_http_request(&request, runtime, &mut stream, &cors)? {
                return Ok(());
            }
            let rbac_auth = match authenticate_rbac_request(&request, runtime, &mut stream, &cors)?
            {
                RbacAuthOutcome::Unauthorized => return Ok(()),
                other => other,
            };
            if let RbacAuthOutcome::Authorized(user) = &rbac_auth {
                let requirement = PermissionRequirement {
                    action: RbacAction::Admin,
                    database: "*".to_string(),
                    table: None,
                };
                if !user_has_requirement(user, &requirement) {
                    return write_http_forbidden(
                        &mut stream,
                        &cors,
                        &permission_denied_message(user, &requirement),
                    );
                }
            }
            let guard = runtime
                .engine
                .lock()
                .map_err(|_| RuseDbError::Corruption("engine lock poisoned".to_string()))?;
            let result = guard.execute_sql("SHOW DATABASES")?;
            write_http_json_response_with_headers(
                &mut stream,
                200,
                &HttpSqlResponse {
                    ok: true,
                    results: vec![http_query_result(result)],
                },
                &cors,
            )
        }
        "/admin/stats" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            if !authorize_http_request(&request, runtime, &mut stream, &cors)? {
                return Ok(());
            }
            let rbac_auth = match authenticate_rbac_request(&request, runtime, &mut stream, &cors)?
            {
                RbacAuthOutcome::Unauthorized => return Ok(()),
                other => other,
            };
            let db_name = query_params
                .get("db")
                .cloned()
                .unwrap_or_else(|| "default".to_string());
            let normalized_db = match normalize_database_name(&db_name) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            if let RbacAuthOutcome::Authorized(user) = &rbac_auth {
                let requirement = PermissionRequirement {
                    action: RbacAction::Admin,
                    database: normalized_db,
                    table: None,
                };
                if !user_has_requirement(user, &requirement) {
                    return write_http_forbidden(
                        &mut stream,
                        &cors,
                        &permission_denied_message(user, &requirement),
                    );
                }
            }
            let db_base = match database_base_from_catalog(&runtime.catalog_base, &db_name) {
                Ok(path) => path,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            let source = family_sidecar_path(&db_base, "stats");
            let stats_text = fs::read_to_string(&source).unwrap_or_default();
            let stats = parse_stats_text_to_json(&stats_text);
            write_http_json_response_with_headers(
                &mut stream,
                200,
                &HttpAdminStatsResponse {
                    ok: true,
                    source: source.display().to_string(),
                    stats,
                },
                &cors,
            )
        }
        "/admin/slowlog" => {
            if request.method != "GET" {
                return write_http_json_response_with_headers(
                    &mut stream,
                    405,
                    &HttpErrorResponse {
                        ok: false,
                        error: "method not allowed".to_string(),
                    },
                    &cors,
                );
            }
            if !authorize_http_request(&request, runtime, &mut stream, &cors)? {
                return Ok(());
            }
            let rbac_auth = match authenticate_rbac_request(&request, runtime, &mut stream, &cors)?
            {
                RbacAuthOutcome::Unauthorized => return Ok(()),
                other => other,
            };
            let db_name = query_params
                .get("db")
                .cloned()
                .unwrap_or_else(|| "default".to_string());
            let normalized_db = match normalize_database_name(&db_name) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            if let RbacAuthOutcome::Authorized(user) = &rbac_auth {
                let requirement = PermissionRequirement {
                    action: RbacAction::Admin,
                    database: normalized_db,
                    table: None,
                };
                if !user_has_requirement(user, &requirement) {
                    return write_http_forbidden(
                        &mut stream,
                        &cors,
                        &permission_denied_message(user, &requirement),
                    );
                }
            }
            let db_base = match database_base_from_catalog(&runtime.catalog_base, &db_name) {
                Ok(path) => path,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            let limit = match parse_query_limit(query_params.get("limit"), 200, 5000) {
                Ok(value) => value,
                Err(err) => {
                    return write_http_json_response_with_headers(
                        &mut stream,
                        400,
                        &HttpErrorResponse {
                            ok: false,
                            error: err.to_string(),
                        },
                        &cors,
                    );
                }
            };
            let source = family_sidecar_path(&db_base, "slowlog");
            let text = fs::read_to_string(&source).unwrap_or_default();
            let mut lines = text
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>();
            if lines.len() > limit {
                lines = lines[lines.len() - limit..].to_vec();
            }
            write_http_json_response_with_headers(
                &mut stream,
                200,
                &HttpAdminLinesResponse {
                    ok: true,
                    source: source.display().to_string(),
                    lines,
                },
                &cors,
            )
        }
        _ => write_http_json_response_with_headers(
            &mut stream,
            404,
            &HttpErrorResponse {
                ok: false,
                error: "route not found".to_string(),
            },
            &cors,
        ),
    }
}

fn read_http_request(reader: &mut BufReader<TcpStream>) -> Result<Option<HttpRequest>> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let request_line = request_line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or(RuseDbError::Parse("invalid HTTP request line".to_string()))?
        .to_string();
    let path = parts
        .next()
        .ok_or(RuseDbError::Parse("invalid HTTP request line".to_string()))?
        .to_string();
    let _version = parts
        .next()
        .ok_or(RuseDbError::Parse("invalid HTTP request line".to_string()))?;

    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_len = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; content_len];
    if content_len > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(Some(HttpRequest {
        method,
        path,
        headers,
        body,
    }))
}

fn write_http_json_response_with_headers<T: Serialize>(
    stream: &mut TcpStream,
    status: u16,
    body: &T,
    extra_headers: &[(String, String)],
) -> Result<()> {
    let payload = serde_json::to_vec(body)
        .map_err(|err| RuseDbError::Parse(format!("json encode error: {err}")))?;
    write_http_response_with_headers(
        stream,
        status,
        "application/json; charset=utf-8",
        &payload,
        extra_headers,
    )
}

fn write_http_response_with_headers(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    extra_headers: &[(String, String)],
) -> Result<()> {
    let status_text = http_status_text(status);
    let mut header = format!(
        "HTTP/1.1 {status} {status_text}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        header.push_str(name);
        header.push_str(": ");
        header.push_str(value);
        header.push_str("\r\n");
    }
    header.push_str("\r\n");
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn http_status_text(status: u16) -> &'static str {
    match status {
        204 => "No Content",
        200 => "OK",
        403 => "Forbidden",
        401 => "Unauthorized",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn cors_headers(allow_origin: &str) -> Vec<(String, String)> {
    vec![
        (
            "Access-Control-Allow-Origin".to_string(),
            allow_origin.to_string(),
        ),
        (
            "Access-Control-Allow-Methods".to_string(),
            "GET, POST, OPTIONS".to_string(),
        ),
        (
            "Access-Control-Allow-Headers".to_string(),
            "Authorization, Content-Type, X-RuseDB-User, X-RuseDB-Token".to_string(),
        ),
        ("Access-Control-Max-Age".to_string(), "86400".to_string()),
    ]
}

fn parse_bearer_token(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() { None } else { Some(token) }
}

fn authorize_http_request(
    request: &HttpRequest,
    runtime: &Arc<HttpApiRuntime>,
    stream: &mut TcpStream,
    cors: &[(String, String)],
) -> Result<bool> {
    let Some(expected_token) = runtime.bearer_token.as_deref() else {
        return Ok(true);
    };
    let provided = request
        .headers
        .get("authorization")
        .and_then(|value| parse_bearer_token(value));
    if provided == Some(expected_token) {
        return Ok(true);
    }

    let mut headers = cors.to_vec();
    headers.push((
        "WWW-Authenticate".to_string(),
        "Bearer realm=\"rusedb-http\"".to_string(),
    ));
    write_http_json_response_with_headers(
        stream,
        401,
        &HttpErrorResponse {
            ok: false,
            error: "unauthorized".to_string(),
        },
        &headers,
    )?;
    Ok(false)
}

fn write_http_forbidden(
    stream: &mut TcpStream,
    cors: &[(String, String)],
    message: &str,
) -> Result<()> {
    write_http_json_response_with_headers(
        stream,
        403,
        &HttpErrorResponse {
            ok: false,
            error: message.to_string(),
        },
        cors,
    )
}

fn write_http_unauthorized(
    stream: &mut TcpStream,
    cors: &[(String, String)],
    message: &str,
) -> Result<()> {
    write_http_json_response_with_headers(
        stream,
        401,
        &HttpErrorResponse {
            ok: false,
            error: message.to_string(),
        },
        cors,
    )
}

fn authenticate_rbac_request(
    request: &HttpRequest,
    runtime: &Arc<HttpApiRuntime>,
    stream: &mut TcpStream,
    cors: &[(String, String)],
) -> Result<RbacAuthOutcome> {
    let store = load_rbac_store(Path::new(&runtime.catalog_base))?;
    if store.users.is_empty() {
        return Ok(RbacAuthOutcome::Disabled);
    }

    let raw_user = request.headers.get("x-rusedb-user").map(String::as_str);
    let raw_token = request.headers.get("x-rusedb-token").map(String::as_str);
    let Some(raw_user) = raw_user else {
        write_http_unauthorized(stream, cors, "missing X-RuseDB-User header")?;
        return Ok(RbacAuthOutcome::Unauthorized);
    };
    let Some(raw_token) = raw_token else {
        write_http_unauthorized(stream, cors, "missing X-RuseDB-Token header")?;
        return Ok(RbacAuthOutcome::Unauthorized);
    };
    let username = match normalize_user_name(raw_user) {
        Ok(value) => value,
        Err(_) => {
            write_http_unauthorized(stream, cors, "invalid RBAC user identity")?;
            return Ok(RbacAuthOutcome::Unauthorized);
        }
    };
    let token = raw_token.trim();
    if token.is_empty() {
        write_http_unauthorized(stream, cors, "invalid RBAC token")?;
        return Ok(RbacAuthOutcome::Unauthorized);
    }

    let Some(user) = store.users.get(&username) else {
        write_http_unauthorized(stream, cors, "invalid RBAC credentials")?;
        return Ok(RbacAuthOutcome::Unauthorized);
    };
    if user.token != token {
        write_http_unauthorized(stream, cors, "invalid RBAC credentials")?;
        return Ok(RbacAuthOutcome::Unauthorized);
    }

    Ok(RbacAuthOutcome::Authorized(AuthenticatedUser {
        username: user.username.clone(),
        grants: user.grants.clone(),
    }))
}

fn parse_sql_batch(statements: &[String]) -> Result<Vec<Statement>> {
    statements
        .iter()
        .map(|statement| parse_sql(statement))
        .collect::<Result<Vec<_>>>()
}

fn current_database_from_engine(engine: &Engine) -> Result<String> {
    let result = engine.execute_sql("SHOW CURRENT DATABASE")?;
    let QueryResult::Rows { rows, .. } = result else {
        return Err(RuseDbError::Corruption(
            "SHOW CURRENT DATABASE returned unexpected result".to_string(),
        ));
    };
    let name = rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            Value::Varchar(name) => Some(name.clone()),
            _ => None,
        })
        .ok_or(RuseDbError::Corruption(
            "SHOW CURRENT DATABASE returned malformed row".to_string(),
        ))?;
    normalize_database_name(&name)
}

fn split_request_path_and_query(raw_path: &str) -> (String, HashMap<String, String>) {
    let mut query = HashMap::new();
    let Some((path, raw_query)) = raw_path.split_once('?') else {
        return (raw_path.to_string(), query);
    };
    for pair in raw_query.split('&') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (key, value) = trimmed.split_once('=').unwrap_or((trimmed, ""));
        if key.trim().is_empty() {
            continue;
        }
        query.insert(key.trim().to_string(), value.trim().to_string());
    }
    (path.to_string(), query)
}

fn parse_query_limit(raw: Option<&String>, default: usize, max: usize) -> Result<usize> {
    let Some(raw_value) = raw else {
        return Ok(default);
    };
    let value = raw_value.trim().parse::<usize>().map_err(|_| {
        RuseDbError::Parse(format!(
            "invalid limit '{}': expected integer 1..{}",
            raw_value, max
        ))
    })?;
    if value == 0 {
        return Err(RuseDbError::Parse(
            "limit must be greater than 0".to_string(),
        ));
    }
    Ok(value.min(max))
}

fn family_sidecar_path(base: &Path, suffix: &str) -> PathBuf {
    let mut out = base.as_os_str().to_os_string();
    out.push(format!(".{suffix}"));
    PathBuf::from(out)
}

fn parse_stats_text_to_json(content: &str) -> serde_json::Value {
    if content.trim().is_empty() {
        return serde_json::json!({
            "version": null,
            "tables": []
        });
    }

    let mut version: Option<u32> = None;
    let mut tables = Vec::new();
    let mut current_table: Option<serde_json::Map<String, serde_json::Value>> = None;
    let mut raw_lines = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        raw_lines.push(trimmed.to_string());
        if version.is_none() && trimmed.starts_with("RUSEDB_STATS ") {
            version = trimmed
                .split_whitespace()
                .nth(1)
                .and_then(|value| value.parse::<u32>().ok());
            continue;
        }

        let mut parts = trimmed.split_whitespace();
        let Some(tag) = parts.next() else {
            continue;
        };
        match tag {
            "TABLE" => {
                if let Some(prev) = current_table.take() {
                    tables.push(prev);
                }
                let table = parts.next().unwrap_or_default().to_string();
                let row_count = parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let updated_unix_ms = parts
                    .next()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                let mut map = serde_json::Map::new();
                map.insert("table".to_string(), serde_json::Value::String(table));
                map.insert("row_count".to_string(), serde_json::Value::from(row_count));
                map.insert(
                    "updated_unix_ms".to_string(),
                    serde_json::Value::from(updated_unix_ms),
                );
                map.insert("columns".to_string(), serde_json::json!([]));
                current_table = Some(map);
            }
            "COLUMN" => {
                if let Some(table) = current_table.as_mut() {
                    let name = parts.next().unwrap_or_default();
                    let distinct = parts
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    let null_count = parts
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    if let Some(columns) = table.get_mut("columns").and_then(|v| v.as_array_mut()) {
                        columns.push(serde_json::json!({
                            "name": name,
                            "distinct_count": distinct,
                            "null_count": null_count
                        }));
                    }
                }
            }
            "END" => {
                if let Some(prev) = current_table.take() {
                    tables.push(prev);
                }
            }
            _ => {}
        }
    }
    if let Some(prev) = current_table.take() {
        tables.push(prev);
    }

    serde_json::json!({
        "version": version,
        "tables": tables,
        "raw_lines": raw_lines
    })
}

fn normalize_identifier_name(name: &str, label: &str) -> Result<String> {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(RuseDbError::Parse(format!("{label} cannot be empty")));
    }
    let mut chars = normalized.chars();
    let Some(first) = chars.next() else {
        return Err(RuseDbError::Parse(format!("{label} cannot be empty")));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(RuseDbError::Parse(format!(
            "invalid {label} '{}': must start with a letter or underscore",
            name
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(RuseDbError::Parse(format!(
            "invalid {label} '{}': only letters, digits and underscore are allowed",
            name
        )));
    }
    Ok(normalized)
}

fn normalize_identifier_or_wildcard(name: &str, label: &str) -> Result<String> {
    let trimmed = name.trim();
    if trimmed == "*" {
        return Ok("*".to_string());
    }
    normalize_identifier_name(trimmed, label)
}

fn normalize_database_name(name: &str) -> Result<String> {
    normalize_identifier_name(name, "database name")
}

fn normalize_table_name(name: &str) -> Result<String> {
    normalize_identifier_name(name, "table name")
}

fn normalize_user_name(name: &str) -> Result<String> {
    normalize_identifier_name(name, "username")
}

fn normalize_scope_database_name(name: &str) -> Result<String> {
    normalize_identifier_or_wildcard(name, "database scope")
}

fn normalize_scope_table_name(name: &str) -> Result<String> {
    normalize_identifier_or_wildcard(name, "table scope")
}

fn rbac_store_path(catalog_base: &Path) -> PathBuf {
    family_sidecar_path(catalog_base, "rbac")
}

fn load_rbac_store(catalog_base: &Path) -> Result<RbacStore> {
    let path = rbac_store_path(catalog_base);
    if !path.exists() {
        return Ok(RbacStore::default());
    }
    let raw = fs::read_to_string(&path)?;
    if raw.trim().is_empty() {
        return Ok(RbacStore::default());
    }
    let mut store: RbacStore = serde_json::from_str(&raw).map_err(|err| {
        RuseDbError::Parse(format!("invalid RBAC store '{}': {err}", path.display()))
    })?;

    let mut normalized = BTreeMap::new();
    for (_, mut user) in store.users {
        let username = normalize_user_name(&user.username)?;
        user.username = username.clone();
        for grant in &mut user.grants {
            match &mut grant.scope {
                RbacScope::Database { database } => {
                    *database = normalize_scope_database_name(database)?;
                }
                RbacScope::Table { database, table } => {
                    *database = normalize_scope_database_name(database)?;
                    *table = normalize_scope_table_name(table)?;
                }
            }
        }
        normalized.insert(username, user);
    }
    store.users = normalized;
    Ok(store)
}

fn persist_rbac_store(catalog_base: &Path, store: &RbacStore) -> Result<()> {
    let path = rbac_store_path(catalog_base);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(store)
        .map_err(|err| RuseDbError::Parse(format!("failed to encode RBAC store: {err}")))?;
    fs::write(path, payload)?;
    Ok(())
}

fn parse_rbac_scope_token(raw: &str) -> Result<RbacScope> {
    let token = raw.trim();
    if token.is_empty() {
        return Err(RuseDbError::Parse("scope cannot be empty".to_string()));
    }
    let (kind, value) = if let Some((prefix, rest)) = token.split_once(':') {
        (Some(prefix.trim().to_ascii_lowercase()), rest.trim())
    } else {
        (None, token)
    };
    if value.is_empty() {
        return Err(RuseDbError::Parse("scope cannot be empty".to_string()));
    }

    let parse_db_scope = || -> Result<RbacScope> {
        Ok(RbacScope::Database {
            database: normalize_scope_database_name(value)?,
        })
    };
    let parse_table_scope = || -> Result<RbacScope> {
        let (database, table) = value.split_once('.').ok_or(RuseDbError::Parse(format!(
            "invalid table scope '{}': expected <database>.<table>",
            raw
        )))?;
        Ok(RbacScope::Table {
            database: normalize_scope_database_name(database)?,
            table: normalize_scope_table_name(table)?,
        })
    };

    match kind.as_deref() {
        Some("db") | Some("database") => parse_db_scope(),
        Some("table") => parse_table_scope(),
        Some(other) => Err(RuseDbError::Parse(format!(
            "invalid scope prefix '{}': expected db/database/table",
            other
        ))),
        None => {
            if value.contains('.') {
                parse_table_scope()
            } else {
                parse_db_scope()
            }
        }
    }
}

fn parse_action_token(raw: &str) -> Result<Vec<RbacAction>> {
    let action = raw.trim().to_ascii_lowercase();
    if action.is_empty() {
        return Err(RuseDbError::Parse("action cannot be empty".to_string()));
    }
    let expanded = match action.as_str() {
        "select" | "read" => vec![RbacAction::Select],
        "insert" => vec![RbacAction::Insert],
        "update" => vec![RbacAction::Update],
        "delete" => vec![RbacAction::Delete],
        "write" => vec![RbacAction::Insert, RbacAction::Update, RbacAction::Delete],
        "ddl" => vec![RbacAction::Ddl],
        "admin" => vec![RbacAction::Admin],
        "all" => vec![
            RbacAction::Select,
            RbacAction::Insert,
            RbacAction::Update,
            RbacAction::Delete,
            RbacAction::Ddl,
            RbacAction::Admin,
        ],
        _ => {
            return Err(RuseDbError::Parse(format!(
                "unknown RBAC action '{}': expected select,insert,update,delete,ddl,admin,write,all",
                raw
            )));
        }
    };
    Ok(expanded)
}

fn parse_actions_csv(raw: &str) -> Result<HashSet<RbacAction>> {
    let mut actions = HashSet::new();
    for token in raw.split(',') {
        for action in parse_action_token(token)? {
            actions.insert(action);
        }
    }
    if actions.is_empty() {
        return Err(RuseDbError::Parse(
            "actions cannot be empty; expected CSV like select,insert".to_string(),
        ));
    }
    Ok(actions)
}

fn upsert_user_grant(user: &mut RbacUser, scope: RbacScope, actions: HashSet<RbacAction>) {
    if let Some(existing) = user.grants.iter_mut().find(|grant| grant.scope == scope) {
        existing.actions.extend(actions);
        return;
    }
    user.grants.push(RbacGrant { scope, actions });
}

fn permission_target(requirement: &PermissionRequirement) -> String {
    match &requirement.table {
        Some(table) => format!("{}.{}", requirement.database, table),
        None => requirement.database.clone(),
    }
}

fn permission_denied_message(
    user: &AuthenticatedUser,
    requirement: &PermissionRequirement,
) -> String {
    format!(
        "RBAC denied: user '{}' requires '{}' on '{}'",
        user.username,
        rbac_action_name(requirement.action),
        permission_target(requirement)
    )
}

fn rbac_action_name(action: RbacAction) -> &'static str {
    match action {
        RbacAction::Select => "select",
        RbacAction::Insert => "insert",
        RbacAction::Update => "update",
        RbacAction::Delete => "delete",
        RbacAction::Ddl => "ddl",
        RbacAction::Admin => "admin",
    }
}

fn names_match(scope_name: &str, concrete_name: &str) -> bool {
    scope_name == "*" || scope_name == concrete_name
}

fn scope_matches_requirement(scope: &RbacScope, requirement: &PermissionRequirement) -> bool {
    match scope {
        RbacScope::Database { database } => names_match(database, &requirement.database),
        RbacScope::Table { database, table } => {
            let Some(requirement_table) = requirement.table.as_deref() else {
                return false;
            };
            names_match(database, &requirement.database) && names_match(table, requirement_table)
        }
    }
}

fn grant_satisfies_requirement(grant: &RbacGrant, requirement: &PermissionRequirement) -> bool {
    if !scope_matches_requirement(&grant.scope, requirement) {
        return false;
    }
    grant.actions.contains(&RbacAction::Admin) || grant.actions.contains(&requirement.action)
}

fn user_has_requirement(user: &AuthenticatedUser, requirement: &PermissionRequirement) -> bool {
    user.grants
        .iter()
        .any(|grant| grant_satisfies_requirement(grant, requirement))
}

fn first_missing_requirement(
    user: &AuthenticatedUser,
    requirements: &[PermissionRequirement],
) -> Option<PermissionRequirement> {
    requirements
        .iter()
        .find(|requirement| !user_has_requirement(user, requirement))
        .cloned()
}

fn push_permission_requirement(
    out: &mut Vec<PermissionRequirement>,
    action: RbacAction,
    database: String,
    table: Option<String>,
) {
    let requirement = PermissionRequirement {
        action,
        database,
        table,
    };
    if !out.contains(&requirement) {
        out.push(requirement);
    }
}

fn collect_expression_permissions(
    expr: &Expr,
    current_db: &str,
    out: &mut Vec<PermissionRequirement>,
) -> Result<()> {
    match expr {
        Expr::Binary { left, right, .. } => {
            collect_expression_permissions(left, current_db, out)?;
            collect_expression_permissions(right, current_db, out)?;
        }
        Expr::Unary { expr, .. } => {
            collect_expression_permissions(expr, current_db, out)?;
        }
        Expr::InList { expr, list, .. } => {
            collect_expression_permissions(expr, current_db, out)?;
            for item in list {
                collect_expression_permissions(item, current_db, out)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expression_permissions(expr, current_db, out)?;
            collect_statement_permissions(subquery, current_db, out)?;
        }
        Expr::ScalarSubquery { subquery } => {
            collect_statement_permissions(subquery, current_db, out)?;
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expression_permissions(expr, current_db, out)?;
            collect_expression_permissions(pattern, current_db, out)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expression_permissions(expr, current_db, out)?;
            collect_expression_permissions(low, current_db, out)?;
            collect_expression_permissions(high, current_db, out)?;
        }
        Expr::IsNull { expr, .. } => {
            collect_expression_permissions(expr, current_db, out)?;
        }
        Expr::Identifier(_) | Expr::Literal(_) | Expr::Aggregate { .. } => {}
    }
    Ok(())
}

fn collect_join_permissions(
    joins: &[JoinClause],
    current_db: &str,
    out: &mut Vec<PermissionRequirement>,
) -> Result<()> {
    for join in joins {
        let join_table = normalize_table_name(&join.table)?;
        push_permission_requirement(
            out,
            RbacAction::Select,
            current_db.to_string(),
            Some(join_table),
        );
        collect_expression_permissions(&join.on, current_db, out)?;
    }
    Ok(())
}

fn collect_statement_permissions(
    statement: &Statement,
    current_db: &str,
    out: &mut Vec<PermissionRequirement>,
) -> Result<()> {
    match statement {
        Statement::Explain { statement, .. } => {
            collect_statement_permissions(statement, current_db, out)?;
        }
        Statement::AnalyzeTable { table } => {
            push_permission_requirement(
                out,
                RbacAction::Admin,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => {}
        Statement::CreateDatabase { name } | Statement::DropDatabase { name } => {
            push_permission_requirement(out, RbacAction::Ddl, normalize_database_name(name)?, None);
        }
        Statement::UseDatabase { name } => {
            push_permission_requirement(
                out,
                RbacAction::Select,
                normalize_database_name(name)?,
                None,
            );
        }
        Statement::ShowDatabases => {
            push_permission_requirement(out, RbacAction::Admin, "*".to_string(), None);
        }
        Statement::ShowCurrentDatabase | Statement::ShowTables => {
            push_permission_requirement(out, RbacAction::Select, current_db.to_string(), None);
        }
        Statement::DropTable { name } => {
            push_permission_requirement(
                out,
                RbacAction::Ddl,
                current_db.to_string(),
                Some(normalize_table_name(name)?),
            );
        }
        Statement::AlterTableAddColumn { table, .. }
        | Statement::AlterTableDropColumn { table, .. }
        | Statement::AlterTableAlterColumn { table, .. }
        | Statement::CreateTable { name: table, .. }
        | Statement::CreateIndex { table, .. } => {
            push_permission_requirement(
                out,
                RbacAction::Ddl,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
        }
        Statement::RenameTable { old_name, new_name } => {
            push_permission_requirement(
                out,
                RbacAction::Ddl,
                current_db.to_string(),
                Some(normalize_table_name(old_name)?),
            );
            push_permission_requirement(
                out,
                RbacAction::Ddl,
                current_db.to_string(),
                Some(normalize_table_name(new_name)?),
            );
        }
        Statement::RenameColumn { table, .. } => {
            push_permission_requirement(
                out,
                RbacAction::Ddl,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
        }
        Statement::Insert { table, rows, .. } => {
            push_permission_requirement(
                out,
                RbacAction::Insert,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
            for row in rows {
                for expr in row {
                    collect_expression_permissions(expr, current_db, out)?;
                }
            }
        }
        Statement::Select {
            table,
            joins,
            selection,
            having,
            ..
        } => {
            push_permission_requirement(
                out,
                RbacAction::Select,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
            collect_join_permissions(joins, current_db, out)?;
            if let Some(selection_expr) = selection {
                collect_expression_permissions(selection_expr, current_db, out)?;
            }
            if let Some(having_expr) = having {
                collect_expression_permissions(having_expr, current_db, out)?;
            }
        }
        Statement::Delete { table, selection } => {
            push_permission_requirement(
                out,
                RbacAction::Delete,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
            if let Some(selection_expr) = selection {
                collect_expression_permissions(selection_expr, current_db, out)?;
            }
        }
        Statement::Update {
            table,
            assignments,
            selection,
        } => {
            push_permission_requirement(
                out,
                RbacAction::Update,
                current_db.to_string(),
                Some(normalize_table_name(table)?),
            );
            for assignment in assignments {
                collect_expression_permissions(&assignment.value, current_db, out)?;
            }
            if let Some(selection_expr) = selection {
                collect_expression_permissions(selection_expr, current_db, out)?;
            }
        }
    }
    Ok(())
}

fn permission_requirements_for_statements(
    statements: &[Statement],
    initial_database: &str,
) -> Result<Vec<PermissionRequirement>> {
    let mut requirements = Vec::new();
    let mut current_db = normalize_database_name(initial_database)?;
    for statement in statements {
        collect_statement_permissions(statement, &current_db, &mut requirements)?;
        if let Statement::UseDatabase { name } = statement {
            current_db = normalize_database_name(name)?;
        }
    }
    Ok(requirements)
}

fn family_dir_and_prefix_path(base: &Path) -> Result<(PathBuf, String)> {
    let dir = base
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let prefix = base
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or(RuseDbError::Parse(format!(
            "invalid catalog base path '{}'",
            base.display()
        )))?;
    Ok((dir, prefix))
}

fn is_catalog_family_file_name(file_name: &str, prefix: &str) -> bool {
    file_name.starts_with(&format!("{prefix}.")) || file_name.starts_with(&format!("{prefix}-db-"))
}

fn list_catalog_family_files(catalog_base: &Path) -> Result<Vec<PathBuf>> {
    let (dir, prefix) = family_dir_and_prefix_path(catalog_base)?;
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if is_catalog_family_file_name(&name, &prefix) {
            out.push(entry.path());
        }
    }
    Ok(out)
}

fn remap_family_file_prefix(file_name: &str, from_prefix: &str, to_prefix: &str) -> Result<String> {
    let suffix = file_name
        .strip_prefix(from_prefix)
        .ok_or(RuseDbError::Corruption(format!(
            "backup file '{}' does not start with '{}'",
            file_name, from_prefix
        )))?;
    Ok(format!("{to_prefix}{suffix}"))
}

fn backup_manifest_path(backup_dir: &Path) -> PathBuf {
    backup_dir.join("backup.manifest")
}

fn database_base_from_catalog(catalog_base: &str, db_name: &str) -> Result<PathBuf> {
    let catalog_base = PathBuf::from(catalog_base);
    let normalized = normalize_database_name(db_name)?;
    if normalized == "default" {
        return Ok(catalog_base);
    }
    let (dir, prefix) = family_dir_and_prefix_path(&catalog_base)?;
    Ok(dir.join(format!("{prefix}-db-{normalized}")))
}

fn migration_state_path(catalog_base: &Path) -> PathBuf {
    family_sidecar_path(catalog_base, "migrations")
}

fn parse_migration_file_name(file_name: &str) -> Result<Option<(u64, String, bool)>> {
    if !file_name.ends_with(".sql") {
        return Ok(None);
    }
    let (stem, is_up) = if let Some(value) = file_name.strip_suffix(".up.sql") {
        (value, true)
    } else if let Some(value) = file_name.strip_suffix(".down.sql") {
        (value, false)
    } else {
        return Err(RuseDbError::Parse(format!(
            "invalid migration file '{}': expected .up.sql or .down.sql suffix",
            file_name
        )));
    };

    let (version_raw, name) = stem.split_once('_').ok_or(RuseDbError::Parse(format!(
        "invalid migration file '{}': expected <version>_<name>.up.sql",
        file_name
    )))?;
    if name.trim().is_empty() {
        return Err(RuseDbError::Parse(format!(
            "invalid migration file '{}': name cannot be empty",
            file_name
        )));
    }
    let version = version_raw.parse::<u64>().map_err(|_| {
        RuseDbError::Parse(format!(
            "invalid migration file '{}': version must be integer",
            file_name
        ))
    })?;
    Ok(Some((version, name.to_string(), is_up)))
}

fn load_migration_entries(migrations_dir: &Path) -> Result<Vec<MigrationEntry>> {
    if !migrations_dir.exists() {
        return Err(RuseDbError::NotFound {
            object: "migrations-dir".to_string(),
            name: migrations_dir.display().to_string(),
        });
    }
    let mut entries = BTreeMap::<(u64, String), MigrationEntry>::new();
    for item in fs::read_dir(migrations_dir)? {
        let item = item?;
        if !item.file_type()?.is_file() {
            continue;
        }
        let file_name = item.file_name().to_string_lossy().to_string();
        let Some((version, name, is_up)) = parse_migration_file_name(&file_name)? else {
            continue;
        };
        let key = (version, name.clone());
        let entry = entries.entry(key).or_insert(MigrationEntry {
            version,
            name,
            up_path: None,
            down_path: None,
        });
        if is_up {
            entry.up_path = Some(item.path());
        } else {
            entry.down_path = Some(item.path());
        }
    }
    Ok(entries.into_values().collect())
}

fn migration_id(entry: &MigrationEntry) -> String {
    format!("{}_{}", entry.version, entry.name)
}

fn load_applied_migrations(catalog_base: &Path) -> Result<Vec<String>> {
    let path = migration_state_path(catalog_base);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(path)?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToString::to_string)
        .collect())
}

fn persist_applied_migrations(catalog_base: &Path, applied: &[String]) -> Result<()> {
    let mut content = String::new();
    for item in applied {
        content.push_str(item);
        content.push('\n');
    }
    fs::write(migration_state_path(catalog_base), content)?;
    Ok(())
}

fn is_tx_control_statement(statement: &str) -> bool {
    let upper = statement.trim().to_ascii_uppercase();
    upper.starts_with("BEGIN") || upper.starts_with("COMMIT") || upper.starts_with("ROLLBACK")
}

fn execute_migration_script(engine: &Engine, migration_id: &str, sql: &str) -> Result<()> {
    let statements = split_sql_statements(sql)?;
    if statements.is_empty() {
        return Err(RuseDbError::Parse(format!(
            "migration '{}' has no SQL statements",
            migration_id
        )));
    }
    let has_tx_control = statements.iter().any(|stmt| is_tx_control_statement(stmt));
    if has_tx_control {
        for statement in statements {
            engine.execute_sql(&statement)?;
        }
        return Ok(());
    }

    engine.execute_sql("BEGIN")?;
    for statement in statements {
        if let Err(err) = engine.execute_sql(&statement) {
            let _ = engine.execute_sql("ROLLBACK");
            return Err(RuseDbError::Parse(format!(
                "migration '{}' failed: {}",
                migration_id, err
            )));
        }
    }
    if let Err(err) = engine.execute_sql("COMMIT") {
        let _ = engine.execute_sql("ROLLBACK");
        return Err(RuseDbError::Parse(format!(
            "migration '{}' commit failed: {}",
            migration_id, err
        )));
    }
    Ok(())
}

fn openapi_spec_json() -> serde_json::Value {
    serde_json::json!({
      "openapi": "3.0.3",
      "info": {
        "title": "RuseDB HTTP API",
        "version": env!("CARGO_PKG_VERSION"),
        "description": "HTTP gateway for executing SQL against RuseDB."
      },
      "paths": {
        "/health": {
          "get": {
            "summary": "Health check",
            "responses": {
              "200": {"description": "Service is healthy"}
            }
          }
        },
        "/sql": {
          "post": {
            "summary": "Execute SQL batch",
            "description": "If RBAC users are configured, include X-RuseDB-User and X-RuseDB-Token headers.",
            "security": [{"bearerAuth": []}],
            "requestBody": {
              "required": true,
              "content": {
                "application/json": {
                  "schema": {
                    "type": "object",
                    "required": ["sql"],
                    "properties": {
                      "sql": {"type": "string", "example": "SHOW DATABASES; SELECT * FROM users;"}
                    }
                  }
                }
              }
            },
            "responses": {
              "200": {"description": "SQL executed successfully"},
              "400": {"description": "Bad SQL or payload"},
              "401": {"description": "Unauthorized"}
            }
          }
        },
        "/admin/databases": {
          "get": {
            "summary": "List databases (management endpoint)",
            "description": "Requires admin permission when RBAC users are configured.",
            "security": [{"bearerAuth": []}],
            "responses": {
              "200": {"description": "Database list"},
              "401": {"description": "Unauthorized"}
            }
          }
        },
        "/admin/stats": {
          "get": {
            "summary": "Read stats sidecar (management endpoint)",
            "description": "Requires admin permission on target database when RBAC users are configured.",
            "security": [{"bearerAuth": []}],
            "parameters": [
              {"name": "db", "in": "query", "required": false, "schema": {"type": "string", "default": "default"}}
            ],
            "responses": {
              "200": {"description": "Stats payload"},
              "401": {"description": "Unauthorized"}
            }
          }
        },
        "/admin/slowlog": {
          "get": {
            "summary": "Read slow query log (management endpoint)",
            "description": "Requires admin permission on target database when RBAC users are configured.",
            "security": [{"bearerAuth": []}],
            "parameters": [
              {"name": "db", "in": "query", "required": false, "schema": {"type": "string", "default": "default"}},
              {"name": "limit", "in": "query", "required": false, "schema": {"type": "integer", "default": 200}}
            ],
            "responses": {
              "200": {"description": "Slow log lines"},
              "401": {"description": "Unauthorized"}
            }
          }
        },
        "/openapi.json": {
          "get": {
            "summary": "OpenAPI document",
            "responses": {
              "200": {"description": "OpenAPI JSON"}
            }
          }
        },
        "/docs": {
          "get": {
            "summary": "Swagger UI page",
            "responses": {
              "200": {"description": "Swagger documentation"}
            }
          }
        }
      },
      "components": {
        "securitySchemes": {
          "bearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "API Token"
          }
        }
      }
    })
}

fn openapi_docs_html() -> &'static str {
    r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>RuseDB HTTP API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: '/openapi.json',
      dom_id: '#swagger-ui'
    });
  </script>
</body>
</html>"#
}

fn http_query_result(result: QueryResult) -> HttpQueryResult {
    match result {
        QueryResult::AffectedRows(count) => HttpQueryResult::AffectedRows { count },
        QueryResult::Message(message) => HttpQueryResult::Message { message },
        QueryResult::Rows { columns, rows } => HttpQueryResult::Rows {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(http_value).collect())
                .collect(),
        },
    }
}

fn http_value(value: Value) -> HttpValue {
    match value {
        Value::Int(v) => HttpValue::Int(v),
        Value::BigInt(v) => HttpValue::BigInt(v),
        Value::Bool(v) => HttpValue::Bool(v),
        Value::Double(v) => HttpValue::Double(v),
        Value::Varchar(v) => HttpValue::String(v),
        Value::Null => HttpValue::Null,
    }
}

fn parse_column_spec(spec: &str) -> Result<Column> {
    let (name, type_token) = spec
        .split_once(':')
        .ok_or(RuseDbError::Parse(format!("invalid column spec '{spec}'")))?;

    let mut ty = type_token.trim();
    let nullable = ty.ends_with('?');
    if nullable {
        ty = ty.strip_suffix('?').unwrap_or(ty);
    }
    let data_type: DataType = ty.parse()?;
    Ok(Column::new(name.trim(), data_type, nullable))
}

fn print_usage() {
    println!("RuseDB {}", env!("CARGO_PKG_VERSION"));
    println!("commands:");
    println!("  rusedb --version");
    println!("  rusedb help");
    println!("  rusedb init [instance_dir]");
    println!("  rusedb start [instance_dir]");
    println!("  rusedb status [instance_dir]");
    println!("  rusedb stop [instance_dir]");
    println!("  rusedb connect [instance_dir]");
    println!("  rusedb shell [catalog_base]");
    println!("  rusedb create-table <catalog_base> <table_name> <col:type[?]>...");
    println!("  rusedb drop-table <catalog_base> <table_name>");
    println!("  rusedb show-tables <catalog_base>");
    println!("  rusedb describe <catalog_base> <table_name>");
    println!("  rusedb parse-sql <sql>");
    println!("  rusedb sql <catalog_base> <sql-or-batch>");
    println!("  rusedb backup <catalog_base> <backup_dir>");
    println!("  rusedb restore <backup_dir> <catalog_base>");
    println!("  rusedb migrate up <catalog_base> <migrations_dir>");
    println!("  rusedb migrate down <catalog_base> <migrations_dir> <steps>");
    println!("  rusedb user-add <catalog_base> <username> <token>");
    println!("  rusedb user-list <catalog_base>");
    println!("  rusedb grant <catalog_base> <username> <scope> <actions_csv>");
    println!(
        "  rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]"
    );
    println!("    scope examples: db:app | table:app.users | table:app.* | db:*");
    println!("    actions: select,insert,update,delete,ddl,admin,write,all");
}

fn print_query_result(result: QueryResult) {
    println!("{}", render_query_result(&result));
}

fn render_query_result(result: &QueryResult) -> String {
    match result {
        QueryResult::AffectedRows(count) => format!("ok: {count} row(s) affected"),
        QueryResult::Message(message) => format!("ok: {message}"),
        QueryResult::Rows { columns, rows } => {
            let mut lines = Vec::with_capacity(rows.len() + 1);
            lines.push(columns.join("\t"));
            for row in rows {
                let cells: Vec<String> = row.iter().map(format_value).collect();
                lines.push(cells.join("\t"));
            }
            lines.join("\n")
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Varchar(v) => v.clone(),
        Value::Null => "NULL".to_string(),
    }
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            current.push(ch);
            if in_string {
                if matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().unwrap_or('\''));
                } else {
                    in_string = false;
                }
            } else {
                in_string = true;
            }
            continue;
        }
        if ch == ';' && !in_string {
            let stmt = current.trim();
            if !stmt.is_empty() {
                out.push(stmt.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }
    if in_string {
        return Err(RuseDbError::Parse(
            "unterminated string literal in SQL batch".to_string(),
        ));
    }
    let tail = current.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        AuthenticatedUser, PermissionRequirement, RbacAction, RbacGrant, RbacScope, RbacUser,
        first_missing_requirement, parse_actions_csv, parse_migration_file_name,
        parse_rbac_scope_token, parse_sql, permission_requirements_for_statements,
        remap_family_file_prefix, split_request_path_and_query, split_sql_statements,
        upsert_user_grant,
    };

    #[test]
    fn parse_migration_filename_works() {
        let parsed = parse_migration_file_name("001_init.up.sql")
            .unwrap()
            .expect("expected migration file");
        assert_eq!(parsed.0, 1);
        assert_eq!(parsed.1, "init");
        assert!(parsed.2);

        let parsed_down = parse_migration_file_name("001_init.down.sql")
            .unwrap()
            .expect("expected migration file");
        assert_eq!(parsed_down.0, 1);
        assert_eq!(parsed_down.1, "init");
        assert!(!parsed_down.2);
    }

    #[test]
    fn split_request_path_and_query_works() {
        let (path, query) = split_request_path_and_query("/admin/slowlog?db=app&limit=50");
        assert_eq!(path, "/admin/slowlog");
        assert_eq!(query.get("db").map(String::as_str), Some("app"));
        assert_eq!(query.get("limit").map(String::as_str), Some("50"));
    }

    #[test]
    fn remap_family_file_prefix_works() {
        let mapped = remap_family_file_prefix("rusedb-db-app.tables", "rusedb", "prod").unwrap();
        assert_eq!(mapped, "prod-db-app.tables");
    }

    #[test]
    fn split_sql_batch_handles_strings() {
        let sql = "INSERT INTO t (name) VALUES ('a;bc'); SELECT * FROM t;";
        let stmts = split_sql_statements(sql).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'a;bc'"));
    }

    #[test]
    fn parse_scope_and_actions_works() {
        let db_scope = parse_rbac_scope_token("db:app").unwrap();
        assert!(matches!(
            db_scope,
            RbacScope::Database { database } if database == "app"
        ));
        let table_scope = parse_rbac_scope_token("table:app.users").unwrap();
        assert!(matches!(
            table_scope,
            RbacScope::Table { database, table } if database == "app" && table == "users"
        ));

        let actions = parse_actions_csv("select,write").unwrap();
        assert!(actions.contains(&RbacAction::Select));
        assert!(actions.contains(&RbacAction::Insert));
        assert!(actions.contains(&RbacAction::Update));
        assert!(actions.contains(&RbacAction::Delete));
    }

    #[test]
    fn upsert_grant_merges_actions() {
        let mut user = RbacUser {
            username: "alice".to_string(),
            token: "t1".to_string(),
            grants: Vec::new(),
        };
        let mut select = HashSet::new();
        select.insert(RbacAction::Select);
        let mut insert = HashSet::new();
        insert.insert(RbacAction::Insert);
        let scope = RbacScope::Table {
            database: "app".to_string(),
            table: "users".to_string(),
        };

        upsert_user_grant(&mut user, scope.clone(), select);
        upsert_user_grant(&mut user, scope, insert);
        assert_eq!(user.grants.len(), 1);
        let actions = &user.grants[0].actions;
        assert!(actions.contains(&RbacAction::Select));
        assert!(actions.contains(&RbacAction::Insert));
    }

    #[test]
    fn permission_requirements_follow_use_database() {
        let statements = vec![
            parse_sql("USE app").unwrap(),
            parse_sql("SELECT * FROM users").unwrap(),
            parse_sql("UPDATE users SET id = 1 WHERE id = 2").unwrap(),
        ];
        let requirements = permission_requirements_for_statements(&statements, "default").unwrap();
        assert!(requirements.iter().any(|item| {
            item.action == RbacAction::Select && item.database == "app" && item.table.is_none()
        }));
        assert!(requirements.iter().any(|item| {
            item.action == RbacAction::Select
                && item.database == "app"
                && item.table.as_deref() == Some("users")
        }));
        assert!(requirements.iter().any(|item| {
            item.action == RbacAction::Update
                && item.database == "app"
                && item.table.as_deref() == Some("users")
        }));
    }

    #[test]
    fn missing_requirement_detected() {
        let user = AuthenticatedUser {
            username: "bob".to_string(),
            grants: vec![RbacGrant {
                scope: RbacScope::Database {
                    database: "app".to_string(),
                },
                actions: [RbacAction::Select].into_iter().collect(),
            }],
        };
        let requirements = vec![
            PermissionRequirement {
                action: RbacAction::Select,
                database: "app".to_string(),
                table: Some("users".to_string()),
            },
            PermissionRequirement {
                action: RbacAction::Delete,
                database: "app".to_string(),
                table: Some("users".to_string()),
            },
        ];
        let missing = first_missing_requirement(&user, &requirements).unwrap();
        assert_eq!(missing.action, RbacAction::Delete);
    }
}
