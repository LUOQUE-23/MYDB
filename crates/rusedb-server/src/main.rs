use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
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
use rusedb_sql::parse_sql;
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
    println!("  GET  /openapi.json");
    println!("  GET  /docs");
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
    let path = request
        .path
        .split('?')
        .next()
        .unwrap_or(request.path.as_str());
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

    match path {
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
            if let Some(expected_token) = runtime.bearer_token.as_deref() {
                let provided = request
                    .headers
                    .get("authorization")
                    .and_then(|value| parse_bearer_token(value));
                if provided != Some(expected_token) {
                    let mut headers = cors.clone();
                    headers.push((
                        "WWW-Authenticate".to_string(),
                        "Bearer realm=\"rusedb-http\"".to_string(),
                    ));
                    return write_http_json_response_with_headers(
                        &mut stream,
                        401,
                        &HttpErrorResponse {
                            ok: false,
                            error: "unauthorized".to_string(),
                        },
                        &headers,
                    );
                }
            }
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

            let mut results = Vec::with_capacity(statements.len());
            let guard = runtime
                .engine
                .lock()
                .map_err(|_| RuseDbError::Corruption("engine lock poisoned".to_string()))?;
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
            "Authorization, Content-Type".to_string(),
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
    println!(
        "  rusedb http <catalog_base> [host:port] [--token <token>] [--allow-origin <origin>]"
    );
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
