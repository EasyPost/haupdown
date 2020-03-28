use std::collections::HashSet;
use std::error::Error;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::{self, Arg};
use derive_more::Display;
use log::{error, info, warn};
use rusqlite::OptionalExtension;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UnixListener;
use tokio::sync::Mutex;

fn current_unix_timestamp() -> i64 {
    unimplemented!();
}

struct UpDownStateInner {
    conn: rusqlite::Connection,
    downed_services: HashSet<String>,
}

impl UpDownStateInner {
    fn set_state(
        &mut self,
        servicename: String,
        downed_by: Option<String>,
        mark_down: bool,
    ) -> Result<(), rusqlite::Error> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)?;
        let previously_downed_by: Option<String> = {
            let mut stmt =
                transaction.prepare("SELECT * FROM down_services WHERE servicename=?")?;
            stmt.query_row(rusqlite::params![], |row| row.get(1))
                .optional()?
        };
        let currently_down = previously_downed_by.is_some();
        match (mark_down, currently_down) {
            (true, true) => {}
            (true, false) => {
                let mut stmt = transaction.prepare(
                    "INSERT INTO down_services(servicename, downed_by, downed_at) VALUES (?, ?, ?",
                )?;
                stmt.execute(rusqlite::params![
                    &servicename,
                    &downed_by,
                    current_unix_timestamp()
                ])?;
                self.downed_services.insert(servicename);
            }
            (false, true) => {
                let mut stmt =
                    transaction.prepare("DELETE FROM down_services WHERE servicename=?")?;
                stmt.execute(rusqlite::params![&servicename,])?;
                self.downed_services.remove(&servicename);
            }
            (false, false) => {}
        }
        transaction.commit()
    }
}

pub struct UpDownState {
    inner: Mutex<UpDownStateInner>,
}

impl UpDownState {
    pub fn try_new<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS down_services(
                servicename TEXT NOT NULL,
                downed_by TEXT NOT NULL,
                downed_at BIGINT NOT NULL,
                PRIMARY KEY (servicename),
            )",
            rusqlite::params![],
        )?;
        conn.execute("PRAGMA journal_mode=WAL", rusqlite::params![])?;
        conn.execute("PRAGMA wal_mode=TRUNCATE", rusqlite::params![])?;
        let downed_services = {
            let mut stmt = conn.prepare("SELECT servicename FROM down_services")?;
            let service_iter = stmt.query_map(rusqlite::params![], |row| row.get(0))?;
            service_iter.collect::<Result<HashSet<String>, _>>()?
        };
        Ok(UpDownState {
            inner: Mutex::new(UpDownStateInner {
                conn,
                downed_services,
            }),
        })
    }

    async fn is_up(&self, servicename: &str) -> bool {
        let inner = self.inner.lock().await;
        inner.downed_services.contains("all") || inner.downed_services.contains(servicename)
    }

    async fn set_down<SN: Into<String>, UN: Into<String>>(
        &self,
        servicename: SN,
        username: UN,
    ) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let username = username.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, Some(username), true)?;
        Ok(())
    }

    async fn set_up<SN: Into<String>>(&self, servicename: SN) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, None, false)?;
        Ok(())
    }
}

/// Bind a listening UNIX domain socket at /foo/bar by first
/// binding to /foo/bar.pid and atomically renaming to /foo/bar
pub fn bind_unix_listener<P: AsRef<Path>>(path: P) -> tokio::io::Result<UnixListener> {
    let mut path_buf = path.as_ref().to_owned();
    let orig_path_buf = path_buf.clone();
    if let Ok(metadata) = std::fs::metadata(&path_buf) {
        if !metadata.file_type().is_socket() {
            panic!(format!("pre-existing non-socket file at {:?}", path_buf));
        }
    }
    let mut target_file_name = path_buf.file_name().unwrap().to_owned();
    target_file_name.push(format!(".{}", std::process::id()));
    path_buf.set_file_name(target_file_name);
    let listener = UnixListener::bind(&path_buf)?;
    std::fs::rename(path_buf, orig_path_buf)?;
    Ok(listener)
}

#[derive(Debug, Display)]
enum AdminClientError {
    Io(tokio::io::Error),
    Db(rusqlite::Error),
}

impl Error for AdminClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AdminClientError::Io(e) => Some(e),
            AdminClientError::Db(e) => Some(e),
        }
    }
}

impl From<tokio::io::Error> for AdminClientError {
    fn from(e: tokio::io::Error) -> AdminClientError {
        AdminClientError::Io(e)
    }
}

impl From<rusqlite::Error> for AdminClientError {
    fn from(e: rusqlite::Error) -> AdminClientError {
        AdminClientError::Db(e)
    }
}

async fn handle_admin_client(
    socket: tokio::net::UnixStream,
    state: Arc<UpDownState>,
) -> Result<(), AdminClientError> {
    state
        .set_down("all".to_string(), "jbrown".to_string())
        .await?;
    state.set_up("all".to_string()).await?;
    unimplemented!();
}

async fn handle_tcp_client(
    mut socket: tokio::net::TcpStream,
    state: Arc<UpDownState>,
) -> Result<(), tokio::io::Error> {
    let (reader, mut writer) = socket.split();
    let mut buffered_reader = tokio::io::BufReader::new(reader);
    let mut line = String::new();
    buffered_reader.read_line(&mut line).await?;
    let service_name = line.trim_end();
    if state.is_up(service_name).await {
        writer.write_all(b"ready\r\n").await?
    } else {
        writer.write_all(b"maint\r\n").await?
    }
    writer.flush().await?;
    writer.shutdown().await?;
    Ok(())
}

async fn handle_tcp_client_wrapper(socket: tokio::net::TcpStream, state: Arc<UpDownState>) -> () {
    match tokio::time::timeout(Duration::from_secs(10), handle_tcp_client(socket, state)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => error!("error handling tcp client: {:?}", e),
        Err(d) => warn!("timeout handling tcp client: {:?}", d),
    }
}

async fn tcp_loop(
    mut socket: TcpListener,
    state: Arc<UpDownState>,
) -> Result<(), tokio::io::Error> {
    loop {
        let (client, _) = socket.accept().await?;
        tokio::spawn(handle_tcp_client_wrapper(client, Arc::clone(&state)));
    }
}

#[tokio::main]
async fn main() {
    let matches = clap::App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author("EasyPost <oss@easypost.com>")
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("socket_bind_path")
                .short("s")
                .long("socket-bind-path")
                .value_name("PATH")
                .help("Path to bind UNIX domain socket at")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db_path")
                .short("d")
                .long("db-path")
                .value_name("PATH")
                .help("Path for state database")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let state = Arc::new(
        UpDownState::try_new(matches.value_of("db_path").unwrap())
            .expect("Could not initialize state database"),
    );

    let port: u16 = std::env::var("PORT")
        .expect("must set $PORT")
        .parse()
        .expect("must set $PORT to an int");

    let address: std::net::IpAddr = "::".parse().unwrap();

    info!("binding on {:?}:{}", address, port);

    let tcp_socket = TcpListener::bind((address, port))
        .await
        .expect("failed to bind TCP socket");

    let mut admin_socket = bind_unix_listener(matches.value_of("socket_bind_path").unwrap())
        .expect("unable to bind UNIX listener");

    tokio::spawn(tcp_loop(tcp_socket, Arc::clone(&state)));

    loop {
        let (client, _) = match admin_socket.accept().await {
            Ok(c) => c,
            Err(_) => break,
        };
        let handle = Arc::clone(&state);
        tokio::spawn(handle_admin_client(client, handle));
    }
}
