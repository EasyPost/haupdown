use std::borrow::Cow;
use std::collections::HashSet;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::{self, Arg};
use derive_more::{Display, Error, From};
use log::{debug, error, info, warn};
use rusqlite::OptionalExtension;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UnixListener;
use tokio::sync::Mutex;

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64
}

struct UpDownStateInner {
    conn: rusqlite::Connection,
    downed_services: HashSet<String>,
}

impl UpDownStateInner {
    fn set_state(
        &mut self,
        servicename: String,
        downed_by: String,
        mark_down: bool,
    ) -> Result<(), rusqlite::Error> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)?;
        let previously_downed_by: Option<String> = {
            let mut stmt =
                transaction.prepare("SELECT * FROM down_services WHERE servicename=?")?;
            stmt.query_row(rusqlite::params![&servicename], |row| row.get(1))
                .optional()?
        };
        let currently_down = previously_downed_by.is_some();
        match (mark_down, currently_down) {
            (true, true) => {}
            (true, false) => {
                info!("marking {} as downed by {}", servicename, downed_by);
                let mut stmt = transaction.prepare(
                    "INSERT INTO down_services(servicename, downed_by, downed_at) VALUES (?, ?, ?)",
                )?;
                stmt.execute(rusqlite::params![
                    &servicename,
                    &downed_by,
                    current_unix_timestamp()
                ])?;
                self.downed_services.insert(servicename);
            }
            (false, true) => {
                info!("marking {} as upped by {}", servicename, downed_by);
                let mut stmt =
                    transaction.prepare("DELETE FROM down_services WHERE servicename=?")?;
                stmt.execute(rusqlite::params![&servicename,])?;
                self.downed_services.remove(&servicename);
            }
            (false, false) => {}
        }
        transaction.commit()
    }

    fn all_downed(&self) -> &HashSet<String> {
        &self.downed_services
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
                servicename TEXT NOT NULL PRIMARY KEY,
                downed_by TEXT NOT NULL,
                downed_at INT NOT NULL
            )",
            rusqlite::params![],
        )?;
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
        !(inner.downed_services.contains("all") || inner.downed_services.contains(servicename))
    }

    async fn set_down<SN: Into<String>, UN: Into<String>>(
        &self,
        servicename: SN,
        username: UN,
    ) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let username = username.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, username, true)?;
        Ok(())
    }

    async fn set_up<SN: Into<String>, UN: Into<String>>(
        &self,
        servicename: SN,
        username: UN,
    ) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let username = username.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, username, false)?;
        Ok(())
    }

    async fn all_downed(&self) -> Vec<String> {
        let inner = self.inner.lock().await;
        let mut v: Vec<String> = inner.all_downed().iter().map(|s| s.clone()).collect();
        v.sort();
        v
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
    std::fs::set_permissions(&path_buf, std::fs::Permissions::from_mode(0o666))?;
    std::fs::rename(path_buf, orig_path_buf)?;
    Ok(listener)
}

#[derive(Debug, Display, Error, From)]
enum AdminClientError {
    Io(tokio::io::Error),
    Db(rusqlite::Error),
    UnableToDeterminePeerCreds,
}

#[derive(Debug, Display, Error, From)]
enum AdminCommandError {
    Io(tokio::io::Error),
    InvalidCommand { command: String },
    IncompleteCommand,
}

enum AdminCommand {
    Ping,
    ShowAll,
    Quit,
    Up { servicename: String },
    Down { servicename: String },
    Status { servicename: String },
}

async fn read_command<B: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut B,
) -> Result<Option<AdminCommand>, AdminCommandError> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let mut parts = line.split_whitespace();
    let first = match parts.next() {
        Some(f) => f,
        None => return Ok(None),
    };
    match first.to_lowercase().as_str() {
        "ping" => Ok(Some(AdminCommand::Ping)),
        "show-all" => Ok(Some(AdminCommand::ShowAll)),
        "show_all" => Ok(Some(AdminCommand::ShowAll)),
        "showall" => Ok(Some(AdminCommand::ShowAll)),
        "quit" => Ok(Some(AdminCommand::Quit)),
        "exit" => Ok(Some(AdminCommand::Quit)),
        "up" => match parts.next() {
            Some(servicename) => Ok(Some(AdminCommand::Up {
                servicename: servicename.to_owned(),
            })),
            None => Err(AdminCommandError::IncompleteCommand),
        },
        "down" => match parts.next() {
            Some(servicename) => Ok(Some(AdminCommand::Down {
                servicename: servicename.to_owned(),
            })),
            None => Err(AdminCommandError::IncompleteCommand),
        },
        "status" => match parts.next() {
            Some(servicename) => Ok(Some(AdminCommand::Status {
                servicename: servicename.to_owned(),
            })),
            None => Err(AdminCommandError::IncompleteCommand),
        },
        other => Err(AdminCommandError::InvalidCommand {
            command: other.to_owned(),
        }),
    }
}

async fn handle_admin_client(
    mut socket: tokio::net::UnixStream,
    state: Arc<UpDownState>,
    required_groups: Arc<Vec<String>>,
) -> Result<(), AdminClientError> {
    let peer = socket.peer_cred()?;
    let username = match nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(peer.uid)) {
        Ok(Some(u)) => u.name,
        Ok(None) => return Err(AdminClientError::UnableToDeterminePeerCreds),
        Err(_) => return Err(AdminClientError::UnableToDeterminePeerCreds),
    };
    let is_authorized_to_write = if required_groups.is_empty() {
        true
    } else {
        required_groups
            .iter()
            .any(|group| match nix::unistd::Group::from_name(group) {
                Ok(Some(g)) => g.mem.iter().any(|u| u.as_ref() == username),
                _ => false,
            })
    };
    debug!(
        "accepted connection from {}; is_authorized_to_write={}",
        username, is_authorized_to_write
    );
    let (reader, mut writer) = socket.split();
    let mut buffered_reader = tokio::io::BufReader::new(reader);
    loop {
        let command = match read_command(&mut buffered_reader).await {
            Ok(Some(c)) => c,
            Ok(None) => break,
            Err(AdminCommandError::IncompleteCommand) => {
                writer
                    .write_all(format!("ERROR incomplete command\n").as_bytes())
                    .await?;
                continue;
            }
            Err(AdminCommandError::InvalidCommand { command }) => {
                writer
                    .write_all(format!("ERROR unknown command {}\n", command).as_bytes())
                    .await?;
                continue;
            }
            Err(AdminCommandError::Io(e)) => return Err(AdminClientError::Io(e)),
        };
        let response = match command {
            AdminCommand::Ping => Cow::Borrowed("pong"),
            AdminCommand::Up { servicename } => {
                if is_authorized_to_write {
                    match state.set_up(servicename, username.clone()).await {
                        Ok(_) => Cow::Borrowed("ok"),
                        Err(e) => Cow::Owned(format!("ERROR unable to up service: {:?}", e)),
                    }
                } else {
                    Cow::Borrowed("ERROR not authorized to write")
                }
            }
            AdminCommand::Down { servicename } => {
                if is_authorized_to_write {
                    match state.set_down(servicename, username.clone()).await {
                        Ok(_) => Cow::Borrowed("ok"),
                        Err(e) => Cow::Owned(format!("ERROR unable to down service: {:?}", e)),
                    }
                } else {
                    Cow::Borrowed("ERROR not authorized to write")
                }
            }
            AdminCommand::Status { servicename } => {
                if state.is_up(&servicename).await {
                    Cow::Borrowed("up")
                } else {
                    Cow::Borrowed("down")
                }
            }
            AdminCommand::ShowAll => {
                let all_downed = state.all_downed().await;
                Cow::Owned(all_downed.join(","))
            }
            AdminCommand::Quit => break,
        };
        writer.write_all(response.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await?;
    writer.shutdown().await?;
    Ok(())
}

async fn handle_admin_client_wrapper(
    socket: tokio::net::UnixStream,
    state: Arc<UpDownState>,
    required_groups: Arc<Vec<String>>,
) -> () {
    match tokio::time::timeout(
        Duration::from_secs(10),
        handle_admin_client(socket, state, required_groups),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => error!("error handling admin client: {:?}", e),
        Err(d) => warn!("timeout handling admin client: {:?}", d),
    }
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

fn init_logging() {
    if let Ok(log_path) = std::env::var("LOG_DGRAM_SYSLOG") {
        syslog::init_unix_custom(
            syslog::Facility::LOG_DAEMON,
            std::env::var("LOG_LEVEL")
                .unwrap_or("info".to_owned())
                .parse()
                .expect("could not parse $LOG_LEVEL as a syslog level"),
            log_path,
        )
        .expect("could not initialize syslog");
    } else {
        env_logger::init();
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
        .arg(
            Arg::with_name("required_groups")
                .short("g")
                .long("required-groups")
                .multiple(true)
                .help(
                    "Require the admin client to be a member of one of these UNIX groups to mutate",
                )
                .takes_value(true),
        )
        .get_matches();

    init_logging();

    let state = Arc::new(
        UpDownState::try_new(matches.value_of("db_path").unwrap())
            .expect("Could not initialize state database"),
    );

    let port: u16 = std::env::var("PORT")
        .expect("must set $PORT")
        .parse()
        .expect("must set $PORT to an int");

    let required_groups = Arc::new(
        matches
            .values_of("required_groups")
            .map(|v| v.map(|s| s.to_owned()).collect())
            .unwrap_or_else(Vec::new),
    );

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
        tokio::spawn(handle_admin_client_wrapper(
            client,
            Arc::clone(&state),
            Arc::clone(&required_groups),
        ));
    }
}
