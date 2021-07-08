use std::borrow::Cow;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::{self, Arg};
use derive_more::{Display, Error, From};
use log::{debug, error, info, warn};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UnixListener;

mod state;

use state::UpDownState;

const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_TIMEOUT: Duration = Duration::from_secs(10);

/// Bind a listening UNIX domain socket at /foo/bar by first
/// binding to /foo/bar.pid and atomically renaming to /foo/bar
pub fn bind_unix_listener<P: AsRef<Path>>(path: P, mode: u32) -> tokio::io::Result<UnixListener> {
    let mut path_buf = path.as_ref().to_owned();
    let orig_path_buf = path_buf.clone();
    if let Ok(metadata) = std::fs::metadata(&path_buf) {
        if !metadata.file_type().is_socket() {
            panic!("pre-existing non-socket file at {:?}", path_buf);
        }
    }
    let mut target_file_name = path_buf.file_name().unwrap().to_owned();
    target_file_name.push(format!(".{}", std::process::id()));
    path_buf.set_file_name(target_file_name);
    let listener = UnixListener::bind(&path_buf)?;
    std::fs::set_permissions(&path_buf, std::fs::Permissions::from_mode(mode))?;
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
    HelpRequested,
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
        "help" => Err(AdminCommandError::HelpRequested),
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
    let username = match nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(peer.uid())) {
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
            Err(AdminCommandError::HelpRequested) => {
                writer.write_all(
                    format!("ERROR supported commands: ping, show-all, up SERVICENAME, down SERVICENAME, status SERVICENAME, quit, help\n").as_bytes())
                    .await?;
                continue;
            }
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
                Cow::Owned(serde_json::to_string(&all_downed).unwrap())
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
        ADMIN_TIMEOUT,
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
    match tokio::time::timeout(TCP_TIMEOUT, handle_tcp_client(socket, state)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => error!("error handling tcp client: {:?}", e),
        Err(d) => warn!("timeout handling tcp client: {:?}", d),
    }
}

async fn tcp_loop(socket: TcpListener, state: Arc<UpDownState>) -> Result<(), tokio::io::Error> {
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

#[tokio::main(worker_threads = 2)]
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
                .env("SOCKET_BIND_PATH")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("socket_mode")
                .long("socket-mode")
                .value_name("OCTAL_MODE")
                .help("Mode for unix domain socket")
                .required(true)
                .env("SOCKET_MODE")
                .default_value("666")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .env("PORT")
                .value_name("TCP_PORT")
                .help("TCP port to bind to for the agent protocol")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("global_down_file")
                .short("G")
                .long("global-down-file")
                .env("GLOBAL_DOWN_FILE")
                .value_name("PATH")
                .help("Filename which, if it exists, will have all services return 'down'")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db_path")
                .short("d")
                .long("db-path")
                .env("DB_PATH")
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
                .use_delimiter(true)
                .env("REQUIRED_GROUPS")
                .help(
                    "Require the admin client to be a member of one of these UNIX groups to mutate",
                )
                .takes_value(true),
        )
        .get_matches();

    init_logging();

    let state = Arc::new(
        UpDownState::try_new(
            matches.value_of("db_path").unwrap(),
            matches.value_of("global_down_file"),
        )
        .expect("Could not initialize state database"),
    );

    let port: u16 = matches
        .value_of("port")
        .unwrap()
        .parse()
        .expect("must set --port to a valid port number");

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

    let admin_socket = bind_unix_listener(
        matches.value_of("socket_bind_path").unwrap(),
        u32::from_str_radix(matches.value_of("socket_mode").unwrap(), 8)
            .expect("--socket-mode must be a valid mode"),
    )
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
