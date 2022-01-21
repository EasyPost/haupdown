use std::borrow::Cow;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{self, Arg};
use log::{debug, error, info, warn};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UnixListener;

mod state;

use state::UpDownState;

const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(10);

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

#[derive(Debug, Error)]
enum AdminClientError {
    #[error("I/O error reading/writing from client: {0})")]
    Io(#[from] tokio::io::Error),
    #[error("sqlite error doing admin work in database: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("system error determing peer of UNIX domain socket")]
    UnableToDeterminePeerCreds,
}

#[derive(Debug, Error)]
enum AdminCommandError {
    #[error("I/O error executing command: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("Invalid command {command}")]
    InvalidCommand { command: String },
    #[error("Incomplete command")]
    IncompleteCommand,
    #[error("User requested help instead of a command")]
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
                Ok(Some(g)) => g.mem.iter().any(|u| u == &username),
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
                    .write_all("ERROR incomplete command\n".as_bytes())
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
                    "ERROR supported commands: ping, show-all, up SERVICENAME, down SERVICENAME, status SERVICENAME, quit, help\n".as_bytes())
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
) {
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

async fn handle_tcp_client_wrapper(socket: tokio::net::TcpStream, state: Arc<UpDownState>) {
    match tokio::time::timeout(TCP_TIMEOUT, handle_tcp_client(socket, state)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => error!("error handling tcp client: {:?}", e),
        Err(d) => warn!("timeout handling tcp client: {:?}", d),
    }
}

async fn healthcheck_loop(state: Arc<UpDownState>) {
    let mut healthcheck_timer = tokio::time::interval(HEALTHCHECK_INTERVAL);
    healthcheck_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        healthcheck_timer.tick().await;
        if let Err(e) = state.update_healthcheck().await {
            warn!("Error updating healthcheck: {:?}", e);
        }
    }
}

async fn tcp_loop(socket: TcpListener, state: Arc<UpDownState>) -> Result<(), tokio::io::Error> {
    loop {
        let (client, _) = socket.accept().await?;
        tokio::spawn(handle_tcp_client_wrapper(client, Arc::clone(&state)));
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LogDest {
    Syslog,
    Stderr,
}

#[derive(Debug, Error)]
enum LogError {
    #[error("Error configuring syslog: {0}")]
    Syslog(#[from] syslog::Error),
}

fn init_logging(matches: &clap::ArgMatches) -> Result<(), LogError> {
    let log_dest = match (
        matches.is_present("stderr"),
        matches.is_present("syslog"),
        std::env::var("LOG_DGRAM_SYSLOG").is_ok(),
        atty::is(atty::Stream::Stderr),
    ) {
        (true, false, _, _) => LogDest::Stderr,
        (false, true, _, _) => LogDest::Syslog,
        (_, _, true, _) => LogDest::Syslog,
        (_, _, false, true) => LogDest::Stderr,
        (_, _, false, false) => LogDest::Stderr,
    };
    let log_level: log::LevelFilter = matches.value_of_t_or_exit("log-level");
    if log_dest == LogDest::Syslog {
        if let Ok(log_path) = std::env::var("LOG_DGRAM_SYSLOG") {
            syslog::init_unix_custom(syslog::Facility::LOG_DAEMON, log_level, log_path)?;
        } else {
            syslog::init_unix(syslog::Facility::LOG_DAEMON, log_level)?;
        };
    } else {
        env_logger::Builder::new()
            .filter_level(log_level)
            .default_format()
            .format_timestamp_millis()
            .target(env_logger::Target::Stderr)
            .init();
    }
    Ok(())
}

fn cli() -> clap::App<'static> {
    clap::App::new(clap::crate_name!())
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about(clap::crate_description!())
        .arg(
            Arg::new("socket_bind_path")
                .short('s')
                .long("socket-bind-path")
                .value_name("PATH")
                .help("Path to bind UNIX domain socket at")
                .required(true)
                .env("SOCKET_BIND_PATH")
                .takes_value(true),
        )
        .arg(
            Arg::new("socket_mode")
                .long("socket-mode")
                .value_name("OCTAL_MODE")
                .help("Mode for unix domain socket")
                .env("SOCKET_MODE")
                .default_value("666")
                .takes_value(true),
        )
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .env("PORT")
                .value_name("TCP_PORT")
                .help("TCP port to bind to for the agent protocol")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("bind-host")
               .short('B')
               .long("bind-host")
               .env("BIND_HOST")
               .default_value("::")
               .multiple_occurrences(true)
               .takes_value(true)
               .help("IP address or hostname to bind for the agent protocol. May be repeated.")
        )
        .arg(
            Arg::new("global_down_file")
                .short('G')
                .long("global-down-file")
                .env("GLOBAL_DOWN_FILE")
                .value_name("PATH")
                .help("Filename which, if it exists, will have all services return 'down'")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::new("db_path")
                .short('d')
                .long("db-path")
                .env("DB_PATH")
                .value_name("PATH")
                .help("Path for state database")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("required_groups")
                .short('g')
                .long("required-groups")
                .multiple_values(true)
                .require_delimiter(true)
                .use_delimiter(true)
                .env("REQUIRED_GROUPS")
                .help(
                    "Require the admin client to be a member of one of these UNIX groups to mutate. Comma-separated list.",
                )
                .takes_value(true),
        )
        .help_heading("LOGGING OPTIONS")
        .arg(
            Arg::new("stderr")
                .short('E')
                .long("stderr")
                .conflicts_with("syslog")
                .help("Force logging to stderr")
        )
        .arg(
            Arg::new("syslog")
                .short('Y')
                .long("syslog")
                .conflicts_with("stderr")
                .help("Force logging to syslog")
        )
        .arg(
            Arg::new("log-level")
                .short('l')
                .long("log-level")
                .possible_values(&["trace", "debug", "info", "warn", "error"])
                .default_value("info")
                .takes_value(true)
                .help("Log level")
        )
}

#[tokio::main(worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();

    init_logging(&matches)
        .map_err(|e| anyhow::anyhow!("non-sync log error: {:?}", e))
        .context("unable to initialize logging")?;

    let state = Arc::new(
        UpDownState::try_new(
            matches.value_of_t_or_exit::<std::path::PathBuf>("db_path"),
            matches.value_of("global_down_file"),
        )
        .context("unable to initialize state database")?,
    );

    let port: u16 = matches.value_of_t_or_exit("port");

    let required_groups = Arc::new(
        matches
            .values_of("required_groups")
            .map(|v| v.map(|s| s.to_owned()).collect())
            .unwrap_or_else(Vec::new),
    );

    let addresses: Vec<std::net::IpAddr> = matches.values_of_t_or_exit("bind-host");

    for address in addresses {
        info!("binding on {:?}:{}", address, port);

        let tcp_socket = TcpListener::bind((address, port))
            .await
            .with_context(|| format!("binding TCP socket on {:?}:{}", address, port))?;
        tokio::spawn(tcp_loop(tcp_socket, Arc::clone(&state)));
    }

    let admin_socket = bind_unix_listener(
        matches.value_of_t_or_exit::<std::path::PathBuf>("socket_bind_path"),
        u32::from_str_radix(matches.value_of("socket_mode").unwrap(), 8)
            .context("parsing --socket-mode as a valid octal mode")?,
    )
    .context("unable to bind UNIX listener")?;

    tokio::spawn(healthcheck_loop(Arc::clone(&state)));

    while let Ok((client, _)) = admin_socket.accept().await {
        tokio::spawn(handle_admin_client_wrapper(
            client,
            Arc::clone(&state),
            Arc::clone(&required_groups),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::cli;

    #[test]
    fn test_cli_debug_assert() {
        cli().debug_assert();
    }
}
