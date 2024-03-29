use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Duration;

use log::{debug, error, info};
use rusqlite::OptionalExtension;
use serde::Serialize;
use tokio::sync::Mutex;

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64
}

struct UpDownStateInner {
    conn: rusqlite::Connection,
    downed_services: HashMap<String, ServiceDownStatus>,
    global_down_path: Option<Box<Path>>,
    last_transaction_succeeded: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceDownStatus {
    downed_by: String,
    downed_at: i64,
}

enum State {
    Up,
    Down,
}

impl UpDownStateInner {
    pub fn try_new<P: AsRef<Path>>(
        path: P,
        global_down_path: Option<Box<Path>>,
    ) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS down_services(
                servicename TEXT NOT NULL PRIMARY KEY,
                downed_by TEXT NOT NULL,
                downed_at INT NOT NULL
            )",
            rusqlite::params![],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS healthcheck(
                last_touched_at INT NOT NULL
            )",
            rusqlite::params![],
        )?;
        let downed_services = {
            let mut stmt =
                conn.prepare("SELECT servicename, downed_by, downed_at FROM down_services")?;
            let service_iter = stmt.query_map(rusqlite::params![], |row| {
                Ok((
                    row.get(0)?,
                    ServiceDownStatus {
                        downed_by: row.get(1)?,
                        downed_at: row.get(2)?,
                    },
                ))
            })?;
            service_iter.collect::<Result<HashMap<String, _>, _>>()?
        };
        debug!(
            "discovered {} downed services in the db",
            downed_services.len()
        );
        let mut s = UpDownStateInner {
            conn,
            downed_services,
            global_down_path,
            last_transaction_succeeded: true,
        };
        if let Err(e) = s.update_healthcheck() {
            error!("Error initializing healthcheck: {:?}", e);
        }
        Ok(s)
    }

    fn update_healthcheck(&mut self) -> Result<(), rusqlite::Error> {
        debug!("running healthcheck");
        match self.update_healthcheck_inner() {
            Ok(_) => {
                self.last_transaction_succeeded = true;
                Ok(())
            }
            Err(e) => {
                error!("Error interacting with sqlite: {:?}; marking all services as down until next successful transaction", e);
                self.last_transaction_succeeded = false;
                Err(e)
            }
        }
    }

    fn update_healthcheck_inner(&mut self) -> Result<(), rusqlite::Error> {
        let mut transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)?;
        transaction.set_drop_behavior(rusqlite::DropBehavior::Rollback);
        {
            let mut stmt =
                transaction.prepare("SELECT last_touched_at FROM healthcheck LIMIT 1")?;
            let exists = stmt.query_map(rusqlite::params![], |_| Ok(()))?.count();
            let mut stmt = if exists == 0 {
                transaction.prepare("INSERT INTO healthcheck(last_touched_at) VALUES(?)")?
            } else {
                transaction.prepare("UPDATE healthcheck SET last_touched_at=?")?
            };
            stmt.execute(rusqlite::params![current_unix_timestamp()])?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn set_state<SN, UN>(
        &mut self,
        servicename: SN,
        downed_by: UN,
        target_state: State,
    ) -> Result<(), rusqlite::Error>
    where
        SN: Into<String>,
        UN: Into<String>,
    {
        match self.set_state_inner(servicename.into(), downed_by.into(), target_state) {
            Ok(true) => {
                self.last_transaction_succeeded = true;
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => {
                error!("Error interacting with sqlite: {:?}; marking all services as down until next successful transaction", e);
                self.last_transaction_succeeded = false;
                Err(e)
            }
        }
    }

    fn set_state_inner(
        &mut self,
        servicename: String,
        downed_by: String,
        target_state: State,
    ) -> Result<bool, rusqlite::Error> {
        let mut transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)?;
        transaction.set_drop_behavior(rusqlite::DropBehavior::Rollback);
        let previously_downed_by: Option<String> = {
            let mut stmt =
                transaction.prepare("SELECT downed_by FROM down_services WHERE servicename=?")?;
            stmt.query_row(rusqlite::params![&servicename], |row| row.get(0))
                .optional()?
        };
        let current_state = if previously_downed_by.is_some() {
            State::Down
        } else {
            State::Up
        };
        match (target_state, current_state) {
            (State::Up, State::Up) => return Ok(false),
            (State::Down, State::Down) => return Ok(false),
            (State::Down, State::Up) => {
                info!("marking {} as downed by {}", servicename, downed_by);
                let mut stmt = transaction.prepare(
                    "INSERT INTO down_services(servicename, downed_by, downed_at) VALUES (?, ?, ?)",
                )?;
                let now = current_unix_timestamp();
                stmt.execute(rusqlite::params![&servicename, &downed_by, now])?;
                self.downed_services.insert(
                    servicename,
                    ServiceDownStatus {
                        downed_by,
                        downed_at: now,
                    },
                );
            }
            (State::Up, State::Down) => {
                info!("marking {} as upped by {}", servicename, downed_by);
                let mut stmt =
                    transaction.prepare("DELETE FROM down_services WHERE servicename=?")?;
                stmt.execute(rusqlite::params![&servicename,])?;
                self.downed_services.remove(&servicename);
            }
        }
        transaction.commit()?;
        Ok(true)
    }

    fn all_downed(&self) -> &HashMap<String, ServiceDownStatus> {
        &self.downed_services
    }

    fn is_up<SN: AsRef<str>>(&self, servicename: SN) -> bool {
        let servicename = servicename.as_ref();
        self.is_healthy()
            && !(self.downed_services.contains_key("all")
                || self.downed_services.contains_key(servicename)
                || self.is_global_down())
    }

    fn is_healthy(&self) -> bool {
        self.last_transaction_succeeded
    }

    fn is_global_down(&self) -> bool {
        self.global_down_path
            .as_ref()
            .map(|p| p.exists())
            .unwrap_or(false)
    }

    fn global_down_status(&self) -> Option<ServiceDownStatus> {
        self.global_down_path
            .as_ref()
            .and_then(|p| p.metadata().ok())
            .map(|metadata| {
                let downed_by =
                    match nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(metadata.uid())) {
                        Ok(Some(u)) => u.name,
                        _ => metadata.uid().to_string(),
                    };
                ServiceDownStatus {
                    downed_by,
                    downed_at: metadata.mtime(),
                }
            })
    }
}

pub struct UpDownState {
    inner: Mutex<UpDownStateInner>,
}

impl UpDownState {
    pub fn try_new<P: AsRef<Path>, G: AsRef<Path>>(
        path: P,
        global_down_path: Option<G>,
    ) -> Result<Self, rusqlite::Error> {
        let inner = UpDownStateInner::try_new(path, global_down_path.map(|p| p.as_ref().into()))?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub async fn update_healthcheck(&self) -> Result<(), rusqlite::Error> {
        let mut inner = self.inner.lock().await;
        inner.update_healthcheck()
    }

    pub async fn is_up(&self, servicename: &str) -> bool {
        let inner = self.inner.lock().await;
        inner.is_up(servicename)
    }

    pub async fn set_down<SN: Into<String>, UN: Into<String>>(
        &self,
        servicename: SN,
        username: UN,
    ) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let username = username.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, username, State::Down)?;
        Ok(())
    }

    pub async fn set_up<SN: Into<String>, UN: Into<String>>(
        &self,
        servicename: SN,
        username: UN,
    ) -> Result<(), rusqlite::Error> {
        let servicename = servicename.into();
        let username = username.into();
        let mut inner = self.inner.lock().await;
        inner.set_state(servicename, username, State::Up)?;
        Ok(())
    }

    pub async fn all_downed(&self) -> HashMap<String, ServiceDownStatus> {
        let inner = self.inner.lock().await;
        let mut res = inner.all_downed().clone();
        if let Some(s) = inner.global_down_status() {
            res.insert("(global-down-file)".to_owned(), s);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use super::current_unix_timestamp;
    use super::State;
    use super::UpDownStateInner;

    #[test]
    fn test_current_unix_timestamp() {
        let first = current_unix_timestamp();
        assert!(first > 1585592412);
        assert!(first < 2216312412);
        // long enough for a leap second to not break this
        sleep(Duration::from_secs(2));
        let second = current_unix_timestamp();
        assert!(second > first);
    }

    #[test]
    fn test_state_inner_basic() {
        let dir = tempfile::Builder::new()
            .prefix("haupdown-test")
            .tempdir()
            .unwrap();
        let db_path = dir.path().join("db.sqlite");
        let mut state = UpDownStateInner::try_new(&db_path, None).unwrap();
        let username = "some-user".to_string();

        // down and up
        assert_eq!(state.is_up("foo"), true);
        state
            .set_state("foo", username.clone(), State::Down)
            .expect("should be able to set state");
        assert_eq!(state.is_up("foo"), false);
        assert_eq!(state.all_downed().keys().collect::<Vec<_>>(), vec!["foo"]);
        state
            .set_state("foo", username.clone(), State::Up)
            .expect("should be able to set state");
        assert_eq!(state.is_up("foo"), true);

        // "all"
        assert_eq!(state.is_up("foo"), true);
        state
            .set_state("all", username.clone(), State::Down)
            .expect("should be able to set state");
        assert_eq!(state.is_up("foo"), false);
        state
            .set_state("all", username.clone(), State::Up)
            .expect("should be able to set state");

        // persists between invocations
        state
            .set_state("foo", username.clone(), State::Down)
            .expect("should be able to set state");
        assert_eq!(state.is_up("foo"), false);
        let state = UpDownStateInner::try_new(&db_path, None).unwrap();
        assert_eq!(state.is_up("foo"), false);
    }

    #[test]
    fn test_global_down_path() {
        let dir = tempfile::Builder::new()
            .prefix("haupdown-test")
            .tempdir()
            .unwrap();
        let db_path = dir.path().join("db.sqlite");
        let global_path = dir.path().join("global_updown").into_boxed_path();
        let state = UpDownStateInner::try_new(&db_path, Some(global_path.clone())).unwrap();

        assert_eq!(state.is_up("foo"), true);

        std::fs::write(&global_path, "downed by init").unwrap();

        assert_eq!(state.is_up("foo"), false);

        std::fs::remove_file(&global_path).unwrap();

        assert_eq!(state.is_up("foo"), true);
    }
}
