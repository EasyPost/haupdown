use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use log::info;
use rusqlite::OptionalExtension;
use serde_derive::Serialize;
use tokio::sync::Mutex;

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64
}

struct UpDownStateInner {
    conn: rusqlite::Connection,
    downed_services: HashMap<String, ServiceDownStatus>,
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
        Ok(UpDownStateInner {
            conn,
            downed_services,
        })
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
        let servicename = servicename.into();
        let downed_by = downed_by.into();
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)?;
        let previously_downed_by: Option<String> = {
            let mut stmt =
                transaction.prepare("SELECT * FROM down_services WHERE servicename=?")?;
            stmt.query_row(rusqlite::params![&servicename], |row| row.get(1))
                .optional()?
        };
        let current_state = if previously_downed_by.is_some() {
            State::Down
        } else {
            State::Up
        };
        match (target_state, current_state) {
            (State::Up, State::Up) => {}
            (State::Down, State::Down) => {}
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
        transaction.commit()
    }

    fn all_downed(&self) -> &HashMap<String, ServiceDownStatus> {
        &self.downed_services
    }

    fn is_up<SN: AsRef<str>>(&self, servicename: SN) -> bool {
        let servicename = servicename.as_ref();
        !(self.downed_services.contains_key("all")
            || self.downed_services.contains_key(servicename))
    }
}

pub struct UpDownState {
    inner: Mutex<UpDownStateInner>,
}

impl UpDownState {
    pub fn try_new<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        let inner = UpDownStateInner::try_new(path)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
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
        inner.all_downed().clone()
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
        let dir = tempdir::TempDir::new("haupdown-test").unwrap();
        let db_path = dir.path().join("db.sqlite");
        let mut state = UpDownStateInner::try_new(&db_path).unwrap();
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
        let state = UpDownStateInner::try_new(&db_path).unwrap();
        assert_eq!(state.is_up("foo"), false);
    }
}
