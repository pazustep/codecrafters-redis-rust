use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

struct Entry {
    value: Vec<u8>,
    expiry: Option<Duration>,
    created_at: Instant,
}

impl Entry {
    fn is_expired(&self) -> bool {
        match self.expiry {
            Some(expiry) => self.created_at + expiry < Instant::now(),
            None => false,
        }
    }
}

struct Database {
    data: HashMap<Vec<u8>, Entry>,
}

enum DatabaseCommand {
    Get {
        key: Vec<u8>,
        reply_to: oneshot::Sender<Option<Vec<u8>>>,
    },

    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: Option<Duration>,
        reply_to: oneshot::Sender<()>,
    },
}

impl Database {
    fn apply(&mut self, command: DatabaseCommand) {
        match command {
            DatabaseCommand::Get { key, reply_to } => {
                let value = self.get(key);
                reply_to.send(value).unwrap();
            }
            DatabaseCommand::Set {
                key,
                value,
                expiry,
                reply_to,
            } => {
                self.set(key, value, expiry);
                reply_to.send(()).unwrap();
            }
        }
    }

    fn get(&mut self, key: Vec<u8>) -> Option<Vec<u8>> {
        match self.data.get(&key) {
            Some(entry) if entry.is_expired() => {
                self.data.remove(&key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration>) {
        let entry = Entry {
            value,
            expiry,
            created_at: Instant::now(),
        };

        self.data.insert(key, entry);
    }
}

pub fn start() -> DatabaseHandle {
    let (sender, mut receiver) = mpsc::unbounded_channel();

    let task_handle = tokio::spawn(async move {
        let mut database = Database {
            data: HashMap::new(),
        };

        loop {
            match receiver.recv().await {
                Some(command) => database.apply(command),
                None => {
                    println!("database channel closed; exiting database task");
                    break;
                }
            }
        }
    });

    DatabaseHandle { sender }
}

#[derive(Clone)]
pub struct DatabaseHandle {
    sender: mpsc::UnboundedSender<DatabaseCommand>,
}

impl DatabaseHandle {
    pub async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let command = DatabaseCommand::Get { key, reply_to: tx };
        match self.sender.send(command) {
            Ok(()) => rx.await.unwrap(),
            Err(_) => {
                println!("database task terminated before GET command could be sent");
                None
            }
        }
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration>) {
        let (tx, rx) = oneshot::channel();
        let command = DatabaseCommand::Set {
            key,
            value,
            expiry,
            reply_to: tx,
        };

        match self.sender.send(command) {
            Ok(()) => rx.await.unwrap(),
            Err(_) => {
                println!("database task terminated before SET command could be sent");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn get_key_not_found() {
        let db = super::start();
        let value = db.get("key".as_bytes().to_vec()).await;
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn set_and_get_no_expiry() {
        let db = super::start();

        db.set("key".as_bytes().to_vec(), "value".as_bytes().to_vec(), None)
            .await;

        match db.get("key".as_bytes().to_vec()).await {
            Some(value) => assert_eq!(value, "value".as_bytes().to_vec()),
            val => panic!("expected Some(\"value\"), got {:?}", val),
        }
    }

    #[tokio::test]
    async fn set_and_get_expiry() {
        let db = super::start();

        db.set(
            "key".as_bytes().to_vec(),
            "value".as_bytes().to_vec(),
            Some(Duration::from_millis(100)),
        )
        .await;

        match db.get("key".as_bytes().to_vec()).await {
            Some(value) => assert_eq!(value, "value".as_bytes().to_vec()),
            val => panic!("expected Some(value), got {:?}", val),
        };

        tokio::time::sleep(Duration::from_millis(100)).await;

        match db.get("key".as_bytes().to_vec()).await {
            None => {}
            Some(value) => panic!("expected None, got {:?}", value),
        };
    }
}
