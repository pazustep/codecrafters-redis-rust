use std::collections::HashMap;
use std::time::{Duration, Instant};

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

pub struct Database {
    data: HashMap<Vec<u8>, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: Vec<u8>) -> Option<Vec<u8>> {
        match self.data.get(&key) {
            Some(entry) if entry.is_expired() => {
                self.data.remove(&key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expiry: Option<Duration>) {
        let entry = Entry {
            value,
            expiry,
            created_at: Instant::now(),
        };

        self.data.insert(key, entry);
    }
}
