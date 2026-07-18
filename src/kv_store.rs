use anyhow::Ok;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    process,
};

use crate::command::Command;
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
    path: PathBuf,
    writer: BufWriter<File>,
}

impl KvStore {
    pub fn new(log_path: PathBuf) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)?;
        let mut map = HashMap::new();
        let reader = BufReader::new(&file);
        {
            for line in reader.lines() {
                let line = line?;
                if line.is_empty() {
                    continue;
                }

                let command = serde_json::from_str(&line)?;
                match command {
                    Command::Set { key, value } => {
                        map.insert(key, value);
                    }
                    Command::Delete { key } => {
                        map.remove(&key);
                    }
                    _ => {}
                }
            }
        }
        let buf_writer = BufWriter::new(file);
        Ok(KvStore {
            map,
            path: log_path,
            writer: buf_writer,
        })
    }
    pub fn set(&mut self, key: String, value: String) -> Result<(), anyhow::Error> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let json_line = serde_json::to_string(&cmd)? + "\n";
        self.writer.write_all(json_line.as_bytes())?;
        self.writer.flush()?;
        self.map.insert(key, value);
        Ok(())
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }
    pub fn delete(&mut self, key: String) -> Result<(), anyhow::Error> {
        let cmd = Command::Delete { key: key.clone() };
        let json_line = serde_json::to_string(&cmd)? + "\n";
        self.writer.write_all(json_line.as_bytes())?;
        self.writer.flush()?;
        self.map.remove(&key);
        Ok(())
    }
    pub fn exit(&self) {
        process::exit(0);
    }
}
