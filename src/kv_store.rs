use anyhow::Ok;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
};



use crate::command::Command;
#[derive(Debug)]
pub struct KvStore {
    pub map: HashMap<String, String>,
    pub path: PathBuf,
    pub writer: BufWriter<File>,
    pub total_writes: usize,
}


impl KvStore {
    pub fn compacter(&mut self) -> Result<(), anyhow::Error> {
        let log_path = self.path.with_extension("log.tmp");
        let new_log = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("log.tmp")?;
        let mut new_writer = BufWriter::new(new_log);
        for (k, v) in self.map.iter() {
            let cmd = Command::Set {
                key: k.clone(),
                value: v.clone(),
            };

            let command = serde_json::to_string(&cmd)? + "\n";
            new_writer.write_all(command.as_bytes())?;
            new_writer.flush()?;
        }

        std::fs::rename(&log_path, &self.path)?;

        let new_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(new_file);
        self.total_writes = 0;
        Ok(())
    }
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
            total_writes: 0,
        })
    }
    pub fn set(&mut self, key: String, value: String) -> Result<(), anyhow::Error> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let json_line = serde_json::to_string(&cmd)? + "\n";
        self.writer.write_all(json_line.as_bytes())?;
        self.total_writes += 1;
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
}
