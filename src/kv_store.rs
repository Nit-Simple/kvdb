use anyhow::Ok;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    str::SplitInclusive,
    sync::Arc,
};

use tokio::sync::Mutex;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::command::Command;
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
    path: PathBuf,
    writer: BufWriter<File>,
    total_writes: usize,
}

pub async fn handle_client(
    mut stream: tokio::net::TcpStream,
    locked_store: Arc<Mutex<KvStore>>,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.split();
    let mut reader = tokio::io::BufReader::new(read_half);

    let mut line = String::new();
    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            std::result::Result::Ok(0) => {
                println!("client disconnected");
                break;
            }

            std::result::Result::Ok(_) => match Command::read_from_line(line.trim()) {
                Err(e) => {
                    let response_stream = format!("error {}\n", e);
                    write_half.write_all(response_stream.as_bytes()).await?;
                    write_half.flush().await?;
                    continue;
                }

                std::result::Result::Ok(Command::Set { key, value }) => {
                    let mut locked_store = locked_store.lock().await;
                    let needs_compact = std::fs::metadata(&locked_store.path)
                        .map(|meta| meta.len() > 1024 * 1024 || locked_store.total_writes >= 10000)
                        .unwrap_or(false);
                    if needs_compact {
                        println!("compaction started");
                        if let Err(e) = locked_store.compacter() {
                            eprintln!("compaction failed {}", e);
                        }
                    }

                    locked_store.set(key, value)?;
                    drop(locked_store);
                    write_half.write_all(b"OK\n").await?;
                    write_half.flush().await?;
                }
                std::result::Result::Ok(Command::Get { key }) => {
                    let locked_store = locked_store.lock().await;
                    match locked_store.get(&key) {
                        std::option::Option::Some(value) => {
                            let response_stream = format!("value : {}\n", value);
                            write_half.write_all(response_stream.as_bytes()).await?;
                        }
                        None => write_half.write_all(b"nil\n").await?,
                    }
                    drop(locked_store);
                    write_half.flush().await?;
                }
                std::result::Result::Ok(Command::Delete { key }) => {
                    let mut locked_store = locked_store.lock().await;
                    locked_store.delete(key)?;
                    drop(locked_store);
                    write_half.write_all(b"OK\n").await?;
                    write_half.flush().await?;
                }
                std::result::Result::Ok(Command::Exit) => {
                    write_half.write_all(b"GoodBye\n").await?;
                    write_half.flush().await?;
                    break;
                }
            },

            Err(e) => {
                let response_stream = format!("Error : {}", e);
                write_half.write_all(response_stream.as_bytes()).await?;
                line.clear();
                write_half.flush().await?;
                break;
            }
        }
    }
    Ok(())
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
