use anyhow::Ok;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
    path::PathBuf,
    process,
    sync::{Arc, Mutex},
};

use crate::command::Command;
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
    path: PathBuf,
    writer: BufWriter<File>,
}

pub fn handle_client(stream: TcpStream, locked_store: Arc<Mutex<KvStore>>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);

    loop {
        let mut line = String::new();

        match reader.read_line(&mut line) {
            std::result::Result::Ok(0) => {
                println!("client disconnected");
                break;
            }

            std::result::Result::Ok(_) => match Command::read_from_line(&line.trim()) {
                Err(e) => {
                    let response_stream = reader.get_mut();
                    writeln!(response_stream, "{}", e)?;
                    response_stream.flush()?;
                    continue;
                }

                std::result::Result::Ok(Command::Set { key, value }) => {
                    let response_stream = reader.get_mut();
                    let mut locked_gaurd = locked_store.lock().unwrap();
                    locked_gaurd.set(key, value)?;
                    writeln!(response_stream, "OK")?;
                    response_stream.flush()?;
                }
                std::result::Result::Ok(Command::Get { key }) => {
                    let response_stream = reader.get_mut();
                    let locked_gaurd = locked_store.lock().unwrap();
                    match locked_gaurd.get(&key) {
                        Some(value) => writeln!(response_stream, "value : {}", value)?,
                        None => writeln!(response_stream, "nil")?,
                    }
                    response_stream.flush()?;
                }
                std::result::Result::Ok(Command::Delete { key }) => {
                    let response_stream = reader.get_mut();
                    let mut locked_gaurd = locked_store.lock().unwrap();
                    locked_gaurd.delete(key)?;
                    writeln!(response_stream, "Ok")?;
                    response_stream.flush()?;
                }
                std::result::Result::Ok(Command::Exit) => {
                    let response_stream = reader.get_mut();
                    writeln!(response_stream, "Goodbye")?;
                    response_stream.flush()?;
                    break;
                }
            },

            Err(e) => {
                let response_stream = reader.get_mut();
                writeln!(response_stream, "Error {}", e)?;
                response_stream.flush()?;
                break;
            }
        }
    }
    Ok(())
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
