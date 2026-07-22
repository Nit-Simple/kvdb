use anyhow::Ok;

use std::{
    sync::Arc,
};

use tokio::sync::Mutex;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::command::Command;
use crate::kv_store::KvStore;

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
