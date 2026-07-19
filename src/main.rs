mod command;
mod kv_store;

use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use kv_store::KvStore;

fn main() -> anyhow::Result<()> {
    let kvstore = KvStore::new("log/store.log".into())?;
    let store = Arc::new(Mutex::new(kvstore));
    let listener = TcpListener::bind("127.0.0.1:8082")?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store_clone = Arc::clone(&store);
                thread::spawn(move || {
                    if let Err(e) = kv_store::handle_client(stream, store_clone) {
                        eprintln!("client error {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("connection failed {}", e);
            }
        }
    }
    Ok(())
}
