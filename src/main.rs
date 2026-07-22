mod command;
mod kv_store;

use std::sync::Arc;

use tokio::{net::TcpListener, sync::Mutex};

use kv_store::KvStore;

use crate::kv_store::handle_client;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let kvstore = KvStore::new("log/store.log".into())?;
    let store_state = Arc::new(Mutex::new(kvstore));
    let listener: tokio::net::TcpListener = TcpListener::bind("127.0.0.1:8082").await?;
    loop {
        let store = Arc::clone(&store_state);
        let (stream, _) = listener.accept().await?;
        tokio::task::spawn(async move {
            if let Err(e) = handle_client(stream, store).await {
                println!("error handling client {:?}", e);
            }
        });
    }
    Ok(())
}
