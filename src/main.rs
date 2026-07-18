mod command;
mod kv_store;

use std::io::{self, BufRead, Write};

use command::Command;
use kv_store::KvStore;

fn main() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut kvstore = KvStore::new("log/store.log".into())?;
    loop {
        print!("=> ");
        io::stdout().flush()?;
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        match Command::read_from_line(&line) {
            Err(e) => {
                print!("{}", e);
                continue;
            }
            Ok(Command::Get { key }) => match kvstore.get(&key) {
                None => println!("=> nil"),
                Some(value) => println!("=> {}", value),
            },
            Ok(Command::Set { key, value }) => kvstore.set(key, value)?,
            Ok(Command::Delete { key }) => kvstore.delete(key)?,
            Ok(Command::Exit) => kvstore.exit(),
        }
    }
    Ok(())
}
