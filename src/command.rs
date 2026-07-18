use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    Exit,
}

impl Command {
    pub fn read_from_line(line: &str) -> Result<Self, String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["SET", key, value] => Ok(Command::Set {
                key: key.to_string(),
                value: value.to_string(),
            }),
            ["GET", key] => Ok(Command::Get {
                key: key.to_string(),
            }),
            ["DELETE", key] => Ok(Command::Delete {
                key: key.to_string(),
            }),
            ["EXIT"] => Ok(Command::Exit),
            _ => Err("Invalid Command".to_string()),
        }
    }
}
