use std::{io::Write, path::Path};
use clap::{Parser, Subcommand};

use libactionkv::{ActionKV, ByteString};


#[derive(Parser, Debug)]
#[command(multicall = true)]
struct Args {
    #[command(subcommand)]
    cmd: Commands
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Get {
        key: ByteString,
    },
    Set {
        key: ByteString,
        value: ByteString,
    },
    Del {
        key: ByteString,
    },
    Exit
}

fn readline() -> Result<String, String> {
    print!("$ ");
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut buffer = String::new();
    std::io::stdin()
        .read_line(&mut buffer)
        .map_err(|e| e.to_string())?;
    Ok(buffer)
}

fn process(line: &str, store: &mut ActionKV) -> Result<bool, String> {
    let args = shlex::split(line).ok_or("error: Invalid quoting")?;
    let cli = Args::try_parse_from(args).map_err(|e| e.to_string())?;
    match cli.cmd {
        Commands::Exit => return Ok(true),
        Commands::Get { key } => {
            let result = store.get(&key).map_err(|e| format!("{:?}", e))?;
            println!("{:?}", result);
        },
        Commands::Set { key, value } => {
            store.insert(&key, &value).map_err(|e| format!("{:?}", e))?;
            println!("Data successfully insertes");
        },
        Commands::Del { key } => {
            store.delete(&key).map_err(|e| format!("{:?}", e))?;
            println!("Data successfully deleted");
        }
    }
    Ok(false)
}

fn main() -> Result<(), String> {
    let path = Path::new("storage.bin");
    let mut store = ActionKV::new(path).map_err(|e| format!("{:?}", e))?;
    loop {
        let line = readline()?;
        let line = line.trim();
        if line.is_empty() { break;}
        match process(line, &mut store) {
            Ok(exit ) =>  if exit {break;},
            Err(err) => {
                println!("{err}");
            }
        }
    }
    Ok(())
}