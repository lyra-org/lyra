// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Result,
    bail,
};

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve { capture_path: Option<String> },
    Docs(Vec<String>),
    Db(DbCommand),
}

#[derive(Debug, PartialEq, Eq)]
enum DbCommand {
    Optimize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_command_args(&args)? {
        Command::Serve { capture_path } => lyra_server::run_server(capture_path).await,
        Command::Docs(args) => lyra_server::run_docs_command(&args),
        Command::Db(DbCommand::Optimize) => lyra_server::run_db_optimize().await,
    }
}

fn parse_command_args(args: &[String]) -> Result<Command> {
    match args {
        [command] if command == "serve" => Ok(Command::Serve { capture_path: None }),
        [command, flag, path] if command == "serve" && flag == "--capture" => Ok(Command::Serve {
            capture_path: Some(path.clone()),
        }),
        [command, rest @ ..] if command == "docs" => Ok(Command::Docs(rest.to_vec())),
        [command, action] if command == "db" && action == "optimize" => {
            Ok(Command::Db(DbCommand::Optimize))
        }
        _ => bail!(usage()),
    }
}

fn usage() -> &'static str {
    "usage:\n  lyra serve [--capture <output-path>]\n  lyra docs <list|print|generate|setup> [options]\n  lyra db optimize"
}

#[cfg(test)]
mod tests {
    use super::{
        Command,
        DbCommand,
        parse_command_args,
    };

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn parse_serve_command_without_capture() {
        let parsed = parse_command_args(&args(&["serve"])).expect("parse serve command");
        assert_eq!(parsed, Command::Serve { capture_path: None });
    }

    #[test]
    fn parse_serve_command_with_capture() {
        let parsed = parse_command_args(&args(&["serve", "--capture", "out.json"]))
            .expect("parse serve capture command");
        assert_eq!(
            parsed,
            Command::Serve {
                capture_path: Some("out.json".to_string()),
            }
        );
    }

    #[test]
    fn parse_docs_command_preserves_args() {
        let parsed = parse_command_args(&args(&["docs", "print", "harmony/http"]))
            .expect("parse docs command");
        assert_eq!(
            parsed,
            Command::Docs(vec!["print".to_string(), "harmony/http".to_string()])
        );
    }

    #[test]
    fn docs_generate_requires_out_dir() {
        let error = lyra_server::run_docs_command(&args(&["generate"]))
            .expect_err("reject missing out dir");
        assert_eq!(
            error.to_string(),
            "usage:\n  lyra docs list\n  lyra docs print <source>\n  lyra docs generate --out-dir <dir>\n  lyra docs setup"
        );
    }

    #[test]
    fn parse_db_optimize_command() {
        let parsed = parse_command_args(&args(&["db", "optimize"])).expect("parse db optimize");
        assert_eq!(parsed, Command::Db(DbCommand::Optimize));
    }
}
