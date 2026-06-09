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
    Db(DbCommand),
    Plugins(PluginsCommand),
}

#[derive(Debug, PartialEq, Eq)]
enum DbCommand {
    Optimize,
}

#[derive(Debug, PartialEq, Eq)]
enum PluginsCommand {
    Add {
        url: String,
        git_ref: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_command_args(&args)? {
        Command::Serve { capture_path } => lyra_server::run_server(capture_path).await,
        Command::Db(DbCommand::Optimize) => lyra_server::run_db_optimize().await,
        Command::Plugins(PluginsCommand::Add { url, git_ref }) => {
            lyra_server::run_plugins_add(&url, git_ref.as_deref()).await
        }
    }
}

fn parse_command_args(args: &[String]) -> Result<Command> {
    match args {
        [command] if command == "serve" => Ok(Command::Serve { capture_path: None }),
        [command, flag, path] if command == "serve" && flag == "--capture" => Ok(Command::Serve {
            capture_path: Some(path.clone()),
        }),
        [command, action] if command == "db" && action == "optimize" => {
            Ok(Command::Db(DbCommand::Optimize))
        }
        [command, action, url] if command == "plugins" && action == "add" => {
            Ok(Command::Plugins(PluginsCommand::Add {
                url: url.clone(),
                git_ref: None,
            }))
        }
        [command, action, url, flag, git_ref]
            if command == "plugins" && action == "add" && flag == "--ref" =>
        {
            Ok(Command::Plugins(PluginsCommand::Add {
                url: url.clone(),
                git_ref: Some(git_ref.clone()),
            }))
        }
        _ => bail!(usage()),
    }
}

fn usage() -> &'static str {
    "usage:\n  lyra serve [--capture <output-path>]\n  lyra db optimize\n  lyra plugins add <url> [--ref <ref>]"
}

#[cfg(test)]
mod tests {
    use super::{
        Command,
        DbCommand,
        PluginsCommand,
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
    fn parse_db_optimize_command() {
        let parsed = parse_command_args(&args(&["db", "optimize"])).expect("parse db optimize");
        assert_eq!(parsed, Command::Db(DbCommand::Optimize));
    }

    #[test]
    fn parse_plugins_add_command() {
        let parsed = parse_command_args(&args(&["plugins", "add", "https://github.com/o/r"]))
            .expect("parse plugins add");
        assert_eq!(
            parsed,
            Command::Plugins(PluginsCommand::Add {
                url: "https://github.com/o/r".to_string(),
                git_ref: None,
            })
        );
    }

    #[test]
    fn parse_plugins_add_command_with_ref() {
        let parsed = parse_command_args(&args(&[
            "plugins",
            "add",
            "https://github.com/o/r",
            "--ref",
            "v2",
        ]))
        .expect("parse plugins add with ref");
        assert_eq!(
            parsed,
            Command::Plugins(PluginsCommand::Add {
                url: "https://github.com/o/r".to_string(),
                git_ref: Some("v2".to_string()),
            })
        );
    }
}
