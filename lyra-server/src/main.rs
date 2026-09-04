// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::PathBuf;

use anyhow::{
    Result,
    bail,
};
use lyra_server::CaptureArgs;

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve { capture: Option<CaptureArgs> },
    Db(DbCommand),
    Plugins(PluginsCommand),
    Settings(SettingsCommand),
}

#[derive(Debug, PartialEq, Eq)]
enum SettingsCommand {
    Reset,
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
        Command::Serve { capture } => lyra_server::run_server(capture).await,
        Command::Db(DbCommand::Optimize) => lyra_server::run_db_optimize().await,
        Command::Settings(SettingsCommand::Reset) => lyra_server::run_settings_reset().await,
        Command::Plugins(PluginsCommand::Add { url, git_ref }) => {
            lyra_server::run_plugins_add(&url, git_ref.as_deref()).await
        }
    }
}

fn parse_command_args(args: &[String]) -> Result<Command> {
    match args {
        [command] if command == "serve" => Ok(Command::Serve { capture: None }),
        [
            command,
            capture_flag,
            output_path,
            library_flag,
            library_path,
        ] if command == "serve" && capture_flag == "--capture" && library_flag == "--library" => {
            Ok(Command::Serve {
                capture: Some(CaptureArgs {
                    output_path: output_path.clone(),
                    library_path: PathBuf::from(library_path),
                }),
            })
        }
        [command, capture_flag, _output_path]
            if command == "serve" && capture_flag == "--capture" =>
        {
            bail!("--capture requires --library <library-dir>\n{}", usage())
        }
        [command, action] if command == "db" && action == "optimize" => {
            Ok(Command::Db(DbCommand::Optimize))
        }
        [command, action] if command == "settings" && action == "reset" => {
            Ok(Command::Settings(SettingsCommand::Reset))
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
    "usage:\n  lyra serve [--capture <output-path> --library <library-dir>]\n  lyra db optimize\n  lyra settings reset\n  lyra plugins add <url> [--ref <ref>]"
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        CaptureArgs,
        Command,
        DbCommand,
        PluginsCommand,
        SettingsCommand,
        parse_command_args,
    };

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn parse_serve_command_without_capture() {
        let parsed = parse_command_args(&args(&["serve"])).expect("parse serve command");
        assert_eq!(parsed, Command::Serve { capture: None });
    }

    #[test]
    fn parse_serve_command_with_capture() {
        let parsed = parse_command_args(&args(&[
            "serve",
            "--capture",
            "out.json",
            "--library",
            "/music",
        ]))
        .expect("parse serve capture command");
        assert_eq!(
            parsed,
            Command::Serve {
                capture: Some(CaptureArgs {
                    output_path: "out.json".to_string(),
                    library_path: PathBuf::from("/music"),
                }),
            }
        );
    }

    #[test]
    fn parse_serve_capture_requires_library_flag() {
        let err = parse_command_args(&args(&["serve", "--capture", "out.json"]))
            .expect_err("capture without library should fail");
        assert!(err.to_string().contains("--library"), "{err}");
    }

    #[test]
    fn parse_serve_capture_with_misplaced_library_flag_is_usage() {
        let err = parse_command_args(&args(&["serve", "--capture", "--library", "/music"]))
            .expect_err("misplaced flags should fail");
        assert!(!err.to_string().contains("requires"), "{err}");
    }

    #[test]
    fn parse_db_optimize_command() {
        let parsed = parse_command_args(&args(&["db", "optimize"])).expect("parse db optimize");
        assert_eq!(parsed, Command::Db(DbCommand::Optimize));
    }

    #[test]
    fn parse_settings_reset_command() {
        let parsed =
            parse_command_args(&args(&["settings", "reset"])).expect("parse settings reset");
        assert_eq!(parsed, Command::Settings(SettingsCommand::Reset));
        assert!(parse_command_args(&args(&["settings"])).is_err());
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
