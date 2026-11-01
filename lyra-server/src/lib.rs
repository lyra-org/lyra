// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Lyra server application crate.
#![cfg_attr(all(test, feature = "nightly"), feature(test))]

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use agdb::DbAny;
use anyhow::{
    Context,
    Result,
};
use tokio::{
    net::TcpListener,
    sync::Notify,
};
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

mod config;
mod plugins;
mod routes;
mod services;
pub mod testing;

pub use services::boot::BuildInfo;

use plugins::bootstrap as plugin_bootstrap;
use services::hls::init as hls_init;
use services::libraries::LibraryConfig;
use services::settings::server::{
    self as server_settings,
    SettingSource,
};

pub async fn run_server(build: BuildInfo, capture: Option<CaptureArgs>) -> Result<()> {
    let _tracing_guard = init_tracing();
    let loaded = config::load()?;
    // Validate the file before touching the port, directories, or database.
    let library = configured_library(&loaded.file)?;
    let file_settings = server_settings::normalize_file(&loaded.file.settings)?;
    // Bind first: a port collision with a running server must fail fast
    // instead of blocking on that server's DB process lock.
    let listener = bind_configured_listener(loaded.boot.port).await?;
    loaded.boot.ensure_directories()?;
    let plugins_dir = config::plugins_dir();
    config::storage::ensure_writable_directory(
        &plugins_dir,
        "plugins directory (LYRA_PLUGINS_DIR)",
    )?;
    let mut created =
        lyra_db::create(&loaded.boot.db).map_err(config::storage::explain_db_open_error)?;
    plugins::repositories::seed_default(
        created
            .unshared_mut()
            .ok_or_else(|| anyhow::anyhow!("database was shared before it was seeded"))?,
    )?;
    let resolved = services::state::resolve_settings(
        &mut created,
        &loaded.boot.data_dir,
        library,
        file_settings,
    )?;
    let boot = services::boot::Boot {
        port: loaded.boot.port,
        transcode_cache_dir: loaded.boot.transcode_cache_dir(),
        hls_dir: loaded.boot.hls_dir(),
        data_dir: loaded.boot.data_dir,
        db: loaded.boot.db,
        plugins_dir,
        build,
    };
    services::STATE.initialize(boot, created, resolved)?;
    serve(capture, listener).await
}

/// The library `file` seeds, validated before anything is touched.
fn configured_library(file: &config::ConfigFile) -> Result<Option<LibraryConfig>> {
    file.library
        .as_ref()
        .map(|library| {
            LibraryConfig::resolve(
                library.path.clone(),
                library.name.as_deref(),
                library.language.as_deref(),
                library.country.as_deref(),
            )
        })
        .transpose()
}

async fn bind_configured_listener(port: u16) -> Result<TcpListener> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("configured port {port} is already in use or unavailable"))
}

/// `serve --capture <output-path> --library <dir>`: syncs the library at
/// `library_path` and writes the provider capture to `output_path` instead
/// of serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureArgs {
    pub output_path: String,
    pub library_path: PathBuf,
}

async fn serve(capture: Option<CaptureArgs>, listener: TcpListener) -> Result<()> {
    let capture_mode = capture.is_some();
    let shutdown_token = services::shutdown::reset();
    let config = services::STATE.config();
    log_setting_overrides();
    harmony_http::set_default_user_agent(services::STATE.boot.get().build.user_agent());
    hls_init::initialize().await;
    services::transcoding::initialize().await;

    let db = services::STATE.db.get();
    let maintenance_shutdown = if capture_mode {
        None
    } else {
        Some(services::maintenance::spawn(db.clone()))
    };
    services::auth::ensure_default_user(&config).await?;
    {
        let mut db_write = services::STATE.db.write().await;
        lyra_db::server::ensure(&mut db_write)?;
    }

    let plugin_runtime = plugin_bootstrap::initialize_harmony().await?;

    if let Some(capture) = capture {
        // sync_library dispatches handlers through the published runtime; publish first.
        plugin_bootstrap::exec_for_capture(plugin_runtime.clone()).await?;
        crate::plugins::STATE.publish_runtime(plugin_runtime);
        let library = services::libraries::prepare_capture_library(capture.library_path).await?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("capture library missing db_id"))?;
        return services::providers::run_capture(library_db_id, &capture.output_path).await;
    }

    services::catalog::index::warm();
    let core_api = routes::build_core_api()?;
    crate::plugins::settings::initialize_registry().await;

    let app = routes::plugin_api::install(core_api.router, core_api.reservations).await?;
    let app = app.layer(axum::middleware::from_fn(routes::reingest_request_gate));

    let shutdown = Arc::new(Notify::new());
    let shutdown_bg = shutdown.clone();
    let config_for_bg = config.clone();
    let bg_handle = tokio::spawn(async move {
        // Publishing must precede finalize_startup (activates plugin routes) and the
        // library sync (dispatches handlers through the published runtime).
        if let Err(err) = crate::plugins::STATE
            .generation()
            .registries
            .start_plugins(plugin_runtime)
            .await
        {
            tracing::error!(error = %err, "plugin initialization failed");
            return;
        }

        if let Err(err) = plugin_bootstrap::finalize_startup().await {
            tracing::error!(error = %err, "failed to finalize plugin startup");
            return;
        }
        let update_checks = tokio::spawn(plugins::repositories::run_update_checks(
            services::shutdown::token(),
        ));

        if let Err(err) = services::libraries::prepare_configured_library(&config_for_bg).await {
            tracing::error!(error = %err, "configured library preparation failed");
        }

        services::providers::run_provider_sync_loop(shutdown_bg).await;
        let _ = update_checks.await;
    });

    routes::serve_app(app, config.as_ref(), listener, shutdown_token).await?;

    tracing::info!("server stopped, running shutdown cleanup");
    services::shutdown::cancel();
    if let Some(ref maintenance_shutdown) = maintenance_shutdown {
        maintenance_shutdown.notify_one();
    }
    shutdown.notify_one();
    let _ = bg_handle.await;

    services::hls::state::teardown_all_hls_jobs().await;
    plugin_bootstrap::teardown_loaded_plugins().await;
    services::wait_for_running_library_syncs().await;

    if let Some(reason) = services::shutdown::failure() {
        anyhow::bail!("server stopped after a fatal error: {reason}");
    }
    Ok(())
}

/// One line per setting that differs from its default, so an operator can
/// see at startup which values came from the database or `config.json`.
fn log_setting_overrides() {
    let settings = services::STATE.settings.get();
    for setting in settings
        .effective
        .iter()
        .filter(|setting| setting.source != SettingSource::Default)
    {
        tracing::info!(
            key = setting.definition.key,
            value = %setting.value,
            source = ?setting.source,
            locked = setting.locked,
            "server setting overrides default"
        );
    }
}

fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    // Non-blocking: a plugin warn-loop can't stall the scheduler on sync stderr.
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "{}=info,tower_http=debug,harmony_core=info",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .init();

    guard
}

/// Ids of the Luau definition sources the server renders, for `lyra-docs`.
#[cfg(feature = "docgen")]
pub fn lyra_doc_source_ids() -> impl Iterator<Item = &'static str> {
    plugins::lyra_doc_source_ids()
}

/// Renders the Luau definitions for `id`, or `None` for an unknown id.
#[cfg(feature = "docgen")]
pub fn render_lyra_doc_source(id: &str) -> Result<Option<String>> {
    plugins::render_lyra_doc_source(id)
}

/// The REST API's OpenAPI document.
#[cfg(feature = "docgen")]
pub fn openapi_spec() -> impl serde::Serialize {
    routes::build_openapi_spec()
}

/// The WebSocket API's AsyncAPI document.
#[cfg(feature = "docgen")]
pub fn asyncapi_spec() -> impl serde::Serialize {
    let mut spec = services::remote::messages::WsApiSpec::asyncapi_spec();
    spec.info.version = env!("CARGO_PKG_VERSION").to_string();
    spec
}

/// Installs plugins from a Git repository into the plugins directory.
/// Disk-only: the next server start (or a runtime reload through the API)
/// loads them, so this works whether or not the server is running.
pub async fn run_plugins_add(build: BuildInfo, url: &str, git_ref: Option<&str>) -> Result<()> {
    let report = plugins::repositories::install_to_disk(
        url,
        git_ref,
        plugins::repositories::Selection::All,
        &build,
        &config::plugins_dir(),
    )
    .await
    .map_err(|err| anyhow::anyhow!("{err}"))?;

    for plugin in &report.installed {
        println!(
            "installed {} v{} ({})",
            plugin.id, plugin.version, plugin.commit
        );
        if !plugin.scopes.is_empty() {
            println!("  scopes: {}", plugin.scopes.join(", "));
        }
    }
    for held in &report.held {
        eprintln!("held {} v{}: {}", held.id, held.release.version, held.hold);
    }
    for failure in &report.failed {
        eprintln!("failed to install {}: {}", failure.id, failure.error);
    }
    let unmet = report.held.len() + report.failed.len();
    if unmet > 0 {
        anyhow::bail!("{unmet} plugin(s) were not installed");
    }

    println!("restart the server (or POST /api/plugins/reload) to load them");
    Ok(())
}

/// A persistent database opened for a CLI subcommand: the process lock is
/// held for as long as the value lives. Opens in `DbFile` regardless of the
/// configured kind, skips schema init, and refuses a database this version
/// would migrate or restamp.
struct CliDb {
    db: DbAny,
    path: std::path::PathBuf,
    _lock: Option<lyra_db::process_lock::DbProcessLock>,
}

/// `command` is the CLI command name (for example `db optimize`) used in
/// diagnostics; `verb` describes the action in "nothing to <verb>" errors.
fn open_cli_db(command: &'static str, verb: &str) -> Result<CliDb> {
    open_cli_db_at(&config::load()?.boot.db, command, verb)
}

fn open_cli_db_at(
    config: &lyra_db::bootstrap::DbConfig,
    command: &'static str,
    verb: &str,
) -> Result<CliDb> {
    if matches!(config.kind, lyra_db::bootstrap::DbKind::Memory) {
        anyhow::bail!(
            "nothing to {verb}: db kind is memory; configure DbKind::File or DbKind::Mmap to use this command"
        );
    }

    let path = config.path.clone();
    if !path.is_file() {
        anyhow::bail!("nothing to {verb}: no database file at {}", path.display());
    }

    let lock = lyra_db::process_lock::acquire(
        config,
        lyra_db::process_lock::LockMode::NonBlocking { command },
    )
    .map_err(config::storage::explain_db_open_error)?;
    let db = lyra_db::bootstrap::open(
        lyra_db::bootstrap::DbKind::File,
        path.to_string_lossy().as_ref(),
    )?;
    lyra_db::bootstrap::ensure_current(&db)?;
    Ok(CliDb {
        db,
        path,
        _lock: lock,
    })
}

/// Force-compact the DB from the CLI.
pub async fn run_db_optimize() -> Result<()> {
    let _tracing_guard = init_tracing();
    let CliDb {
        mut db,
        path: db_path,
        _lock,
    } = open_cli_db("db optimize", "optimize")?;
    tracing::info!(path = %db_path.display(), "optimizing db");

    // After the open: WAL recovery may have grown the file before optimize runs.
    let before_logical = db.size();
    let before_file = std::fs::metadata(&db_path)
        .with_context(|| {
            format!(
                "failed to read db metadata at {} after open",
                db_path.display()
            )
        })?
        .len();

    db.optimize_storage()
        .map_err(|err| anyhow::anyhow!("optimize_storage failed: {err}"))?;
    let after_logical = db.size();
    drop(db);
    let after_file = std::fs::metadata(&db_path)
        .with_context(|| {
            format!(
                "failed to read db metadata at {} after optimize",
                db_path.display()
            )
        })?
        .len();

    let reclaimed = before_file.saturating_sub(after_file);
    eprintln!("optimize_storage complete:");
    eprintln!("  logical bytes: {before_logical} -> {after_logical}");
    eprintln!("  file bytes:    {before_file} -> {after_file} (reclaimed {reclaimed})");

    Ok(())
}

/// Clears every stored server setting so a database whose entries no longer
/// validate can start again.
pub async fn run_settings_reset() -> Result<()> {
    let _tracing_guard = init_tracing();
    let mut cli = open_cli_db("settings reset", "reset")?;
    let removed = server_settings::reset_stored(&mut cli.db)?;

    if removed.is_empty() {
        eprintln!("no stored server settings to remove");
    } else {
        eprintln!("removed {} stored server setting(s):", removed.len());
        for key in removed {
            eprintln!("  {key}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_library_resolves_to_none() -> anyhow::Result<()> {
        assert!(configured_library(&config::ConfigFile::parse("{}")?)?.is_none());
        Ok(())
    }

    fn cli_config(dir: &std::path::Path) -> lyra_db::bootstrap::DbConfig {
        lyra_db::bootstrap::DbConfig {
            kind: lyra_db::bootstrap::DbKind::File,
            path: dir.join("lyra.db"),
        }
    }

    #[test]
    fn cli_opens_a_current_database() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = cli_config(dir.path());
        drop(lyra_db::bootstrap::create(&config)?);
        open_cli_db_at(&config, "settings reset", "reset")?;
        Ok(())
    }

    #[test]
    fn cli_refuses_a_database_that_is_not_current() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = cli_config(dir.path());
        let mut db = lyra_db::bootstrap::open(config.kind, config.path.to_string_lossy().as_ref())?;
        db.exec_mut(
            agdb::QueryBuilder::insert()
                .nodes()
                .aliases("tracks")
                .query(),
        )?;
        drop(db);
        let error = open_cli_db_at(&config, "settings reset", "reset")
            .err()
            .expect("an unstamped database should be refused");
        assert!(
            error
                .to_string()
                .starts_with("database has no Lyra version; "),
            "{error}"
        );

        let config = cli_config(&dir.path().join("empty"));
        std::fs::create_dir(dir.path().join("empty"))?;
        drop(lyra_db::bootstrap::open(
            config.kind,
            config.path.to_string_lossy().as_ref(),
        )?);
        let error = open_cli_db_at(&config, "settings reset", "reset")
            .err()
            .expect("an empty database should be refused");
        assert_eq!(error.to_string(), "database is empty");
        Ok(())
    }
}
