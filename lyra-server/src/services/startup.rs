// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    future::IntoFuture,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
};
use axum::{
    Router,
    extract::DefaultBodyLimit,
};
use tokio::{
    net::TcpListener,
    sync::Notify,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::plugins::api as plugin_api;
use crate::{
    STATE,
    plugins::bootstrap as plugin_bootstrap,
    routes,
    services,
    services::hls::init as hls_init,
    services::settings::server::SettingSource,
};

const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn bind_configured_listener(port: u16) -> Result<TcpListener> {
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

pub(crate) async fn run_server(capture: Option<CaptureArgs>, listener: TcpListener) -> Result<()> {
    let capture_mode = capture.is_some();
    let shutdown_token = services::shutdown::reset();
    let config = STATE.config();
    log_setting_overrides();
    harmony_http::set_default_user_agent(crate::outbound_user_agent());
    hls_init::initialize().await;

    let db = STATE.db.get();
    let maintenance_shutdown = if capture_mode {
        None
    } else {
        Some(services::maintenance::spawn(db.clone()))
    };
    services::auth::ensure_default_user(&config).await?;
    {
        let mut db_write = STATE.db.write().await;
        crate::db::server::ensure(&mut db_write)?;
    }

    let plugin_runtime = plugin_bootstrap::initialize_harmony().await?;

    if let Some(capture) = capture {
        // sync_library dispatches handlers that resolve STATE.generation().plugin_runtime; publish first.
        plugin_bootstrap::exec_for_capture(plugin_runtime.clone()).await?;
        plugin_bootstrap::publish_runtime(plugin_runtime);
        let library = services::libraries::prepare_capture_library(capture.library_path).await?;
        let library_db_id = library
            .db_id
            .ok_or_else(|| anyhow::anyhow!("capture library missing db_id"))?;
        return services::providers::run_capture(library_db_id, &capture.output_path).await;
    }

    let core_api = routes::build_core_api()?;
    crate::plugins::settings::initialize_registry().await;

    let app = plugin_api::install(core_api.router, core_api.reservations).await?;
    let app = app.layer(axum::middleware::from_fn(
        services::metadata::mapping_admin::reingest_request_gate,
    ));

    let shutdown = Arc::new(Notify::new());
    let shutdown_bg = shutdown.clone();
    let config_for_bg = config.clone();
    let bg_handle = tokio::spawn(async move {
        if let Err(err) = plugin_runtime.exec_all().await {
            tracing::error!(error = %err, "plugin initialization failed");
            return;
        }

        // Must precede finalize_startup (activates plugin routes) and the library sync
        // (dispatches handlers that resolve STATE.generation().plugin_runtime at call time).
        plugin_bootstrap::publish_runtime(plugin_runtime.clone());

        if let Err(err) = plugin_bootstrap::finalize_startup().await {
            tracing::error!(error = %err, "failed to finalize plugin startup");
            return;
        }

        if let Err(err) = services::libraries::prepare_configured_library(&config_for_bg).await {
            tracing::error!(error = %err, "configured library preparation failed");
            return;
        }

        services::providers::run_provider_sync_loop(shutdown_bg).await;
    });

    serve(app, config.as_ref(), listener, shutdown_token).await?;

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

    Ok(())
}

/// One line per setting that differs from its default, so an operator can
/// see at startup which values came from the database or `config.json`.
fn log_setting_overrides() {
    let settings = STATE.settings.get();
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

pub(crate) fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
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

// Sharply below axum's 2MB default to bound CPU/RAM amplification on auth'd POSTs.
const REQUEST_BODY_LIMIT_BYTES: usize = 256 * 1024;

async fn serve(
    app: Router,
    config: &crate::config::Config,
    listener: TcpListener,
    shutdown_token: CancellationToken,
) -> Result<()> {
    tracing::info!("listening on {}", listener.local_addr()?);
    let app = services::rate_limit::apply(app, config);
    let app = app.layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT_BYTES));
    let app = services::cors::apply(app);
    let app = app.layer(TraceLayer::new_for_http());
    let signal_token = shutdown_token.clone();
    let signal_handle = tokio::spawn(async move {
        shutdown_signal().await;
        signal_token.cancel();
    });

    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_token.clone().cancelled_owned())
    .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            signal_handle.abort();
            result?;
            return Ok(());
        }
        _ = shutdown_token.cancelled() => {}
    }

    match timeout(GRACEFUL_SHUTDOWN_TIMEOUT, &mut server).await {
        Ok(result) => result?,
        Err(_) => {
            tracing::warn!(
                timeout_ms = GRACEFUL_SHUTDOWN_TIMEOUT.as_millis() as u64,
                "server graceful drain deadline elapsed; forcing shutdown"
            );
        }
    }
    signal_handle.abort();

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await.ok();

    tracing::info!("shutdown signal received, draining connections");
}

#[cfg(test)]
mod tests {
    use super::bind_configured_listener;

    #[tokio::test]
    async fn bind_configured_listener_rejects_occupied_port() -> anyhow::Result<()> {
        let first = bind_configured_listener(0).await?;
        let port = first.local_addr()?.port();

        let second = bind_configured_listener(port).await;

        assert!(second.is_err());
        Ok(())
    }
}
