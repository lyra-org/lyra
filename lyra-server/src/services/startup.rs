// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    net::SocketAddr,
    sync::Arc,
};

use agdb::DbId;
use anyhow::Result;
use axum::{
    Router,
    extract::DefaultBodyLimit,
};
use harmony_core::Harmony;
use tokio::{
    net::TcpListener,
    sync::Notify,
};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::{
    STATE,
    config::DbKind,
    plugins::{
        api as plugin_api,
        bootstrap as plugin_bootstrap,
    },
    routes,
    services,
    services::hls::init as hls_init,
};

pub(crate) async fn run_server(capture_path: Option<String>) -> Result<()> {
    let _tracing_guard = init_tracing();

    let capture_mode = capture_path.is_some();
    let config = STATE.config.get();
    harmony_http::set_default_user_agent(crate::outbound_user_agent());
    hls_init::initialize_for_config(&config).await;

    let db = STATE.db.get();
    let maintenance_shutdown = if capture_mode {
        None
    } else {
        let storage_path = match config.db.kind {
            DbKind::Memory => None,
            _ => Some(config.db.path.clone()),
        };
        Some(services::maintenance::spawn(db.clone(), storage_path))
    };
    services::auth::ensure_default_user(&config).await?;
    {
        let mut db_write = STATE.db.write().await;
        crate::db::server::ensure(&mut db_write)?;
    }
    let configured_library =
        services::libraries::prepare_configured_library(&config, capture_mode).await?;
    let library_db_id = configured_library
        .as_ref()
        .and_then(|library| library.db_id);

    let harmony = plugin_bootstrap::initialize_harmony()?;

    if let Some(output_path) = capture_path {
        let capture_library_db_id = library_db_id.ok_or_else(|| {
            anyhow::anyhow!("--capture requires a library configured in config.json")
        })?;
        run_capture_mode(harmony, capture_library_db_id, &output_path).await?;
        return Ok(());
    }

    let core_api = routes::build_core_api()?;
    crate::plugins::runtime::initialize_registry().await;

    let app = plugin_api::install(core_api.router, core_api.reservations).await;
    let app = app.layer(axum::middleware::from_fn(
        services::metadata::mapping_admin::reingest_request_gate,
    ));

    let interval_secs = config.sync.interval_secs;
    let shutdown = Arc::new(Notify::new());
    let shutdown_bg = shutdown.clone();
    let bg_handle = tokio::spawn(async move {
        if let Err(err) = harmony.exec_all().await {
            tracing::error!(error = %err, "plugin initialization failed");
            return;
        }

        if let Err(err) = plugin_bootstrap::finalize_startup().await {
            tracing::error!(error = %err, "failed to finalize plugin startup");
            return;
        }

        plugin_bootstrap::publish_runtime(harmony.clone());
        services::providers::run_provider_sync_loop(interval_secs, shutdown_bg).await;
    });

    serve(app, config.as_ref()).await?;

    tracing::info!("server stopped, running shutdown cleanup");
    if let Some(ref maintenance_shutdown) = maintenance_shutdown {
        maintenance_shutdown.notify_one();
    }
    shutdown.notify_one();
    let _ = bg_handle.await;

    plugin_bootstrap::teardown_loaded_plugins().await;
    services::wait_for_running_library_syncs().await;
    optimize_storage_on_shutdown(&db).await;

    Ok(())
}

fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    // Non-blocking: a plugin warn-loop can't stall the scheduler on sync stderr.
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "{}=trace,tower_http=debug,harmony_core=info",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking))
        .init();

    guard
}

async fn run_capture_mode(
    harmony: Arc<Harmony>,
    library_db_id: DbId,
    output_path: &str,
) -> Result<()> {
    plugin_bootstrap::exec_for_capture(harmony).await?;
    services::providers::run_capture(library_db_id, output_path).await
}

async fn optimize_storage_on_shutdown(db: &crate::db::DbAsync) {
    let mut db_write = db.write().await;
    if let Err(err) = db_write.optimize_storage() {
        tracing::warn!(error = %err, "failed to optimize storage on shutdown");
    }
}

// Sharply below axum's 2MB default to bound CPU/RAM amplification on auth'd POSTs.
const REQUEST_BODY_LIMIT_BYTES: usize = 256 * 1024;

async fn serve(app: Router, config: &crate::config::Config) -> Result<()> {
    let port = config.port;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    tracing::debug!("listening on {}", listener.local_addr()?);
    let app = app.layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT_BYTES));
    let app = services::cors::apply(app, config);
    let app = app.layer(TraceLayer::new_for_http());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

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
