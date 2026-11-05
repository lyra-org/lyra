// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! State setup for tests of the plugin layer and the layers above it.

#[cfg(test)]
pub(crate) use self::test_state::{
    init_default_test_state,
    init_library_test_state,
    init_test_state_with_file_settings,
};
use crate::{
    plugins,
    services::{
        self,
        libraries::LibraryConfig,
        settings::server::FileSettings,
    },
};

/// A `lyra` requirement for test manifests that matches the running server
/// alone, including a pre-release, which `*` would not match.
#[cfg(test)]
pub(crate) const HOST_REQUIREMENT: &str = concat!("=", env!("CARGO_PKG_VERSION"));

/// Opens a memory database seeded with `library` and the `settings` file layer
/// the way the server opens its own and publishes it to every layer. Call while
/// holding [`services::testing::runtime_test_lock`].
pub(crate) fn initialize_state(
    library: Option<LibraryConfig>,
    settings: FileSettings,
) -> anyhow::Result<()> {
    let (boot, mut created, resolved) = services::testing::open_memory_state(library, settings)?;
    plugins::repositories::seed_default(
        created
            .unshared_mut()
            .ok_or_else(|| anyhow::anyhow!("database was shared before it was seeded"))?,
    )?;
    services::STATE.initialize(boot, created, resolved)
}

/// [`services::testing::start_runtime`] plus the outbound user agent.
pub(crate) async fn start_runtime() -> anyhow::Result<()> {
    harmony_http::set_default_user_agent(services::STATE.boot.get().build.user_agent());
    services::testing::start_runtime().await
}

/// The runtime published to the current generation.
#[cfg(test)]
pub(crate) fn published_runtime() -> anyhow::Result<plugins::bootstrap::PluginRuntime> {
    use anyhow::Context;
    plugins::STATE
        .generation()
        .runtime
        .get()
        .context("published plugin runtime")
}

/// Writes a plugin `id` with `scopes` whose entrypoint is `init` into `directory`.
#[cfg(test)]
pub(crate) fn write_plugin(
    directory: &std::path::Path,
    id: &str,
    scopes: &[&str],
    init: &str,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(directory)?;
    std::fs::write(
        directory.join("plugin.json"),
        serde_json::json!({
            "schema_version": 1,
            "id": id,
            "name": id,
            "version": "1.0.0",
            "lyra": HOST_REQUIREMENT,
            "description": "",
            "entrypoint": "init.luau",
            "scopes": scopes,
        })
        .to_string(),
    )?;
    std::fs::write(directory.join("init.luau"), init)?;
    Ok(())
}

/// Makes reloads discover the plugins in `plugins_dir`.
#[cfg(test)]
pub(crate) fn use_plugins_dir(plugins_dir: &std::path::Path) {
    let boot = services::STATE.boot.get();
    services::STATE
        .boot
        .replace(std::sync::Arc::new(services::boot::Boot {
            plugins_dir: plugins_dir.to_path_buf(),
            ..(*boot).clone()
        }));
}

#[cfg(test)]
mod test_state {
    use std::path::PathBuf;

    use super::{
        initialize_state,
        start_runtime,
    };
    use crate::services::{
        settings::server::FileSettings,
        testing::library_config,
    };

    /// Default config over an in-memory DB.
    pub(crate) fn init_default_test_state() -> anyhow::Result<()> {
        initialize_state(None, FileSettings::default())
    }

    /// [`init_default_test_state`] with `settings` as the `config.json`
    /// layer, for tests that need file-locked server settings.
    pub(crate) fn init_test_state_with_file_settings(settings: FileSettings) -> anyhow::Result<()> {
        initialize_state(None, settings)
    }

    /// State over a library at `directory`, started like the runtime.
    pub(crate) async fn init_library_test_state(directory: PathBuf) -> anyhow::Result<()> {
        initialize_state(
            Some(library_config(directory, None, None)?),
            FileSettings::default(),
        )?;
        start_runtime().await
    }
}
