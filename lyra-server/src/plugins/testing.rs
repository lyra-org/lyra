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
    config::ConfigFile,
    plugins,
    services,
};

/// Opens a memory database for `file` the way the server opens its own and
/// publishes it to every layer. Call while holding
/// [`services::testing::runtime_test_lock`].
pub(crate) fn initialize_state(file: ConfigFile) -> anyhow::Result<()> {
    let (boot, mut created, resolved) = services::testing::open_memory_state(file)?;
    plugins::repositories::seed_default(
        created
            .unshared_mut()
            .ok_or_else(|| anyhow::anyhow!("database was shared before it was seeded"))?,
        boot.build.release_tag.as_deref(),
    )?;
    services::STATE.initialize(boot, created, resolved)
}

/// [`services::testing::start_runtime`] plus the outbound user agent.
pub(crate) async fn start_runtime() -> anyhow::Result<()> {
    harmony_http::set_default_user_agent(services::STATE.boot.get().build.user_agent());
    services::testing::start_runtime().await
}

#[cfg(test)]
mod test_state {
    use std::path::PathBuf;

    use super::{
        initialize_state,
        start_runtime,
    };
    use crate::{
        config::{
            ConfigFile,
            FileSettings,
        },
        services::testing::library_config,
    };

    /// Default config over an in-memory DB.
    pub(crate) fn init_default_test_state() -> anyhow::Result<()> {
        initialize_state(ConfigFile::default())
    }

    /// [`init_default_test_state`] with `settings` as the `config.json`
    /// layer, for tests that need file-locked server settings.
    pub(crate) fn init_test_state_with_file_settings(settings: FileSettings) -> anyhow::Result<()> {
        initialize_state(ConfigFile {
            settings,
            ..ConfigFile::default()
        })
    }

    /// State over a library at `directory`, started like the runtime.
    pub(crate) async fn init_library_test_state(directory: PathBuf) -> anyhow::Result<()> {
        initialize_state(library_config(directory, None, None))?;
        start_runtime().await
    }
}
