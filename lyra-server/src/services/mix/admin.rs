// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_db::mixers::MixerConfig;

use super::{
    MixHandlerSpec,
    MixShape,
    mix_registry,
};
use crate::services::{
    STATE,
    options::OptionDeclaration,
    plugin_scope::PluginId,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MixerAdminError {
    #[error("Mixer not found: {0}")]
    MixerNotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MixerUpdate {
    pub(crate) position: Option<u32>,
    pub(crate) enabled: Option<bool>,
}

/// A stored mixer with what its plugin registered for it this run, if it is loaded.
#[derive(Debug)]
pub(crate) struct MixerListing {
    pub(crate) config: MixerConfig,
    pub(crate) plugin_id: Option<PluginId>,
    pub(crate) options: Vec<OptionDeclaration>,
    pub(crate) blend: Option<MixHandlerSpec>,
    pub(crate) path: Option<MixHandlerSpec>,
}

/// What the built-in mixer takes, and every mixer in priority order.
#[derive(Debug)]
pub(crate) struct MixersListing {
    pub(crate) builtin: MixHandlerSpec,
    pub(crate) mixers: Vec<MixerListing>,
}

pub(crate) async fn list_mixers() -> Result<MixersListing, MixerAdminError> {
    let configs = {
        let db = STATE.db.read().await;
        lyra_db::mixers::get(&db)?
    };
    let registry = mix_registry().read_owned().await;
    let mixers = configs
        .into_iter()
        .map(|config| {
            let spec = |shape| {
                registry
                    .handler(&config.mixer_id, shape)
                    .map(|registration| registration.spec.clone())
            };
            MixerListing {
                plugin_id: registry.plugin_id(&config.mixer_id).cloned(),
                options: registry.get_options(&config.mixer_id).to_vec(),
                blend: spec(MixShape::Blend),
                path: spec(MixShape::Path),
                config,
            }
        })
        .collect();
    Ok(MixersListing {
        builtin: super::builtin::spec(),
        mixers,
    })
}

/// Applies the update and lists the mixers as [`list_mixers`] does.
pub(crate) async fn update_mixer(
    mixer_id: &str,
    update: MixerUpdate,
) -> Result<MixersListing, MixerAdminError> {
    let mut db = STATE.db.write().await;
    if lyra_db::mixers::get_by_mixer_id(&db, mixer_id)?.is_none() {
        return Err(MixerAdminError::MixerNotFound(mixer_id.to_string()));
    }
    db.transaction_mut(|transaction| -> anyhow::Result<()> {
        if let Some(position) = update.position {
            lyra_db::mixers::move_to(transaction, mixer_id, position)?;
        }
        if let Some(enabled) = update.enabled {
            lyra_db::mixers::set_enabled(transaction, mixer_id, enabled)?;
        }
        Ok(())
    })?;
    drop(db);
    list_mixers().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn updating_a_mixer_returns_the_new_order() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        {
            let mut db = STATE.db.write().await;
            lyra_db::mixers::register(&mut db, "first", None)?;
            lyra_db::mixers::register(&mut db, "second", None)?;
        }

        let mixers = update_mixer(
            "second",
            MixerUpdate {
                position: Some(0),
                enabled: Some(false),
            },
        )
        .await?;
        let order = mixers
            .mixers
            .iter()
            .map(|mixer| {
                (
                    mixer.config.mixer_id.as_str(),
                    mixer.config.position,
                    mixer.config.enabled,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(order, [("second", 0, false), ("first", 1, true)]);

        let result = update_mixer("missing", MixerUpdate::default()).await;
        assert!(matches!(result, Err(MixerAdminError::MixerNotFound(_))));
        Ok(())
    }
}
