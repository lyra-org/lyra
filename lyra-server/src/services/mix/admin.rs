// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use lyra_db::mixers::MixerConfig;

use crate::services::STATE;

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

pub(crate) async fn list_mixer_configs() -> Result<Vec<MixerConfig>, MixerAdminError> {
    let db = STATE.db.read().await;
    Ok(lyra_db::mixers::get(&db)?)
}

/// Applies the update and returns every mixer in priority order.
pub(crate) async fn update_mixer(
    mixer_id: &str,
    update: MixerUpdate,
) -> Result<Vec<MixerConfig>, MixerAdminError> {
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
    Ok(lyra_db::mixers::get(&db)?)
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
            lyra_db::mixers::register(&mut db, "first")?;
            lyra_db::mixers::register(&mut db, "second")?;
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
            .iter()
            .map(|mixer| (mixer.mixer_id.as_str(), mixer.position, mixer.enabled))
            .collect::<Vec<_>>();
        assert_eq!(order, [("second", 0, false), ("first", 1, true)]);

        let result = update_mixer("missing", MixerUpdate::default()).await;
        assert!(matches!(result, Err(MixerAdminError::MixerNotFound(_))));
        Ok(())
    }
}
