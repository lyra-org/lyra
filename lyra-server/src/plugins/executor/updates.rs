// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_core::{
    LocalScheduler,
    luau::{
        ThreadDriveOptions,
        drive_thread,
    },
};
use serde::Serialize;

use super::PluginExecutor;
use crate::{
    plugins::update_callbacks::UpdateCallbackStore,
    services::{
        listens::ListenUpdatePayload,
        playback_sessions::PlaybackUpdatePayload,
    },
};

impl PluginExecutor {
    pub(crate) fn dispatch_playback_update(&self, payload: PlaybackUpdatePayload) -> Result<()> {
        self.run_update_callbacks(&payload, |plugin_id, error| {
            tracing::warn!(
                playback_session_public_id = %payload.playback_session_public_id,
                event = %payload.event,
                plugin_id,
                error = %error,
                "playback on_update callback failed"
            );
        })
    }

    pub(crate) fn dispatch_listen_update(&self, payload: ListenUpdatePayload) -> Result<()> {
        self.run_update_callbacks(&payload, |plugin_id, error| {
            tracing::warn!(
                user_id = %payload.user_id,
                listens = payload.listens.len(),
                event = %payload.event,
                plugin_id,
                error = %error,
                "listen on_update callback failed"
            );
        })
    }

    /// Runs every callback registered for `Payload` updates to completion, one after another.
    fn run_update_callbacks<Payload: Serialize + 'static>(
        &self,
        payload: &Payload,
        on_error: impl Fn(&str, &anyhow::Error),
    ) -> Result<()> {
        let handlers = self
            .vm
            .data()
            .get::<UpdateCallbackStore<Payload>>()?
            .snapshot();
        if handlers.is_empty() {
            return Ok(());
        }

        let payload_value = harmony_luau::serializable_to_luau_owned(payload)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        for handler in handlers {
            let thread = self.vm.create_thread(&handler.function)?;
            scheduler.spawn_luau_thread(
                handler.context.clone(),
                self.vm.clone(),
                thread.clone(),
                vec![payload_value.clone()],
            );
            if let Err(error) = drive_thread(
                &self.tokio_runtime,
                &scheduler,
                &thread,
                ThreadDriveOptions::default(),
            ) {
                on_error(
                    handler.context.origin.plugin.as_deref().unwrap_or_default(),
                    &error,
                );
            }
        }
        Ok(())
    }
}
