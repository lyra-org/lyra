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

use super::{
    CallChain,
    PluginExecutor,
};
use crate::{
    plugins::update_callbacks::UpdateCallbackStore,
    services::{
        listens::ListenUpdatePayload,
        playback_sessions::PlaybackUpdatePayload,
    },
};

impl PluginExecutor {
    pub(crate) fn dispatch_playback_update(&self, payload: PlaybackUpdatePayload) -> Result<()> {
        let update = crate::plugins::playback_sessions::PlaybackUpdate::from(&payload);
        self.run_update_callbacks::<PlaybackUpdatePayload>(&update, |plugin_id, error| {
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
        self.run_update_callbacks::<ListenUpdatePayload>(&payload, |plugin_id, error| {
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

    /// Runs every callback registered for `Payload` updates to completion, one after another,
    /// handing each `payload` as plugins read it.
    fn run_update_callbacks<Payload: 'static>(
        &self,
        payload: &impl Serialize,
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
            if self.is_quiesced(handler.context.origin.plugin.as_deref()) {
                continue;
            }
            let thread = self.vm.create_thread(&handler.function)?;
            // Run to completion on the executor, so what they call can never wait on it.
            let options = ThreadDriveOptions::default();
            let mut context = handler.context.clone();
            context.caller.insert(CallChain::synchronous(
                &options,
                context.origin.plugin.clone(),
            ));
            scheduler.spawn_luau_thread(
                context,
                self.vm.clone(),
                thread.clone(),
                vec![payload_value.clone()],
            );
            if let Err(error) = drive_thread(&self.tokio_runtime, &scheduler, &thread, options) {
                on_error(
                    handler.context.origin.plugin.as_deref().unwrap_or_default(),
                    &error,
                );
            }
        }
        Ok(())
    }
}
