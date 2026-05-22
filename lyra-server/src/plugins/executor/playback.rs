// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_core::LocalScheduler;

use super::{
    PluginExecutor,
    runner::drive_luau_thread,
};

impl PluginExecutor {
    pub(crate) fn dispatch_playback_update(
        &self,
        payload: crate::services::playback_sessions::PlaybackUpdatePayload,
    ) -> Result<()> {
        let callbacks = self
            .vm
            .data()
            .get::<crate::plugins::playback_sessions::PlaybackUpdateCallbackStore>()?;
        let handlers = callbacks.snapshot();
        if handlers.is_empty() {
            return Ok(());
        }

        let payload_value = harmony_luau::serializable_to_luau_owned(&payload)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        for handler in handlers {
            let thread = self.vm.create_thread(&handler.function)?;
            scheduler.spawn_luau_thread(
                handler.context.clone(),
                self.vm.clone(),
                thread.clone(),
                vec![payload_value.clone()],
            );
            if let Err(error) = drive_luau_thread(&self.tokio_runtime, &scheduler, &thread) {
                tracing::warn!(
                    playback_session_public_id = %payload.playback_session_public_id,
                    event = %payload.event,
                    plugin_id = %handler.plugin_id,
                    error = %error,
                    "playback on_update callback failed"
                );
            }
        }
        Ok(())
    }
}
