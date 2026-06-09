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

use super::{
    PluginExecutor,
    messages::{
        MetadataRefreshRequest,
        MetadataRefreshResult,
    },
};

impl PluginExecutor {
    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        let handlers = self
            .vm
            .data()
            .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
        let handler = handlers
            .get(request.handler_id)
            .ok_or_else(|| anyhow::anyhow!("metadata handler {} not found", request.handler_id))?;
        let ctx = harmony_serde::json_to_luau_owned(request.context, 0)?;
        let thread = self.vm.create_thread(&handler.function)?;
        let scheduler = self.vm.data().get::<LocalScheduler>()?;
        scheduler.spawn_luau_thread(
            handler.context.clone(),
            self.vm.clone(),
            thread.clone(),
            vec![ctx],
        );
        let values = drive_thread(
            &self.tokio_runtime,
            &scheduler,
            &thread,
            ThreadDriveOptions::default(),
        )?
        .iter()
        .map(|value| harmony_serde::luau_to_json(&self.vm, value, 0).map_err(anyhow::Error::new))
        .collect::<Result<Vec<_>>>()?;
        Ok(MetadataRefreshResult { values })
    }
}
