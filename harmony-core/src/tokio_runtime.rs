// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Context,
    Result,
};

pub struct TokioRuntimeContext {
    handle: tokio::runtime::Handle,
    owned: Option<tokio::runtime::Runtime>,
}

impl TokioRuntimeContext {
    pub fn new() -> Result<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self {
                handle,
                owned: None,
            });
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .context("create Tokio runtime context")?;
        let handle = runtime.handle().clone();
        Ok(Self {
            handle,
            owned: Some(runtime),
        })
    }

    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }
}

impl Drop for TokioRuntimeContext {
    fn drop(&mut self) {
        if let Some(runtime) = self.owned.take() {
            runtime.shutdown_background();
        }
    }
}
