// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    future::poll_fn,
    rc::Rc,
    time::Duration,
};

use anyhow::{
    Context,
    Result,
    bail,
};
use harmony_core::{
    LocalLuauTaskCompletion,
    LocalScheduler,
    luau::{
        ThreadDriveOptions,
        drive_thread,
    },
};

use super::{
    PluginExecutor,
    messages::{
        MetadataDispatchContext,
        MetadataRefreshRequest,
        MetadataRefreshResult,
        SimilarReleaseCandidate,
        SimilarReleaseExternalRef,
        SimilarReleasesDispatchRequest,
        SimilarReleasesDispatchResult,
    },
};

const DEFAULT_DRIVE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SIMILAR_RELEASE_STRING_BYTES: usize = 4096;

impl PluginExecutor {
    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        let values = self.dispatch_metadata_handler(
            request.handler_id,
            request.context,
            DEFAULT_DRIVE_TIMEOUT,
            None,
        )?;
        let values = values
            .iter()
            .map(|value| {
                harmony_serde::luau_to_json(&self.vm, value, 0).map_err(anyhow::Error::new)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MetadataRefreshResult { values })
    }

    pub(crate) fn dispatch_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> Result<SimilarReleasesDispatchResult> {
        let handler = similar_releases_handler(&self.vm, request.handler_id, &request.provider_id)?;
        let values = self.dispatch_handler(
            handler,
            request.context,
            request.timeout,
            Some(&request.cancellation),
        )?;
        let candidates =
            decode_similar_releases_result(&self.vm, values.first(), request.max_candidates)?;
        Ok(SimilarReleasesDispatchResult { candidates })
    }

    fn dispatch_metadata_handler(
        &self,
        handler_id: u64,
        context: serde_json::Value,
        timeout: Duration,
        cancellation: Option<&super::MetadataRefreshCancellation>,
    ) -> Result<Vec<harmony_luau::Value>> {
        let handlers = self
            .vm
            .data()
            .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
        let handler: crate::plugins::metadata::MetadataCallback = handlers
            .get(handler_id)
            .ok_or_else(|| anyhow::anyhow!("metadata handler {handler_id} not found"))?;
        self.dispatch_handler(handler, context, timeout, cancellation)
    }

    fn dispatch_handler(
        &self,
        handler: crate::plugins::metadata::MetadataCallback,
        context: serde_json::Value,
        timeout: Duration,
        cancellation: Option<&super::MetadataRefreshCancellation>,
    ) -> Result<Vec<harmony_luau::Value>> {
        if cancellation.is_some_and(|cancellation| cancellation.is_cancelled()) {
            bail!("metadata handler dispatch was cancelled");
        }
        let (scheduler, thread) =
            schedule_metadata_handler(&self.vm, handler, context, timeout, None)?;
        let values = drive_thread(
            &self.tokio_runtime,
            &scheduler,
            &thread,
            ThreadDriveOptions {
                timeout,
                cancellation: cancellation.map(|cancellation| cancellation.flag()),
                ..ThreadDriveOptions::default()
            },
        )?;
        if cancellation.is_some_and(|cancellation| cancellation.is_cancelled()) {
            bail!("metadata handler dispatch was cancelled");
        }
        Ok(values)
    }
}

fn schedule_metadata_handler(
    vm: &harmony_luau::Vm,
    handler: crate::plugins::metadata::MetadataCallback,
    request_context: serde_json::Value,
    timeout: Duration,
    completion: Option<Rc<LocalLuauTaskCompletion>>,
) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)> {
    let argument = harmony_serde::json_to_luau_owned(request_context, 0)?;
    let thread = vm.create_thread(&handler.function)?;
    let scheduler = vm.data().get::<LocalScheduler>()?;
    let mut context = handler.context.clone();
    context.caller.insert(MetadataDispatchContext);
    if let Some(completion) = completion {
        scheduler.schedule_luau_thread_with_budget_and_completion(
            context,
            vm.clone(),
            thread.clone(),
            vec![argument],
            timeout,
            completion,
        );
    } else {
        scheduler.schedule_luau_thread_with_budget(
            context,
            vm.clone(),
            thread.clone(),
            vec![argument],
            timeout,
        );
    }
    Ok((scheduler, thread))
}

pub(crate) async fn dispatch_similar_releases_in_vm(
    vm: harmony_luau::Vm,
    request: SimilarReleasesDispatchRequest,
) -> Result<SimilarReleasesDispatchResult> {
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let handler = similar_releases_handler(&vm, request.handler_id, &request.provider_id)?;
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let (scheduler, thread) = schedule_metadata_handler(
        &vm,
        handler,
        request.context,
        request.timeout,
        Some(completion.clone()),
    )?;
    let mut guard = ScheduledThreadGuard {
        scheduler,
        thread,
        armed: true,
    };
    let result = tokio::time::timeout(request.timeout, poll_fn(|cx| completion.poll(cx))).await;
    let values = match result {
        Ok(result) => {
            guard.armed = false;
            result.map_err(anyhow::Error::msg)?
        }
        Err(_) => bail!("similar releases handler did not complete within its timeout"),
    };
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let candidates = decode_similar_releases_result(&vm, values.first(), request.max_candidates)?;
    Ok(SimilarReleasesDispatchResult { candidates })
}

fn similar_releases_handler(
    vm: &harmony_luau::Vm,
    handler_id: u64,
    provider_id: &str,
) -> Result<crate::plugins::metadata::MetadataCallback> {
    vm.data()
        .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?
        .get_for_provider(
            handler_id,
            provider_id,
            crate::services::EntityType::Release,
        )
        .ok_or_else(|| {
            anyhow::anyhow!(
                "similar releases handler {handler_id} not found for provider '{provider_id}'"
            )
        })
}

struct ScheduledThreadGuard {
    scheduler: Rc<LocalScheduler>,
    thread: harmony_luau::Thread,
    armed: bool,
}

impl Drop for ScheduledThreadGuard {
    fn drop(&mut self) {
        if self.armed {
            self.scheduler
                .schedule_cancel_luau_thread(self.thread.clone());
        }
    }
}

fn decode_similar_releases_result(
    vm: &harmony_luau::Vm,
    value: Option<&harmony_luau::Value>,
    max_candidates: usize,
) -> Result<Vec<SimilarReleaseCandidate>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if matches!(value, harmony_luau::Value::Nil) {
        return Ok(Vec::new());
    }
    let harmony_luau::Value::Table(result) = value else {
        bail!("similar releases handler result must be a table or nil");
    };
    let harmony_luau::Value::Table(candidates) = result.get_raw(vm, "candidates")? else {
        bail!("similar releases handler result.candidates must be an array table");
    };

    let mut decoded = Vec::with_capacity(max_candidates.min(candidates.raw_len(vm)?));
    for index in 1..=max_candidates {
        let index = i32::try_from(index).context("similar release candidate index overflow")?;
        let value = candidates.get_integer_raw(vm, index)?;
        if matches!(value, harmony_luau::Value::Nil) {
            break;
        }
        decoded.push(decode_similar_release_candidate(vm, value)?);
    }
    Ok(decoded)
}

fn decode_similar_release_candidate(
    vm: &harmony_luau::Vm,
    value: harmony_luau::Value,
) -> Result<SimilarReleaseCandidate> {
    let harmony_luau::Value::Table(candidate) = value else {
        bail!("similar release candidate must be a table");
    };
    let local_id = candidate.get_raw(vm, "release_db_id")?;
    let external = candidate.get_raw(vm, "external_id")?;
    match (local_id, external) {
        (harmony_luau::Value::Integer(db_id), harmony_luau::Value::Nil) if db_id > 0 => {
            let release_id =
                bounded_luau_string(candidate.get_raw(vm, "release_id")?, "release_id")?;
            Ok(SimilarReleaseCandidate::Local {
                release_db_id: db_id,
                release_id,
            })
        }
        (harmony_luau::Value::Number(db_id), harmony_luau::Value::Nil)
            if db_id.is_finite() && db_id.fract() == 0.0 && db_id > 0.0 =>
        {
            let release_id =
                bounded_luau_string(candidate.get_raw(vm, "release_id")?, "release_id")?;
            Ok(SimilarReleaseCandidate::Local {
                release_db_id: db_id as i64,
                release_id,
            })
        }
        (harmony_luau::Value::Nil, harmony_luau::Value::Table(external)) => {
            let provider_id =
                bounded_luau_string(external.get_raw(vm, "provider_id")?, "provider_id")?;
            let id_type = bounded_luau_string(external.get_raw(vm, "id_type")?, "id_type")?;
            let id_value = bounded_luau_string(external.get_raw(vm, "id_value")?, "id_value")?;
            Ok(SimilarReleaseCandidate::External(
                SimilarReleaseExternalRef {
                    provider_id,
                    id_type,
                    id_value,
                },
            ))
        }
        _ => bail!(
            "similar release candidate must contain exactly one valid release_db_id/release_id pair or external_id"
        ),
    }
}

fn bounded_luau_string(value: harmony_luau::Value, field: &str) -> Result<String> {
    let harmony_luau::Value::String(bytes) = value else {
        bail!("similar release candidate {field} must be a string");
    };
    if bytes.is_empty() || bytes.len() > MAX_SIMILAR_RELEASE_STRING_BYTES {
        bail!(
            "similar release candidate {field} must contain 1..={MAX_SIMILAR_RELEASE_STRING_BYTES} bytes"
        );
    }
    Ok(String::from_utf8(bytes)?)
}
