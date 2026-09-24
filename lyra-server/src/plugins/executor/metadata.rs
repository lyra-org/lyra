// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    future::poll_fn,
    rc::Rc,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};
use harmony_core::{
    LocalLuauTaskCompletion,
    LocalScheduler,
};

use super::{
    PluginExecutor,
    messages::{
        IdLinkDispatchContext,
        MetadataDispatchContext,
        MetadataRefreshRequest,
        MetadataRefreshResult,
        SimilarReleaseCandidate,
        SimilarReleaseExternalRef,
        SimilarReleasesDispatchRequest,
        SimilarReleasesDispatchResult,
    },
};

use crate::services::providers::{
    ID_LINK_BATCH_TIMEOUT,
    ID_LINK_CALL_BUDGET,
    IdLinkCall,
    IdLinkCallError,
    IdLinkCallResult,
};

const MAX_SIMILAR_RELEASE_STRING_BYTES: usize = 4096;
/// Calls aren't started with less than this left before the batch deadline.
const MIN_ID_LINK_CALL_TIME: Duration = Duration::from_millis(10);

impl PluginExecutor {
    pub(super) fn start_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
        reply: tokio::sync::oneshot::Sender<Result<MetadataRefreshResult>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let handler = self
            .vm
            .data()
            .get::<crate::plugins::metadata::MetadataCallbackRegistry>()
            .map_err(anyhow::Error::new)
            .and_then(|handlers| {
                handlers.get(request.handler_id).ok_or_else(|| {
                    anyhow::anyhow!("metadata handler {} not found", request.handler_id)
                })
            });
        self.start_metadata_task(
            handler,
            request.context,
            request.deadline,
            super::callbacks::CallbackReply::Refresh(reply),
            permit,
        );
    }

    pub(super) fn start_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
        reply: tokio::sync::oneshot::Sender<Result<SimilarReleasesDispatchResult>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let handler = similar_releases_handler(&self.vm, request.handler_id, &request.provider_id);
        self.start_metadata_task(
            handler,
            request.context,
            Instant::now() + request.timeout,
            super::callbacks::CallbackReply::Similar {
                reply,
                cancellation: request.cancellation,
                max_candidates: request.max_candidates,
            },
            permit,
        );
    }

    fn start_metadata_task(
        &self,
        handler: Result<crate::plugins::metadata::MetadataCallback>,
        context: serde_json::Value,
        deadline: Instant,
        reply: super::callbacks::CallbackReply,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        self.start_callback(deadline, reply, permit, |timeout, completion| {
            schedule_metadata_handler(&self.vm, handler?, context, timeout, completion)
        });
    }

    #[cfg(test)]
    pub(crate) fn dispatch_similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> Result<SimilarReleasesDispatchResult> {
        self.drive_callback(|reply, permit| self.start_similar_releases(request, reply, permit))
    }
}

fn schedule_metadata_handler(
    vm: &harmony_luau::Vm,
    handler: crate::plugins::metadata::MetadataCallback,
    request_context: serde_json::Value,
    timeout: Duration,
    completion: Rc<LocalLuauTaskCompletion>,
) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)> {
    let argument = harmony_serde::json_to_luau_owned(request_context, 0)?;
    schedule_handler(
        vm,
        &handler,
        MetadataDispatchContext,
        vec![argument],
        timeout,
        completion,
    )
}

/// Schedules `handler` on its own thread, marking the call with `marker`.
fn schedule_handler<M: Send + Sync + 'static>(
    vm: &harmony_luau::Vm,
    handler: &crate::plugins::metadata::MetadataCallback,
    marker: M,
    args: Vec<harmony_luau::Value>,
    resume_budget: Duration,
    completion: Rc<LocalLuauTaskCompletion>,
) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)> {
    let thread = vm.create_thread(&handler.function)?;
    let scheduler = vm.data().get::<LocalScheduler>()?;
    let mut context = handler.context.clone();
    context.caller.insert(marker);
    scheduler.schedule_luau_thread_with_budget_and_completion(
        context,
        vm.clone(),
        thread.clone(),
        args,
        resume_budget,
        completion,
    );
    Ok((scheduler, thread))
}

/// Runs `handler` to completion, or cancels it at `deadline` and returns `None`.
async fn run_handler<M: Send + Sync + 'static>(
    vm: &harmony_luau::Vm,
    handler: &crate::plugins::metadata::MetadataCallback,
    marker: M,
    args: Vec<harmony_luau::Value>,
    resume_budget: Duration,
    deadline: Instant,
) -> Result<Option<Vec<harmony_luau::Value>>> {
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let (scheduler, thread) =
        schedule_handler(vm, handler, marker, args, resume_budget, completion.clone())?;
    let mut guard = ScheduledThreadGuard {
        scheduler,
        thread,
        armed: true,
    };
    let completed =
        tokio::time::timeout_at(deadline.into(), poll_fn(|cx| completion.poll(cx))).await;
    let Ok(result) = completed else {
        return Ok(None);
    };
    guard.armed = false;
    result.map(Some).map_err(anyhow::Error::msg)
}

pub(crate) async fn dispatch_similar_releases_in_vm(
    vm: harmony_luau::Vm,
    request: SimilarReleasesDispatchRequest,
) -> Result<SimilarReleasesDispatchResult> {
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let handler = similar_releases_handler(&vm, request.handler_id, &request.provider_id)?;
    let argument = harmony_serde::json_to_luau_owned(request.context, 0)?;
    let values = run_handler(
        &vm,
        &handler,
        MetadataDispatchContext,
        vec![argument],
        request.timeout,
        Instant::now() + request.timeout,
    )
    .await?
    .context("similar releases handler did not complete within its timeout")?;
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let candidates = decode_similar_releases_result(&vm, values.first(), request.max_candidates)?;
    Ok(SimilarReleasesDispatchResult { candidates })
}

/// Runs the calls one at a time, each resume capped by the call budget and
/// the batch deadline, so CPU-bound generators can't stall the VM past it.
/// Once a generator times out, its remaining calls are skipped.
pub(crate) async fn dispatch_id_links_in_vm(
    vm: harmony_luau::Vm,
    provider_id: String,
    calls: Vec<IdLinkCall>,
) -> Result<Vec<IdLinkCallResult>> {
    if calls.iter().any(|call| call.vm_id != vm.id()) {
        bail!("id link generators of provider '{provider_id}' belong to another plugin runtime");
    }
    let handlers = vm
        .data()
        .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
    let deadline = Instant::now() + ID_LINK_BATCH_TIMEOUT;
    let mut timed_out = HashSet::new();
    let mut results = Vec::with_capacity(calls.len());
    for call in &calls {
        let budget = id_link_call_budget(deadline.saturating_duration_since(Instant::now()));
        let result = match budget {
            Some(budget) if !timed_out.contains(&call.handler_id) => {
                run_id_link(&vm, &handlers, &provider_id, call, budget, deadline).await
            }
            _ => Err(IdLinkCallError::NotRun),
        };
        if result == Err(IdLinkCallError::TimedOut) {
            timed_out.insert(call.handler_id);
        }
        results.push(result);
    }
    Ok(results)
}

/// The full per-call budget, clamped to what's left of the batch; `None`
/// when too little is left to start the call.
fn id_link_call_budget(remaining: Duration) -> Option<Duration> {
    (remaining >= MIN_ID_LINK_CALL_TIME).then(|| remaining.min(ID_LINK_CALL_BUDGET))
}

async fn run_id_link(
    vm: &harmony_luau::Vm,
    handlers: &crate::plugins::metadata::MetadataCallbackRegistry,
    provider_id: &str,
    call: &IdLinkCall,
    budget: Duration,
    deadline: Instant,
) -> IdLinkCallResult {
    let failed = |error: String| IdLinkCallError::Failed(error);
    let handler = handlers
        .get_for_provider(call.handler_id, provider_id, call.entity)
        .ok_or_else(|| failed(format!("id link generator {} not found", call.handler_id)))?;
    let ctx = id_link_context(vm, &handler, call).map_err(|error| failed(format!("{error:#}")))?;
    let id = harmony_luau::Value::String(call.id.clone().into_bytes());
    let outcome = run_handler(
        vm,
        &handler,
        IdLinkDispatchContext,
        vec![id, ctx],
        budget,
        deadline,
    )
    .await;
    id_link_outcome(outcome, budget)
}

/// Only a call that used up the full per-call budget counts as timed out.
/// One cut short by the batch deadline, whether interrupted on a clamped
/// budget or still waiting, is `NotRun`, so it doesn't suspend its generator.
fn id_link_outcome(
    outcome: Result<Option<Vec<harmony_luau::Value>>>,
    budget: Duration,
) -> IdLinkCallResult {
    match outcome {
        Ok(Some(values)) => decode_id_link(values.into_iter().next()),
        Ok(None) => Err(IdLinkCallError::NotRun),
        // Task failures reach us as text; the busy-generator executor test
        // fails if the interrupt message stops coming through.
        Err(error) if error.to_string().contains(harmony_luau::INTERRUPTED_ERROR) => {
            if budget == ID_LINK_CALL_BUDGET {
                Err(IdLinkCallError::TimedOut)
            } else {
                Err(IdLinkCallError::NotRun)
            }
        }
        Err(error) => Err(IdLinkCallError::Failed(format!("{error:#}"))),
    }
}

/// Built only from `IdLinkCall`, whose fields all feed the link cache key,
/// so cached results can't come from a different context.
fn id_link_context(
    vm: &harmony_luau::Vm,
    handler: &crate::plugins::metadata::MetadataCallback,
    call: &IdLinkCall,
) -> Result<harmony_luau::Value> {
    use harmony_luau::{
        OwnedTable,
        Value,
    };

    let string = |value: &str| Value::String(value.as_bytes().to_vec());
    let mut library = OwnedTable::with_capacity(0, 2);
    if let Some(language) = &call.library.language {
        library.set_field("language", string(language));
    }
    if let Some(country) = &call.library.country {
        library.set_field("country", string(country));
    }
    let mut external_ids = OwnedTable::with_capacity(0, call.external_ids.len());
    for (id_type, value) in call.external_ids.iter() {
        external_ids.set_field(id_type.clone(), string(value));
    }
    let mut ctx = OwnedTable::with_capacity(0, 4);
    ctx.set_field(
        "entity",
        crate::services::EntityType::_harmony_userdata_class().create_value(
            vm,
            &handler.function.origin,
            call.entity,
        )?,
    );
    ctx.set_field("id_type", string(&call.id_type));
    ctx.set_field("library", Value::TableData(library));
    ctx.set_field("external_ids", Value::TableData(external_ids));
    Ok(Value::TableData(ctx))
}

fn decode_id_link(value: Option<harmony_luau::Value>) -> IdLinkCallResult {
    match value {
        None | Some(harmony_luau::Value::Nil) => Ok(None),
        Some(harmony_luau::Value::String(bytes)) if bytes.is_empty() => Ok(None),
        Some(harmony_luau::Value::String(bytes)) => String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| IdLinkCallError::Failed("generator returned a non-utf-8 URL".to_string())),
        Some(other) => Err(IdLinkCallError::Failed(format!(
            "generator returned {}, expected string or nil",
            other.type_name()
        ))),
    }
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

pub(super) fn decode_similar_releases_result(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calls_start_only_with_enough_time_left_and_get_at_most_the_call_budget() {
        assert_eq!(id_link_call_budget(Duration::ZERO), None);
        assert_eq!(id_link_call_budget(Duration::from_micros(500)), None);
        assert_eq!(
            id_link_call_budget(MIN_ID_LINK_CALL_TIME),
            Some(MIN_ID_LINK_CALL_TIME)
        );
        assert_eq!(
            id_link_call_budget(Duration::from_millis(50)),
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            id_link_call_budget(ID_LINK_BATCH_TIMEOUT),
            Some(ID_LINK_CALL_BUDGET)
        );
    }

    #[test]
    fn only_a_call_that_used_the_full_budget_times_out() {
        let interrupted = || {
            Err(anyhow::anyhow!(
                "Luau task 3 failed: Luau runtime failed: {}",
                harmony_luau::INTERRUPTED_ERROR
            ))
        };
        assert_eq!(
            id_link_outcome(interrupted(), ID_LINK_CALL_BUDGET),
            Err(IdLinkCallError::TimedOut)
        );
        assert_eq!(
            id_link_outcome(interrupted(), Duration::from_millis(20)),
            Err(IdLinkCallError::NotRun),
            "interrupted on a budget clamped by the batch deadline"
        );
        assert_eq!(
            id_link_outcome(Ok(None), ID_LINK_CALL_BUDGET),
            Err(IdLinkCallError::NotRun),
            "still running at the batch deadline"
        );
        assert!(matches!(
            id_link_outcome(Err(anyhow::anyhow!("boom")), ID_LINK_CALL_BUDGET),
            Err(IdLinkCallError::Failed(_))
        ));
    }
}
