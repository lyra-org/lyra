// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    rc::Rc,
    sync::Arc,
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
    ResumeBound,
    ResumeLimit,
    UserDataBinding,
};

use super::CallChain;
use super::{
    PluginExecutor,
    messages::{
        IdLinkDispatchContext,
        MetadataDispatchContext,
    },
};
use crate::plugins::state::VmScope;
use crate::services::plugin_dispatch::{
    CallSource,
    DispatchNotStarted,
    MetadataRefreshRequest,
    MetadataRefreshResult,
    MixHandlerRequest,
    MixHandlerResult,
    PluginCallbackHandle,
    RuntimeCalls,
    SimilarReleaseCandidate,
    SimilarReleaseExternalRef,
    SimilarReleasesDispatchRequest,
    SimilarReleasesDispatchResult,
    TransitionWait,
};
use crate::services::plugin_scope::PluginId;

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
            .and_then(|handlers| handlers.get(request.handler));
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
        let handler = similar_releases_handler(&self.vm, request.handler, &request.provider_id);
        self.start_metadata_task(
            handler,
            request.context,
            self.now() + request.timeout,
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
        let plugin = handler
            .as_ref()
            .ok()
            .and_then(|handler| handler.context.origin.plugin.clone());
        self.start_callback(deadline, reply, permit, plugin, |limit, completion| {
            schedule_metadata_handler(&self.vm, handler?, context, deadline, limit, completion)
        });
    }

    #[cfg(test)]
    pub(crate) fn dispatch_metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        self.drive_callback(|reply, permit| self.start_metadata_refresh(request, reply, permit))
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
    deadline: Instant,
    limit: ResumeLimit,
    completion: Rc<LocalLuauTaskCompletion>,
) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)> {
    let argument = harmony_serde::json_to_luau_owned(request_context, 0)?;
    let mut context = handler.context.clone();
    context.caller.insert(MetadataDispatchContext);
    context
        .caller
        .insert(CallChain::root(deadline, context.origin.plugin.clone()));
    schedule_function(
        vm,
        &handler.function,
        context,
        vec![argument],
        limit,
        completion,
    )
}

fn schedule_function(
    vm: &harmony_luau::Vm,
    function: &harmony_luau::Function,
    context: harmony_core::CallContext,
    args: Vec<harmony_luau::Value>,
    limit: ResumeLimit,
    completion: Rc<LocalLuauTaskCompletion>,
) -> Result<(Rc<LocalScheduler>, harmony_luau::Thread)> {
    let thread = vm.create_thread(function)?;
    let scheduler = vm.data().get::<LocalScheduler>()?;
    scheduler.schedule_luau_thread_with_limit_and_completion(
        context,
        vm.clone(),
        thread.clone(),
        args,
        limit,
        completion,
    );
    Ok((scheduler, thread))
}

/// [`run_function`] for `handler`, marking the call with `marker`.
async fn run_handler<M: Send + Sync + 'static>(
    vm: &harmony_luau::Vm,
    chain: &CallChain,
    handler: &crate::plugins::metadata::MetadataCallback,
    marker: M,
    args: Vec<harmony_luau::Value>,
    deadline: Instant,
    resume_budget: Option<Duration>,
) -> Result<Option<Vec<harmony_luau::Value>>> {
    let mut context = handler.context.clone();
    context.caller.insert(marker);
    run_function(
        vm,
        chain,
        &handler.function,
        context,
        args,
        deadline,
        resume_budget,
    )
    .await
}

/// A handler's raised error, whole, for a caller that reads its kind, with the bound of its limit
/// that cut it short, if it failed as interrupted.
#[derive(Debug, thiserror::Error)]
#[error("{}", .raised.message)]
struct HandlerRaised {
    raised: harmony_luau::RaisedError,
    interrupted_by: Option<ResumeBound>,
}

/// [`super::callbacks::refresh_failure`] for a handler run on the calling VM.
fn raised_refresh_failure(error: anyhow::Error) -> anyhow::Error {
    match error.downcast::<HandlerRaised>() {
        Ok(HandlerRaised { raised, .. }) => super::callbacks::refresh_failure(raised),
        Err(error) => error,
    }
}

/// Runs `function` and returns what it returned, or `None` unless it ended before `deadline`, by
/// [`super::now`], as a handler the executor starts must: one still running then is cancelled.
/// Dropping the future cancels it too. No resume of it runs past the deadline, nor past that of
/// the handler `chain` began with, nor for longer than `resume_budget`, if it has one.
pub(super) async fn run_function(
    vm: &harmony_luau::Vm,
    chain: &CallChain,
    function: &harmony_luau::Function,
    context: harmony_core::CallContext,
    args: Vec<harmony_luau::Value>,
    deadline: Instant,
    resume_budget: Option<Duration>,
) -> Result<Option<Vec<harmony_luau::Value>>> {
    let running = vm.data().get::<super::callbacks::RunningHandlers>()?;
    let plugin = context.origin.plugin.clone();
    // A plugin being replaced takes no new calls, except from a chain it is already running in.
    if running.is_quiesced(plugin.as_deref())
        && !plugin.as_deref().is_some_and(|plugin| chain.admits(plugin))
    {
        return Err(DispatchNotStarted.into());
    }
    let mut context = context;
    context.caller.insert(chain.admitting(plugin.as_ref()));
    let _running = running.enter(plugin);
    let completion = Rc::new(LocalLuauTaskCompletion::default());
    let limit = ResumeLimit::until(deadline);
    let limit = match resume_budget {
        Some(budget) => limit.with_budget(budget),
        None => limit,
    };
    let (scheduler, thread) = schedule_function(
        vm,
        function,
        context,
        args,
        chain.limit_resumes(limit),
        completion.clone(),
    )?;
    let expired = scheduler.sleep_until(deadline);
    let mut guard = ScheduledThreadGuard {
        scheduler,
        thread,
        armed: true,
    };
    let completed = tokio::select! {
        biased;
        result = completion.wait() => Some(result),
        () = expired => None,
    };
    let Some(result) = completed else {
        return Ok(None);
    };
    guard.armed = false;
    if completion.ended_at().is_some_and(|ended| ended >= deadline) {
        return Ok(None);
    }
    result.map(Some).map_err(|raised| {
        HandlerRaised {
            raised,
            interrupted_by: completion.interrupted_by(),
        }
        .into()
    })
}

/// How a host function reaches plugin handlers: on the VM making the call, which the executor is
/// already running, with no queue slot. Queueing to it could wait on the very thread doing the
/// waiting, or be refused behind a drain.
#[derive(Clone)]
pub(crate) struct VmCalls {
    vm: harmony_luau::Vm,
    scope: Rc<VmScope>,
    caller: Option<PluginId>,
    chain: CallChain,
    wait: TransitionWait,
}

impl VmCalls {
    pub(crate) fn of(
        vm: &harmony_luau::Vm,
        context: &harmony_luau::CallContext,
    ) -> harmony_luau::runtime::Result<Self> {
        let now = super::now(vm)?;
        let chain = CallChain::of(context, now);
        // The wait is Tokio's, which keeps its own time: it gets what is left of the chain's.
        let wait = TransitionWait::Until(
            tokio::time::Instant::now() + chain.wait_deadline(now).saturating_duration_since(now),
        );
        Ok(Self {
            vm: vm.clone(),
            scope: VmScope::of(vm)?,
            caller: context
                .origin
                .plugin
                .clone()
                .map(PluginId::new)
                .transpose()?,
            chain,
            wait,
        })
    }
}

impl CallSource for VmCalls {
    type Calls = Self;

    async fn reach(&self, target: &str) -> Option<Self> {
        crate::services::STATE
            .generation()
            .settled_for_nested(self.chain.admitted(), target, &self.wait)
            .await
            .ok()?;
        Some(self.clone())
    }

    fn current(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn in_transition(&self, target: &str) -> bool {
        crate::services::STATE
            .generation()
            .nested_in_transition(self.chain.admitted(), target)
    }
}

impl RuntimeCalls for VmCalls {
    async fn providers(
        &self,
    ) -> Arc<tokio::sync::RwLock<crate::services::providers::ProviderRegistry>> {
        self.scope.provider_view(self.caller.as_ref()).await
    }

    async fn mixers(&self) -> Arc<tokio::sync::RwLock<crate::services::mix::MixRegistry>> {
        self.scope.mix_view(self.caller.as_ref()).await
    }

    async fn metadata_refresh(
        &self,
        request: MetadataRefreshRequest,
    ) -> Result<MetadataRefreshResult> {
        let handler = self
            .vm
            .data()
            .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?
            .get(request.handler)?;
        let argument = harmony_serde::json_to_luau_owned(request.context, 0)?;
        let values = run_handler(
            &self.vm,
            &self.chain,
            &handler,
            MetadataDispatchContext,
            vec![argument],
            request.deadline,
            None,
        )
        .await
        .map_err(raised_refresh_failure)?
        .context("metadata handler did not complete within its timeout")?;
        let values = values
            .iter()
            .map(|value| harmony_serde::luau_to_json(value).map_err(anyhow::Error::new))
            .collect::<Result<Vec<_>>>()?;
        Ok(MetadataRefreshResult { values })
    }

    async fn mix_handler(&self, request: MixHandlerRequest) -> Result<MixHandlerResult> {
        super::mix::dispatch_mix_in_vm(&self.vm, &self.chain, request).await
    }

    async fn similar_releases(
        &self,
        request: SimilarReleasesDispatchRequest,
    ) -> Result<SimilarReleasesDispatchResult> {
        dispatch_similar_releases_in_vm(self.vm.clone(), &self.chain, request).await
    }
}

async fn dispatch_similar_releases_in_vm(
    vm: harmony_luau::Vm,
    chain: &CallChain,
    request: SimilarReleasesDispatchRequest,
) -> Result<SimilarReleasesDispatchResult> {
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let handler = similar_releases_handler(&vm, request.handler, &request.provider_id)?;
    let argument = harmony_serde::json_to_luau_owned(request.context, 0)?;
    let deadline = super::now(&vm)? + request.timeout;
    let values = run_handler(
        &vm,
        chain,
        &handler,
        MetadataDispatchContext,
        vec![argument],
        deadline,
        None,
    )
    .await?
    .context("similar releases handler did not complete within its timeout")?;
    if request.cancellation.is_cancelled() {
        bail!("metadata handler dispatch was cancelled");
    }
    let candidates = decode_similar_releases_result(values.first(), request.max_candidates)?;
    Ok(SimilarReleasesDispatchResult { candidates })
}

/// Runs the calls one at a time, each resume capped by the call budget and by the batch deadline,
/// so CPU-bound generators can't stall the VM past it. Once a generator times out, its remaining
/// calls are skipped.
pub(crate) async fn dispatch_id_links_in_vm(
    vm: harmony_luau::Vm,
    chain: CallChain,
    provider_id: String,
    calls: Vec<IdLinkCall>,
) -> Result<Vec<IdLinkCallResult>> {
    // A listing doesn't wait on a plugin being replaced: its links are left out of this one, and
    // not remembered.
    if crate::services::STATE
        .generation()
        .nested_in_transition(chain.admitted(), &provider_id)
    {
        return Ok(vec![Err(IdLinkCallError::NotRun); calls.len()]);
    }
    let handlers = vm
        .data()
        .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?;
    // The batch ends with the handler that asked for it, if that is sooner.
    let batch_deadline = super::now(&vm)? + ID_LINK_BATCH_TIMEOUT;
    let deadline = chain
        .deadline()
        .map_or(batch_deadline, |deadline| deadline.min(batch_deadline));
    let mut timed_out = HashSet::new();
    let mut results = Vec::with_capacity(calls.len());
    for call in &calls {
        let left = deadline.saturating_duration_since(super::now(&vm)?);
        let result = if left >= MIN_ID_LINK_CALL_TIME && !timed_out.contains(&call.handler) {
            run_id_link(&vm, &chain, &handlers, &provider_id, call, deadline).await
        } else {
            Err(IdLinkCallError::NotRun)
        };
        if result == Err(IdLinkCallError::TimedOut) {
            timed_out.insert(call.handler);
        }
        results.push(result);
    }
    Ok(results)
}

async fn run_id_link(
    vm: &harmony_luau::Vm,
    chain: &CallChain,
    handlers: &crate::plugins::metadata::MetadataCallbackRegistry,
    provider_id: &str,
    call: &IdLinkCall,
    deadline: Instant,
) -> IdLinkCallResult {
    let failed = |error: String| IdLinkCallError::Failed(error);
    let handler = handlers
        .get_for_provider(call.handler, provider_id, call.entity)
        .map_err(|error| {
            if DispatchNotStarted::caused(&error) {
                IdLinkCallError::NotRun
            } else {
                failed(format!("{error:#}"))
            }
        })?;
    let ctx = id_link_context(vm, &handler, call).map_err(|error| failed(format!("{error:#}")))?;
    let id = harmony_luau::Value::String(call.id.clone().into_bytes());
    let outcome = run_handler(
        vm,
        chain,
        &handler,
        IdLinkDispatchContext,
        vec![id, ctx],
        deadline,
        Some(ID_LINK_CALL_BUDGET),
    )
    .await;
    id_link_outcome(outcome)
}

/// Only a call whose resume used up the call budget counts as timed out. One cut short by the
/// batch deadline, whether interrupted at it or not ended before it, is `NotRun`, so it doesn't
/// suspend its generator.
fn id_link_outcome(outcome: Result<Option<Vec<harmony_luau::Value>>>) -> IdLinkCallResult {
    let error = match outcome {
        Ok(Some(values)) => return decode_id_link(values.into_iter().next()),
        Ok(None) => return Err(IdLinkCallError::NotRun),
        Err(error) => error,
    };
    if DispatchNotStarted::caused(&error) {
        return Err(IdLinkCallError::NotRun);
    }
    let interrupted_by = error
        .downcast_ref::<HandlerRaised>()
        .and_then(|raised| raised.interrupted_by);
    Err(match interrupted_by {
        Some(ResumeBound::Budget) => IdLinkCallError::TimedOut,
        Some(ResumeBound::Deadline | ResumeBound::Signal) => IdLinkCallError::NotRun,
        None => IdLinkCallError::Failed(format!("{error:#}")),
    })
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
    for (scheme, value) in call.external_ids.iter() {
        external_ids.set_field(scheme.clone(), string(value));
    }
    let mut ctx = OwnedTable::with_capacity(0, 4);
    ctx.set_field(
        "entity",
        crate::plugins::types::LuauEntityType::class().create_value(
            vm,
            &handler.function.origin,
            call.entity.into(),
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
    handler: PluginCallbackHandle,
    provider_id: &str,
) -> Result<crate::plugins::metadata::MetadataCallback> {
    vm.data()
        .get::<crate::plugins::metadata::MetadataCallbackRegistry>()?
        .get_for_provider(handler, provider_id, crate::services::EntityType::Release)
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
    let harmony_luau::Value::Table(candidates) = result.get("candidates")? else {
        bail!("similar releases handler result.candidates must be an array table");
    };

    let mut decoded = Vec::with_capacity(max_candidates.min(candidates.len()?));
    for index in 1..=max_candidates {
        let index = i32::try_from(index).context("similar release candidate index overflow")?;
        let value = candidates.get_integer(index)?;
        if matches!(value, harmony_luau::Value::Nil) {
            break;
        }
        decoded.push(decode_similar_release_candidate(value)?);
    }
    Ok(decoded)
}

fn decode_similar_release_candidate(value: harmony_luau::Value) -> Result<SimilarReleaseCandidate> {
    let harmony_luau::Value::Table(candidate) = value else {
        bail!("similar release candidate must be a table");
    };
    let local_id = candidate.get("release_id")?;
    let external = candidate.get("external_id")?;
    match (local_id, external) {
        (release_id @ harmony_luau::Value::String(_), harmony_luau::Value::Nil) => {
            Ok(SimilarReleaseCandidate::Local {
                release_id: bounded_luau_string(release_id, "release_id")?,
            })
        }
        (harmony_luau::Value::Nil, harmony_luau::Value::Table(external)) => {
            let provider_id = bounded_luau_string(external.get("provider_id")?, "provider_id")?;
            let id_type = bounded_luau_string(external.get("id_type")?, "id_type")?;
            let id_value = bounded_luau_string(external.get("id_value")?, "id_value")?;
            Ok(SimilarReleaseCandidate::External(
                SimilarReleaseExternalRef {
                    provider_id,
                    id_type,
                    id_value,
                },
            ))
        }
        _ => {
            bail!("similar release candidate must contain exactly one of release_id or external_id")
        }
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

    /// A chain's deadline is by the executor's clock and the wait by Tokio's: on a manual clock
    /// an hour ahead of it, the wait is still for what the chain has left.
    #[test]
    fn a_nested_call_waits_for_a_transition_as_long_as_its_chain_has_left() -> Result<()> {
        const LEFT: Duration = Duration::from_secs(7);

        let _guard = futures::executor::block_on(crate::services::testing::runtime_test_lock());
        crate::plugins::testing::init_default_test_state()?;
        let clock = harmony_core::testing::ManualClock::new();
        let runtime = PluginExecutor::with_manual_clock(Arc::from(Vec::new()), clock.clone())?;
        clock.advance(Duration::from_secs(3600));
        let mut context = harmony_core::CallContext::default();
        context
            .caller
            .insert(CallChain::root(clock.now() + LEFT, None));

        let before = tokio::time::Instant::now();
        let calls = VmCalls::of(&runtime.vm, &context)?;
        let after = tokio::time::Instant::now();

        let TransitionWait::Until(until) = calls.wait else {
            bail!("a nested call's wait has an end");
        };
        assert!(
            before + LEFT <= until && until <= after + LEFT,
            "the wait ends {:?} from now",
            until - before
        );
        Ok(())
    }

    #[test]
    fn only_not_found_from_a_refresh_on_the_calling_vm_says_its_target_is_gone() {
        use crate::services::plugin_dispatch::RefreshTargetGone;

        let raised = |kind| {
            anyhow::Error::new(HandlerRaised {
                raised: harmony_luau::RaisedError {
                    kind,
                    message: "gone".to_string(),
                },
                interrupted_by: None,
            })
        };
        assert!(raised_refresh_failure(raised(Some("not_found"))).is::<RefreshTargetGone>());
        let failure = raised_refresh_failure(raised(None));
        assert!(!failure.is::<RefreshTargetGone>());
        assert_eq!(failure.to_string(), "gone");
    }

    #[test]
    fn only_a_call_interrupted_on_its_own_budget_times_out() {
        let failed = |interrupted_by| {
            Err(anyhow::Error::new(HandlerRaised {
                raised: harmony_luau::INTERRUPTED_ERROR.into(),
                interrupted_by,
            }))
        };
        assert_eq!(
            id_link_outcome(failed(Some(ResumeBound::Budget))),
            Err(IdLinkCallError::TimedOut)
        );
        for batch_s in [ResumeBound::Deadline, ResumeBound::Signal] {
            assert_eq!(
                id_link_outcome(failed(Some(batch_s))),
                Err(IdLinkCallError::NotRun),
                "interrupted at the batch deadline"
            );
        }
        assert_eq!(
            id_link_outcome(Ok(None)),
            Err(IdLinkCallError::NotRun),
            "still waiting at the batch deadline"
        );
        // The message alone doesn't make it an interruption.
        assert!(matches!(
            id_link_outcome(failed(None)),
            Err(IdLinkCallError::Failed(_))
        ));
        assert!(matches!(
            id_link_outcome(Err(anyhow::anyhow!("boom"))),
            Err(IdLinkCallError::Failed(_))
        ));
    }
}
