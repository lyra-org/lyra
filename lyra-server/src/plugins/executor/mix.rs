// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::{
    Context,
    Result,
};
use harmony_core::{
    LocalScheduler,
    UserDataBinding,
};
use harmony_luau as luau;

use super::PluginExecutor;
use crate::services::plugin_dispatch::{
    MixHandlerRequest,
    MixHandlerResult,
    MixHandlerTrack,
};

/// How long a mixer handler may run. Requests cut it to their own budget.
const MIX_HANDLER_BUDGET: std::time::Duration = std::time::Duration::from_secs(300);

impl PluginExecutor {
    pub(super) fn start_mix_handler(
        &self,
        request: MixHandlerRequest,
        reply: tokio::sync::oneshot::Sender<Result<MixHandlerResult>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let prepared = (|| {
            let handlers = self
                .vm
                .data()
                .get::<crate::plugins::mix::MixCallbackRegistry>()?;
            let handler = handlers.get(request.handler)?;
            let argument = mix_context_value(&self.vm, &handler, &request)?;
            let thread = self.vm.create_thread(&handler.function)?;
            Ok::<_, anyhow::Error>((handler, thread, argument))
        })();
        let mixer_id = prepared
            .as_ref()
            .map(|(handler, _, _)| handler.mixer_id.clone())
            .unwrap_or_default();
        self.start_callback(
            std::time::Instant::now() + MIX_HANDLER_BUDGET,
            super::callbacks::CallbackReply::Mix { reply, mixer_id },
            permit,
            |timeout, completion| {
                let (handler, thread, argument) = prepared?;
                let scheduler = self.vm.data().get::<LocalScheduler>()?;
                scheduler.schedule_luau_thread_with_budget_and_completion(
                    handler.context.clone(),
                    self.vm.clone(),
                    thread.clone(),
                    vec![argument],
                    timeout,
                    completion,
                );
                Ok((scheduler, thread))
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn dispatch_mix_handler(
        &self,
        request: MixHandlerRequest,
    ) -> Result<MixHandlerResult> {
        self.drive_callback(|reply, permit| self.start_mix_handler(request, reply, permit))
    }
}

/// Runs the mixer on `vm`, the VM making the call, as a queued dispatch would.
pub(super) async fn dispatch_mix_in_vm(
    vm: &luau::Vm,
    request: MixHandlerRequest,
) -> Result<MixHandlerResult> {
    let handler = vm
        .data()
        .get::<crate::plugins::mix::MixCallbackRegistry>()?
        .get(request.handler)?;
    let argument = mix_context_value(vm, &handler, &request)?;
    let values = super::metadata::run_function(
        vm,
        &handler.function,
        handler.context.clone(),
        vec![argument],
        MIX_HANDLER_BUDGET,
        std::time::Instant::now() + MIX_HANDLER_BUDGET,
    )
    .await?
    .context("mixer handler did not complete within its budget")?;
    parse_mix_result(&handler.mixer_id, values)
}

fn mix_context_value(
    vm: &luau::Vm,
    handler: &crate::plugins::mix::MixCallback,
    request: &MixHandlerRequest,
) -> Result<luau::Value> {
    let mut seeds = luau::OwnedTable::with_capacity(request.seeds.len(), 0);
    for seed in &request.seeds {
        let mut entry = luau::OwnedTable::with_capacity(0, 3);
        entry.set_field("id", seed.id.as_str());
        entry.set_field(
            "type",
            crate::plugins::types::LuauMixSeedType::class().create_value(
                vm,
                &handler.function.origin,
                seed.seed_type.into(),
            )?,
        );
        entry.set_field("exclude", seed.exclude);
        seeds.push_array(entry);
    }
    let mut table = luau::OwnedTable::with_capacity(0, 4);
    table.set_field("seeds", seeds);
    if let Some(query) = &request.query {
        table.set_field("q", query.as_str());
    }
    if let Some(limit) = request.limit {
        table.set_field("limit", limit as i64);
    }
    if !request.options.is_empty() {
        table.set_field(
            "options",
            harmony_serde::json_to_luau_owned(
                serde_json::Value::Object(request.options.clone()),
                0,
            )?,
        );
    }
    Ok(table.into())
}

pub(super) fn parse_mix_result(
    mixer_id: &str,
    values: Vec<luau::Value>,
) -> Result<MixHandlerResult> {
    let Some(value) = values.into_iter().next() else {
        return Ok(MixHandlerResult { tracks: Vec::new() });
    };
    let luau::Value::Table(result) = value else {
        anyhow::bail!(
            "raw mixer '{mixer_id}' returned {}, expected table",
            value.type_name()
        );
    };
    let tracks = match result.get("tracks")? {
        luau::Value::Table(table) => table,
        luau::Value::Nil => return Ok(MixHandlerResult { tracks: Vec::new() }),
        other => {
            anyhow::bail!(
                "raw mixer '{mixer_id}' returned tracks as {}, expected table",
                other.type_name()
            );
        }
    };

    let mut entries = Vec::new();
    for (key, value) in tracks.pairs()? {
        let index = match key {
            luau::Value::Integer(index) => index,
            luau::Value::Number(index) => index as i64,
            _ => continue,
        };
        let luau::Value::Table(entry) = value else {
            continue;
        };
        let luau::Value::String(track_id) = entry.get("track_id")? else {
            continue;
        };
        let Ok(track_id) = String::from_utf8(track_id) else {
            continue;
        };
        let score = match entry.get("score")? {
            luau::Value::Nil => None,
            luau::Value::Integer(score) => Some(score as f64),
            luau::Value::Number(score) => Some(score),
            other => anyhow::bail!(
                "raw mixer '{mixer_id}' returned score {} for track '{track_id}', expected number",
                other.type_name()
            ),
        };
        if let Some(score) = score
            && !(0.0..=1.0).contains(&score)
        {
            anyhow::bail!(
                "raw mixer '{mixer_id}' returned score {score} for track '{track_id}', expected 0 to 1"
            );
        }
        entries.push((index, MixHandlerTrack { track_id, score }));
        if entries.len() >= crate::services::mix::MAX_LIMIT {
            break;
        }
    }
    entries.sort_by_key(|(index, _)| *index);
    Ok(MixHandlerResult {
        tracks: entries.into_iter().map(|(_, track)| track).collect(),
    })
}
