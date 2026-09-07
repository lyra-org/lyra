// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_core::LocalScheduler;
use harmony_luau as luau;

use super::{
    PluginExecutor,
    messages::{
        MixHandlerRequest,
        MixHandlerResult,
    },
};

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
            let handler = handlers
                .get(request.handler_id)
                .ok_or_else(|| anyhow::anyhow!("mix handler {} not found", request.handler_id))?;
            let argument = mix_context_value(&request)?;
            let thread = self.vm.create_thread(&handler.function)?;
            Ok::<_, anyhow::Error>((handler, thread, argument))
        })();
        let mixer_id = prepared
            .as_ref()
            .map(|(handler, _, _)| handler.mixer_id.clone())
            .unwrap_or_default();
        self.start_callback(
            std::time::Instant::now() + std::time::Duration::from_secs(300),
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

fn mix_context_value(request: &MixHandlerRequest) -> Result<luau::Value> {
    let mut table = luau::OwnedTable::with_capacity(0, 5);
    table.set_field("seed_id", luau::Value::Integer(request.seed_id));
    if let Some(limit) = request.limit {
        table.set_field("limit", luau::Value::Integer(limit as i64));
    }
    if let Some(user_id) = request.user_id {
        table.set_field("user_id", luau::Value::Integer(user_id));
    }
    if !request.recent_track_ids.is_empty() {
        let mut recent = luau::OwnedTable::with_capacity(request.recent_track_ids.len(), 0);
        for track_id in &request.recent_track_ids {
            recent.push_array(luau::Value::Integer(*track_id));
        }
        table.set_field("recent_track_ids", luau::Value::TableData(recent));
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
    Ok(luau::Value::TableData(table))
}

pub(super) fn parse_mix_result(
    vm: &luau::Vm,
    mixer_id: &str,
    values: Vec<luau::Value>,
) -> Result<MixHandlerResult> {
    let Some(value) = values.into_iter().next() else {
        return Ok(MixHandlerResult {
            track_ids: Vec::new(),
        });
    };
    let luau::Value::Table(result) = value else {
        anyhow::bail!(
            "raw mixer '{mixer_id}' returned {}, expected table",
            value.type_name()
        );
    };
    let tracks = match result.get_raw(vm, "tracks")? {
        luau::Value::Table(table) => table,
        luau::Value::Nil => {
            return Ok(MixHandlerResult {
                track_ids: Vec::new(),
            });
        }
        other => {
            anyhow::bail!(
                "raw mixer '{mixer_id}' returned tracks as {}, expected table",
                other.type_name()
            );
        }
    };

    let mut entries = Vec::new();
    for (key, value) in tracks.pairs_raw(vm)? {
        let index = match key {
            luau::Value::Integer(index) => index,
            luau::Value::Number(index) => index as i64,
            _ => continue,
        };
        let luau::Value::Table(entry) = value else {
            continue;
        };
        let track_id = match entry.get_raw(vm, "track_id")? {
            luau::Value::Integer(track_id) => track_id,
            luau::Value::Number(track_id) => track_id as i64,
            _ => continue,
        };
        entries.push((index, track_id));
        if entries.len() >= crate::services::mix::MAX_LIMIT {
            break;
        }
    }
    entries.sort_by_key(|(index, _)| *index);
    Ok(MixHandlerResult {
        track_ids: entries.into_iter().map(|(_, track_id)| track_id).collect(),
    })
}
