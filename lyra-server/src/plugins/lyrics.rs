// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use std::fmt;

use harmony_core::{
    Danger,
    Module,
    ModuleBuilder,
    ModuleExport,
};
use harmony_luau::ModuleFunctionDescriptor;
use mlua::{
    ExternalResult,
    Lua,
    LuaSerdeExt,
    Result,
    Table,
    Value,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    STATE,
    plugins::db::{
        self,
        IdSource,
        NodeId,
        lyrics::{
            LineInput,
            LyricsDetail,
            LyricsInput,
            WordInput,
        },
    },
    plugins::{
        LUA_SERIALIZE_OPTIONS,
        caller::RequestCaller,
        from_lua_json_value,
        parse_ids,
    },
    services::metadata::lyrics as lyrics_service,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[harmony_macros::enumeration]
enum LyricsOrigin {
    User,
    Plugin,
}

impl From<IdSource> for LyricsOrigin {
    fn from(source: IdSource) -> Self {
        match source {
            IdSource::User => Self::User,
            IdSource::Plugin => Self::Plugin,
        }
    }
}

harmony_macros::compile!(type_path = LyricsOrigin, variants = true);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[harmony_macros::interface]
pub(crate) struct PluginLyricWordInput {
    pub(crate) ts_ms: u64,
    pub(crate) char_start: u32,
    pub(crate) char_end: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[harmony_macros::interface]
pub(crate) struct PluginLyricLineInput {
    pub(crate) ts_ms: u64,
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) words: Vec<PluginLyricWordInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[harmony_macros::interface]
pub(crate) struct PluginLyricsInput {
    pub(crate) id: String,
    pub(crate) language: String,
    #[serde(default)]
    pub(crate) plain_text: String,
    #[serde(default)]
    pub(crate) lines: Vec<PluginLyricLineInput>,
}

impl PluginLyricsInput {
    fn from_lyrics_input(input: LyricsInput) -> Self {
        Self {
            id: input.id,
            language: input.language,
            plain_text: input.plain_text,
            lines: input
                .lines
                .into_iter()
                .map(|line| PluginLyricLineInput {
                    ts_ms: line.ts_ms,
                    text: line.text,
                    words: line
                        .words
                        .into_iter()
                        .map(|word| PluginLyricWordInput {
                            ts_ms: word.ts_ms,
                            char_start: word.char_start,
                            char_end: word.char_end,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl PluginLyricsInput {
    pub(crate) fn into_lyrics_input(self, now_ms: u64) -> Result<LyricsInput> {
        if self.id.trim().is_empty() {
            return Err(mlua::Error::runtime("lyrics id cannot be empty"));
        }

        Ok(LyricsInput {
            id: self.id,
            provider_id: String::new(),
            language: self.language,
            plain_text: self.plain_text,
            lines: self
                .lines
                .into_iter()
                .map(|line| LineInput {
                    ts_ms: line.ts_ms,
                    text: line.text,
                    words: line
                        .words
                        .into_iter()
                        .map(|word| WordInput {
                            ts_ms: word.ts_ms,
                            char_start: word.char_start,
                            char_end: word.char_end,
                        })
                        .collect(),
                })
                .collect(),
            last_checked_at: now_ms,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[harmony_macros::interface]
struct UserLyricsUploadInput {
    content_type: String,
    body: String,
    language: Option<String>,
}

#[derive(Serialize)]
#[harmony_macros::interface]
struct LyricsInfo {
    db_id: Option<NodeId>,
    id: String,
    provider_id: String,
    language: String,
    origin: LyricsOrigin,
    plain_text: String,
    has_word_cues: bool,
    updated_at: u64,
    lines: Vec<LyricLineInfo>,
}

#[derive(Serialize)]
#[harmony_macros::interface]
struct LyricLineInfo {
    ts_ms: u64,
    text: String,
    words: Vec<LyricWordInfo>,
}

#[derive(Serialize)]
#[harmony_macros::interface]
struct LyricWordInfo {
    ts_ms: u64,
    char_start: u32,
    char_end: u32,
}

fn lyrics_detail_to_info(detail: LyricsDetail) -> LyricsInfo {
    let LyricsDetail { lyrics, lines } = detail;
    LyricsInfo {
        db_id: lyrics.db_id,
        id: lyrics.id,
        provider_id: lyrics.provider_id,
        language: lyrics.language,
        origin: lyrics.origin.into(),
        plain_text: lyrics.plain_text,
        has_word_cues: lyrics.has_word_cues,
        updated_at: lyrics.updated_at,
        lines: lines
            .into_iter()
            .map(|detail| LyricLineInfo {
                ts_ms: detail.line.ts_ms,
                text: detail.line.text,
                words: detail
                    .words
                    .into_iter()
                    .map(|word| LyricWordInfo {
                        ts_ms: word.ts_ms,
                        char_start: word.char_start,
                        char_end: word.char_end,
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn lyrics_info_to_value(lua: &Lua, info: &LyricsInfo) -> Result<Value> {
    let value = lua.to_value_with(info, LUA_SERIALIZE_OPTIONS)?;
    if let Value::Table(table) = &value {
        table.set("origin", info.origin)?;
    }
    Ok(value)
}

fn lyrics_detail_to_value(lua: &Lua, detail: LyricsDetail) -> Result<Value> {
    let info = lyrics_detail_to_info(detail);
    lyrics_info_to_value(lua, &info)
}

/// Returns the preferred lyrics for a track, or nil when none are available.
pub(crate) async fn get(
    lua: Lua,
    caller: RequestCaller,
    track_id: NodeId,
    language: Option<String>,
    require_synced: Option<bool>,
) -> Result<Value> {
    let track_db_id = DbId::from(track_id);
    if track_db_id.0 <= 0 {
        return Ok(Value::Nil);
    }

    let db = STATE.db.read().await;
    if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Ok(Value::Nil);
    }
    let detail = lyrics_service::get_preferred_detail(
        &*db,
        track_db_id,
        language.as_deref(),
        require_synced.unwrap_or(false),
    )
    .into_lua_err()?;
    match detail {
        Some(detail) => lyrics_detail_to_value(&lua, detail),
        None => Ok(Value::Nil),
    }
}

/// Parses an LRC payload (line timestamps + Enhanced-LRC word cues) into a
/// [`PluginLyricsInput`]-shaped table. The returned table has an empty `id`;
/// the plugin must stamp a provider-namespaced id before passing it to
/// [`upsert`], which rejects empty ids loudly. `language` defaults to
/// `"und"` when omitted or blank.
pub(crate) fn parse_lrc(lua: &Lua, args: (String, Option<String>)) -> Result<Value> {
    let (text, language) = args;
    let language = language
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "und".to_string());
    let now = lyrics_service::now_ms().into_lua_err()?;
    let input = lyrics_service::lrc_to_input(&text, String::new(), language, now)
        .map_err(|err| mlua::Error::runtime(err.to_string()))?;
    let plugin_input = PluginLyricsInput::from_lyrics_input(input);
    lua.to_value_with(&plugin_input, LUA_SERIALIZE_OPTIONS)
}

/// Upserts plugin-provided lyrics for a track. The provider id is always the caller's plugin id.
pub(crate) async fn upsert(
    lua: Lua,
    caller: RequestCaller,
    track_id: NodeId,
    lyrics: Table,
) -> Result<NodeId> {
    let plugin_id = caller.plugin_id.to_string();
    let now = lyrics_service::now_ms().into_lua_err()?;
    let lyrics: PluginLyricsInput = from_lua_json_value(&lua, Value::Table(lyrics))?;
    let input = lyrics.into_lyrics_input(now)?;

    let mut db = STATE.db.write().await;
    let track_db_id = DbId::from(track_id);
    if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Err(mlua::Error::runtime("track not found"));
    }
    let lyrics_db_id = lyrics_service::upsert_plugin_lyrics(&mut db, track_db_id, input, plugin_id)
        .into_lua_err()?;
    Ok(lyrics_db_id.into())
}

/// Creates or replaces the user-authored lyrics override for a track.
pub(crate) async fn upsert_user_override(
    lua: Lua,
    caller: RequestCaller,
    track_id: NodeId,
    upload: Table,
) -> Result<Value> {
    let track_db_id = DbId::from(track_id);
    let upload: UserLyricsUploadInput = from_lua_json_value(&lua, Value::Table(upload))?;
    let UserLyricsUploadInput {
        content_type,
        body,
        language,
    } = upload;

    let now = lyrics_service::now_ms().into_lua_err()?;
    let input = lyrics_service::input_from_upload(&content_type, body.as_bytes(), language, now)
        .into_lua_err()?;

    let mut db = STATE.db.write().await;
    if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Err(mlua::Error::runtime("track not found"));
    }
    let detail =
        lyrics_service::upsert_user_lyrics_by_db_id(&mut db, track_db_id, input).into_lua_err()?;
    lyrics_detail_to_value(&lua, detail)
}

/// Deletes the user-authored lyrics override for a track. Provider lyrics are left intact.
pub(crate) async fn delete_user_override_for_track(
    caller: RequestCaller,
    track_id: NodeId,
) -> Result<bool> {
    let mut db = STATE.db.write().await;
    let track_db_id = DbId::from(track_id);
    if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Ok(false);
    }
    lyrics_service::delete_user_lyrics_for_track_by_db_id(&mut db, track_db_id).into_lua_err()
}

/// Deletes every lyrics row for a track. Intended for trusted cleanup workflows.
pub(crate) async fn delete_for_track(caller: RequestCaller, track_id: NodeId) -> Result<()> {
    let mut db = STATE.db.write().await;
    let track_db_id = DbId::from(track_id);
    if !crate::routes::entity_accessible_to_principal(&*db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Ok(());
    }
    lyrics_service::delete_all_lyrics_for_track(&mut db, track_db_id).into_lua_err()
}

/// Returns true when the track has preferred lyrics available.
pub(crate) async fn has(caller: RequestCaller, track_id: NodeId) -> Result<bool> {
    let track_db_id = DbId::from(track_id);
    if track_db_id.0 <= 0 {
        return Ok(false);
    }

    let db = STATE.db.read().await;
    if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, track_db_id)
        .into_lua_err()?
    {
        return Ok(false);
    }
    let detail =
        lyrics_service::get_preferred_detail(&*db, track_db_id, None, false).into_lua_err()?;
    Ok(detail.is_some())
}

/// Returns preferred-lyrics availability for many tracks.
pub(crate) async fn has_many(lua: Lua, caller: RequestCaller, track_ids: Table) -> Result<Table> {
    let track_ids = parse_ids(track_ids)?;
    let db = STATE.db.read().await;
    let providers = db::providers::get(&*db).into_lua_err()?;

    let table = lua.create_table()?;
    for track_id in track_ids {
        if !crate::routes::entity_accessible_to_principal(&db, &caller.principal, track_id)
            .into_lua_err()?
        {
            table.set(track_id.0, false)?;
            continue;
        }
        let has_lyrics = match db::tracks::get_by_id(&*db, track_id).into_lua_err()? {
            Some(track) => {
                let candidates = db::lyrics::get_for_track(&*db, track_id).into_lua_err()?;
                lyrics_service::pick_preferred(
                    &candidates,
                    &providers,
                    None,
                    track.duration_ms,
                    false,
                )
                .is_some()
            }
            None => false,
        };
        table.set(track_id.0, has_lyrics)?;
    }
    Ok(table)
}
fn module_export() -> ModuleExport {
    ModuleBuilder::empty("lyra/lyrics", "Lyrics", "lyrics")
        .scope_id("lyra.lyrics", "Read and write track lyrics.", Danger::High)
        .interface::<PluginLyricsInput>()
        .interface::<PluginLyricLineInput>()
        .interface::<PluginLyricWordInput>()
        .interface::<UserLyricsUploadInput>()
        .interface::<LyricsInfo>()
        .interface::<LyricLineInfo>()
        .interface::<LyricWordInfo>()
        .class::<LyricsOrigin>()
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("get")
                .description("Returns the preferred lyrics for a track, or nil when none are available.")
                .param_type::<u64>("track_id")
                .param_type::<Option<String>>("language")
                .param_type::<Option<bool>>("require_synced")
                .returns::<Option<LyricsInfo>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, (track_id, language, require_synced)| async move {
                let caller = caller?;
                get(lua, caller, track_id, language, require_synced).await
            },
        )
        .sync_function(
            ModuleFunctionDescriptor::new("parse_lrc")
                .description(
                    "Parses an LRC payload (line timestamps + Enhanced-LRC word cues) into a PluginLyricsInput-shaped table.",
                )
                .param_type::<String>("text")
                .param_type::<Option<String>>("language")
                .returns::<PluginLyricsInput>(),
            |lua, args| parse_lrc(lua, args),
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("upsert")
                .description("Upserts plugin-provided lyrics for a track. The provider id is always the caller's plugin id.")
                .param_type::<u64>("track_id")
                .param_type::<PluginLyricsInput>("lyrics")
                .returns::<u64>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, (track_id, lyrics)| async move {
                let caller = caller?;
                upsert(lua, caller, track_id, lyrics).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("upsert_user_override")
                .description("Creates or replaces the user-authored lyrics override for a track.")
                .param_type::<u64>("track_id")
                .param_type::<UserLyricsUploadInput>("upload")
                .returns::<LyricsInfo>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, (track_id, upload)| async move {
                let caller = caller?;
                upsert_user_override(lua, caller, track_id, upload).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("delete_user_override_for_track")
                .description("Deletes the user-authored lyrics override for a track. Provider lyrics are left intact.")
                .param_type::<NodeId>("track_id")
                .returns::<bool>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, track_id| async move {
                let caller = caller?;
                delete_user_override_for_track(caller, track_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("delete_for_track")
                .description("Deletes every lyrics row for a track. Intended for trusted cleanup workflows.")
                .param_type::<NodeId>("track_id")
                .returns::<()>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, track_id| async move {
                let caller = caller?;
                delete_for_track(caller, track_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("has")
                .description("Returns true when the track has preferred lyrics available.")
                .param_type::<NodeId>("track_id")
                .returns::<bool>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |_, caller, track_id| async move {
                let caller = caller?;
                has(caller, track_id).await
            },
        )
        .async_function_with_prelude(
            ModuleFunctionDescriptor::new("has_many")
                .description("Returns preferred-lyrics availability for many tracks.")
                .param_type::<Vec<u64>>("track_ids")
                .returns::<std::collections::BTreeMap<u64, bool>>()
                .yields(),
            crate::plugins::caller::request_caller_from_stack,
            |lua, caller, track_ids| async move {
                let caller = caller?;
                has_many(lua, caller, track_ids).await
            },
        )
        .build()
}

pub(crate) fn get_module() -> Module {
    module_export().into_module()
}

pub(crate) fn render_luau_definition() -> std::result::Result<String, fmt::Error> {
    module_export().render_luau_definition()
}
