// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;
use std::sync::Arc;

use agdb::DbId;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeUserData,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use serde::{
    Deserialize,
    Serialize,
    de::DeserializeOwned,
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
    services::metadata::lyrics as lyrics_service,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

impl LuauTypeInfo for LyricsOrigin {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricsOrigin")
    }
}

impl DescribeUserData for LyricsOrigin {
    fn class_descriptor() -> ClassDescriptor {
        let mut descriptor = ClassDescriptor::new("LyricsOrigin", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "User",
                ty: LyricsOrigin::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "Plugin",
                ty: LyricsOrigin::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PluginLyricWordInput {
    pub(crate) ts_ms: u64,
    pub(crate) char_start: u32,
    pub(crate) char_end: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PluginLyricLineInput {
    pub(crate) ts_ms: u64,
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) words: Vec<PluginLyricWordInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    pub(crate) fn into_lyrics_input(self, now_ms: u64) -> anyhow::Result<LyricsInput> {
        if self.id.trim().is_empty() {
            anyhow::bail!("lyrics id cannot be empty");
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
struct UserLyricsUploadInput {
    content_type: String,
    body: String,
    language: Option<String>,
}

#[derive(Serialize)]
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
struct LyricLineInfo {
    ts_ms: u64,
    text: String,
    words: Vec<LyricWordInfo>,
}

#[derive(Serialize)]
struct LyricWordInfo {
    ts_ms: u64,
    char_start: u32,
    char_end: u32,
}

struct LyricsModule;

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

fn parse_lrc_input(text: String, language: Option<String>) -> anyhow::Result<PluginLyricsInput> {
    let language = language
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "und".to_string());
    let now = lyrics_service::now_ms()?;
    let input = lyrics_service::lrc_to_input(&text, String::new(), language, now)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(PluginLyricsInput::from_lyrics_input(input))
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/lyrics")
        .capability("lyra.lyrics")
        .function(get_spec())
        .function(parse_lrc_spec())
        .function(upsert_spec())
        .function(upsert_user_override_spec())
        .function(delete_user_override_for_track_spec())
        .function(delete_for_track_spec())
        .function(has_spec())
        .function(has_many_spec())
        .install(|_| Ok(ModuleExport::new(LyricsModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .named_arg::<i64>("track_id")
        .named_arg::<Option<String>>("language")
        .named_arg::<Option<bool>>("require_synced")
        .returns::<Option<LyricsInfo>>()
        .call_async(Arc::new(get_callback))
}

fn parse_lrc_spec() -> FunctionSpec {
    let spec = FunctionSpec::sync_fn("parse_lrc")
        .named_arg::<String>("text")
        .named_arg::<Option<String>>("language")
        .returns::<PluginLyricsInput>();
    spec.call(parse_lrc_callback)
}

fn upsert_spec() -> FunctionSpec {
    FunctionSpec::async_fn("upsert")
        .context::<crate::plugins::auth::DispatchAuth>()
        .named_arg::<i64>("track_id")
        .named_arg::<PluginLyricsInput>("lyrics")
        .returns::<i64>()
        .call_async(Arc::new(upsert_callback))
}

fn upsert_user_override_spec() -> FunctionSpec {
    FunctionSpec::async_fn("upsert_user_override")
        .context::<crate::plugins::auth::DispatchAuth>()
        .named_arg::<i64>("track_id")
        .named_arg::<UserLyricsUploadInput>("upload")
        .returns::<LyricsInfo>()
        .call_async(Arc::new(upsert_user_override_callback))
}

fn delete_user_override_for_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("delete_user_override_for_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_id")
        .args::<i64>()
        .returns::<bool>()
        .call_async(Arc::new(delete_user_override_for_track_callback))
}

fn delete_for_track_spec() -> FunctionSpec {
    FunctionSpec::async_fn("delete_for_track")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_id")
        .args::<i64>()
        .call_async(Arc::new(delete_for_track_callback))
}

fn has_spec() -> FunctionSpec {
    FunctionSpec::async_fn("has")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_id")
        .args::<i64>()
        .returns::<bool>()
        .call_async(Arc::new(has_callback))
}

fn has_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("has_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<Vec<u64>>()
        .returns::<std::collections::BTreeMap<u64, bool>>()
        .call_async(Arc::new(has_many_callback))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let language: Option<String> = frame.args.read_optional_named("language")?;
    let require_synced: Option<bool> = frame.args.read_optional_named("require_synced")?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = DbId(track_id);
        if track_db_id.0 <= 0 {
            return Ok(luau::Value::Nil);
        }

        let db = STATE.db.read().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Nil);
        }
        let detail = lyrics_service::get_preferred_detail(
            &db,
            track_db_id,
            language.as_deref(),
            require_synced.unwrap_or(false),
        )
        .map_err(crate::plugins::runtime_error)?;

        let value = match detail {
            Some(detail) => lyrics_detail_to_luau_value(detail)?,
            None => luau::Value::Nil,
        };
        Ok(value)
    }))
}

fn parse_lrc_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let text: String = frame.args.read_named("text")?;
    let language: Option<String> = frame.args.read_optional_named("language")?;
    let parsed = parse_lrc_input(text, language).map_err(crate::plugins::runtime_error)?;
    frame.returns.write(parsed.into_luau_table())?;
    Ok(())
}

fn upsert_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let lyrics_value: luau::Value = frame.args.read_named("lyrics")?;
    let lyrics: PluginLyricsInput = from_luau_json(frame.vm, &lyrics_value)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;
    let plugin_id = frame.context.origin.plugin.clone().ok_or_else(|| {
        luau::Error::Runtime("lyrics.upsert must be called from plugin Luau code".into())
    })?;

    Ok(luau::ScheduledFuture::new(async move {
        let now = lyrics_service::now_ms().map_err(crate::plugins::runtime_error)?;
        let input = lyrics
            .into_lyrics_input(now)
            .map_err(crate::plugins::runtime_error)?;
        let track_db_id = require_positive_id(track_id, "track_id")?;

        let mut db = STATE.db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Err(crate::plugins::runtime_error("track not found"));
        }
        let lyrics_db_id = lyrics_service::upsert_plugin_lyrics(
            &mut db,
            track_db_id,
            input,
            plugin_id.to_string(),
        )
        .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Integer(lyrics_db_id.0))
    }))
}

fn upsert_user_override_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let upload_value: luau::Value = frame.args.read_named("upload")?;
    let upload: UserLyricsUploadInput = from_luau_json(frame.vm, &upload_value)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = require_positive_id(track_id, "track_id")?;
        let now = lyrics_service::now_ms().map_err(crate::plugins::runtime_error)?;
        let UserLyricsUploadInput {
            content_type,
            body,
            language,
        } = upload;
        let input =
            lyrics_service::input_from_upload(&content_type, body.as_bytes(), language, now)
                .map_err(crate::plugins::runtime_error)?;

        let mut db = STATE.db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Err(crate::plugins::runtime_error("track not found"));
        }
        let detail = lyrics_service::upsert_user_lyrics_by_db_id(&mut db, track_db_id, input)
            .map_err(crate::plugins::runtime_error)?;
        lyrics_detail_to_luau_value(detail)
    }))
}

fn delete_user_override_for_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = require_positive_id(track_id, "track_id")?;
        let mut db = STATE.db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Boolean(false));
        }
        let deleted = lyrics_service::delete_user_lyrics_for_track_by_db_id(&mut db, track_db_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(deleted))
    }))
}

fn delete_for_track_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = require_positive_id(track_id, "track_id")?;
        let mut db = STATE.db.write().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(());
        }
        lyrics_service::delete_all_lyrics_for_track(&mut db, track_db_id)
            .map_err(crate::plugins::runtime_error)?;
        Ok(())
    }))
}

fn has_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id: i64 = frame.args.read_named("track_id")?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let track_db_id = DbId(track_id);
        if track_db_id.0 <= 0 {
            return Ok(luau::Value::Boolean(false));
        }

        let db = STATE.db.read().await;
        if !crate::services::auth::access::entity_accessible(&db, &principal, track_db_id)
            .map_err(crate::plugins::runtime_error)?
        {
            return Ok(luau::Value::Boolean(false));
        }
        let detail = lyrics_service::get_preferred_detail(&db, track_db_id, None, false)
            .map_err(crate::plugins::runtime_error)?;
        Ok(luau::Value::Boolean(detail.is_some()))
    }))
}

fn has_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids_table: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = parse_db_ids(frame.vm, &track_ids_table)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = STATE.db.read().await;
        let providers = db::providers::get(&db).map_err(crate::plugins::runtime_error)?;
        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, track_ids.len());

        for track_id in track_ids {
            let has_lyrics =
                if !crate::services::auth::access::entity_accessible(&db, &principal, track_id)
                    .map_err(crate::plugins::runtime_error)?
                {
                    false
                } else {
                    match db::tracks::get_by_id(&db, track_id)
                        .map_err(crate::plugins::runtime_error)?
                    {
                        Some(track) => {
                            let candidates = db::lyrics::get_for_track(&db, track_id)
                                .map_err(crate::plugins::runtime_error)?;
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
                    }
                };
            table.set_key(
                luau::Value::Integer(track_id.0),
                luau::Value::Boolean(has_lyrics),
            );
        }

        Ok(luau::Value::TableData(table))
    }))
}
impl PluginLyricsInput {
    fn into_luau_table(self) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::with_capacity(0, 4);
        table.set_field("id", luau::Value::String(self.id.into_bytes()));
        table.set_field("language", luau::Value::String(self.language.into_bytes()));
        table.set_field(
            "plain_text",
            luau::Value::String(self.plain_text.into_bytes()),
        );
        table.set_field(
            "lines",
            luau::Value::TableData(lyric_lines_table(self.lines)),
        );
        table
    }
}
fn lyric_lines_table(lines: Vec<PluginLyricLineInput>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(lines.len(), 0);
    for line in lines {
        table.push_array(luau::Value::TableData(lyric_line_table(line)));
    }
    table
}
fn lyric_line_table(line: PluginLyricLineInput) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field("ts_ms", luau::Value::Number(line.ts_ms as f64));
    table.set_field("text", luau::Value::String(line.text.into_bytes()));
    table.set_field(
        "words",
        luau::Value::TableData(lyric_words_table(line.words)),
    );
    table
}
fn lyric_words_table(words: Vec<PluginLyricWordInput>) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(words.len(), 0);
    for word in words {
        table.push_array(luau::Value::TableData(lyric_word_table(word)));
    }
    table
}
fn lyric_word_table(word: PluginLyricWordInput) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field("ts_ms", luau::Value::Number(word.ts_ms as f64));
    table.set_field(
        "char_start",
        luau::Value::Number(f64::from(word.char_start)),
    );
    table.set_field("char_end", luau::Value::Number(f64::from(word.char_end)));
    table
}

fn lyrics_detail_to_luau_value(detail: LyricsDetail) -> luau::runtime::Result<luau::Value> {
    harmony_luau::serializable_to_luau_owned(lyrics_detail_to_info(detail))
}

fn from_luau_json<T>(vm: &luau::Vm, value: &luau::Value) -> luau::runtime::Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(harmony_serde::luau_to_json(vm, value, 0)?)
        .map_err(crate::plugins::runtime_error)
}

fn require_positive_id(value: i64, field_name: &str) -> luau::runtime::Result<DbId> {
    if value <= 0 {
        return Err(crate::plugins::runtime_error(format!(
            "{field_name} must be a positive id"
        )));
    }
    Ok(DbId(value))
}

fn parse_db_ids(vm: &luau::Vm, table: &luau::Table) -> luau::runtime::Result<Vec<DbId>> {
    let mut values = Vec::new();
    for (key, value) in table.pairs_raw(vm)? {
        let Some(index) = array_index(key) else {
            continue;
        };
        let Some(id) = db_id_value(value)? else {
            continue;
        };
        values.push((index, id));
    }
    values.sort_by_key(|(index, _)| *index);

    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for (_, id) in values {
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn array_index(value: luau::Value) -> Option<i64> {
    match value {
        luau::Value::Integer(index) if index > 0 => Some(index),
        luau::Value::Number(index) if index.is_finite() && index.fract() == 0.0 && index > 0.0 => {
            Some(index as i64)
        }
        _ => None,
    }
}

fn db_id_value(value: luau::Value) -> luau::runtime::Result<Option<DbId>> {
    let id = match value {
        luau::Value::Integer(id) => id,
        luau::Value::Number(id) if id.is_finite() && id.fract() == 0.0 => id as i64,
        luau::Value::Nil => return Ok(None),
        _ => return Err(crate::plugins::runtime_error("expected numeric id")),
    };

    if id <= 0 {
        return Ok(None);
    }
    Ok(Some(DbId(id)))
}

impl LuauTypeInfo for PluginLyricWordInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginLyricWordInput")
    }
}

impl DescribeInterface for PluginLyricWordInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PluginLyricWordInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "ts_ms",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "char_start",
                ty: u32::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "char_end",
                ty: u32::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PluginLyricLineInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginLyricLineInput")
    }
}

impl DescribeInterface for PluginLyricLineInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PluginLyricLineInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "ts_ms",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "text",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "words",
                ty: Vec::<PluginLyricWordInput>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PluginLyricsInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginLyricsInput")
    }
}

impl DescribeInterface for PluginLyricsInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PluginLyricsInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "language",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "plain_text",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "lines",
                ty: Vec::<PluginLyricLineInput>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for UserLyricsUploadInput {
    fn luau_type() -> LuauType {
        LuauType::literal("UserLyricsUploadInput")
    }
}

impl DescribeInterface for UserLyricsUploadInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("UserLyricsUploadInput", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "content_type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "body",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "language",
                ty: Option::<String>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LyricsInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricsInfo")
    }
}

impl DescribeInterface for LyricsInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LyricsInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "db_id",
                ty: Option::<NodeId>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "provider_id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "language",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "origin",
                ty: LyricsOrigin::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "plain_text",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "has_word_cues",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "updated_at",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "lines",
                ty: Vec::<LyricLineInfo>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LyricLineInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricLineInfo")
    }
}

impl DescribeInterface for LyricLineInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LyricLineInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "ts_ms",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "text",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "words",
                ty: Vec::<LyricWordInfo>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LyricWordInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricWordInfo")
    }
}

impl DescribeInterface for LyricWordInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LyricWordInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "ts_ms",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "char_start",
                ty: u32::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "char_end",
                ty: u32::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Lyrics",
        local_name: "lyrics",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: Some(
                    "Returns the preferred lyrics for a track, or nil when none are available.",
                ),
                params: vec![
                    param("track_id", NodeId::luau_type()),
                    param("language", Option::<String>::luau_type()),
                    param("require_synced", Option::<bool>::luau_type()),
                ],
                returns: vec![Option::<LyricsInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["parse_lrc"],
                description: Some("Parses an LRC payload into a PluginLyricsInput-shaped table."),
                params: vec![
                    param("text", String::luau_type()),
                    param("language", Option::<String>::luau_type()),
                ],
                returns: vec![PluginLyricsInput::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["upsert"],
                description: Some(
                    "Upserts plugin-provided lyrics for a track. The provider id is always the caller's plugin id.",
                ),
                params: vec![
                    param("track_id", NodeId::luau_type()),
                    param("lyrics", PluginLyricsInput::luau_type()),
                ],
                returns: vec![NodeId::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["upsert_user_override"],
                description: Some(
                    "Creates or replaces the user-authored lyrics override for a track.",
                ),
                params: vec![
                    param("track_id", NodeId::luau_type()),
                    param("upload", UserLyricsUploadInput::luau_type()),
                ],
                returns: vec![LyricsInfo::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["delete_user_override_for_track"],
                description: Some(
                    "Deletes the user-authored lyrics override for a track. Provider lyrics are left intact.",
                ),
                params: vec![param("track_id", NodeId::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["delete_for_track"],
                description: Some(
                    "Deletes every lyrics row for a track. Intended for trusted cleanup workflows.",
                ),
                params: vec![param("track_id", NodeId::luau_type())],
                returns: vec![],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has"],
                description: Some("Returns true when the track has preferred lyrics available."),
                params: vec![param("track_id", NodeId::luau_type())],
                returns: vec![bool::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["has_many"],
                description: Some("Returns preferred-lyrics availability for many tracks."),
                params: vec![param("track_ids", Vec::<u64>::luau_type())],
                returns: vec![LuauType::map(u64::luau_type(), bool::luau_type())],
                yields: true,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            PluginLyricsInput::interface_descriptor(),
            PluginLyricLineInput::interface_descriptor(),
            PluginLyricWordInput::interface_descriptor(),
            UserLyricsUploadInput::interface_descriptor(),
            LyricsInfo::interface_descriptor(),
            LyricLineInfo::interface_descriptor(),
            LyricWordInfo::interface_descriptor(),
        ],
        &[LyricsOrigin::class_descriptor()],
    )
}
