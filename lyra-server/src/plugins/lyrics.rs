// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::Arc;

use agdb::DbId;
use harmony_core::LuaAsyncExt;
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
    db::{
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

struct LyricsModule;

#[harmony_macros::module(
    plugin_scoped,
    name = "Lyrics",
    local = "lyrics",
    path = "lyra/lyrics",
    interfaces(
        PluginLyricsInput,
        PluginLyricLineInput,
        PluginLyricWordInput,
        UserLyricsUploadInput,
        LyricsInfo,
        LyricLineInfo,
        LyricWordInfo
    ),
    classes(LyricsOrigin)
)]
impl LyricsModule {
    /// Returns the preferred lyrics for a track, or nil when none are available.
    #[harmony(args(track_id: u64, language: Option<String>, require_synced: Option<bool>), returns(Option<LyricsInfo>))]
    pub(crate) async fn get(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        track_id: NodeId,
        language: Option<String>,
        require_synced: Option<bool>,
    ) -> Result<Value> {
        let track_db_id = DbId::from(track_id);
        if track_db_id.0 <= 0 {
            return Ok(Value::Nil);
        }

        let db = STATE.db.read().await;
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
    #[harmony(args(text: String, language: Option<String>), returns(PluginLyricsInput))]
    pub(crate) async fn parse_lrc(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        text: String,
        language: Option<String>,
    ) -> Result<Value> {
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
    #[harmony(args(track_id: u64, lyrics: PluginLyricsInput), returns(u64))]
    pub(crate) async fn upsert(
        lua: Lua,
        plugin_id: Option<Arc<str>>,
        track_id: NodeId,
        lyrics: Table,
    ) -> Result<NodeId> {
        let plugin_id = plugin_id
            .ok_or_else(|| mlua::Error::runtime("lyrics.upsert requires plugin scope"))?
            .to_string();
        let now = lyrics_service::now_ms().into_lua_err()?;
        let lyrics: PluginLyricsInput = from_lua_json_value(&lua, Value::Table(lyrics))?;
        let input = lyrics.into_lyrics_input(now)?;

        let mut db = STATE.db.write().await;
        let lyrics_db_id =
            lyrics_service::upsert_plugin_lyrics(&mut db, DbId::from(track_id), input, plugin_id)
                .into_lua_err()?;
        Ok(lyrics_db_id.into())
    }

    /// Creates or replaces the user-authored lyrics override for a track.
    #[harmony(args(track_id: u64, upload: UserLyricsUploadInput), returns(LyricsInfo))]
    pub(crate) async fn upsert_user_override(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
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
        let input =
            lyrics_service::input_from_upload(&content_type, body.as_bytes(), language, now)
                .into_lua_err()?;

        let mut db = STATE.db.write().await;
        let detail = lyrics_service::upsert_user_lyrics_by_db_id(&mut db, track_db_id, input)
            .into_lua_err()?;
        lyrics_detail_to_value(&lua, detail)
    }

    /// Deletes the user-authored lyrics override for a track. Provider lyrics are left intact.
    pub(crate) async fn delete_user_override_for_track(
        _plugin_id: Option<Arc<str>>,
        track_id: NodeId,
    ) -> Result<bool> {
        let mut db = STATE.db.write().await;
        lyrics_service::delete_user_lyrics_for_track_by_db_id(&mut db, track_id.into())
            .into_lua_err()
    }

    /// Deletes every lyrics row for a track. Intended for trusted cleanup workflows.
    pub(crate) async fn delete_for_track(
        _plugin_id: Option<Arc<str>>,
        track_id: NodeId,
    ) -> Result<()> {
        let mut db = STATE.db.write().await;
        lyrics_service::delete_all_lyrics_for_track(&mut db, track_id.into()).into_lua_err()
    }

    /// Returns true when the track has preferred lyrics available.
    pub(crate) async fn has(_plugin_id: Option<Arc<str>>, track_id: NodeId) -> Result<bool> {
        let track_db_id = DbId::from(track_id);
        if track_db_id.0 <= 0 {
            return Ok(false);
        }

        let db = STATE.db.read().await;
        let detail =
            lyrics_service::get_preferred_detail(&*db, track_db_id, None, false).into_lua_err()?;
        Ok(detail.is_some())
    }

    /// Returns preferred-lyrics availability for many tracks.
    #[harmony(args(track_ids: Vec<u64>), returns(std::collections::BTreeMap<u64, bool>))]
    pub(crate) async fn has_many(
        lua: Lua,
        _plugin_id: Option<Arc<str>>,
        track_ids: Table,
    ) -> Result<Table> {
        let track_ids = parse_ids(track_ids)?;
        let db = STATE.db.read().await;
        let providers = db::providers::get(&*db).into_lua_err()?;

        let table = lua.create_table()?;
        for track_id in track_ids {
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
}

crate::plugins::plugin_surface_exports!(
    LyricsModule,
    "lyra.lyrics",
    "Read and write track lyrics.",
    High
);
