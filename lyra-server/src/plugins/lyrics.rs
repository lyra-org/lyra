// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    StringEnum,
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    plugins::{
        args::{
            self,
            Json,
            UniqueIds,
        },
        db::Db,
        ids::{
            self,
            Resolved,
        },
    },
    services::{
        auth::Principal,
        metadata::lyrics::{
            self as lyrics_service,
            payload::{
                PluginLyricLineInput,
                PluginLyricWordInput,
            },
        },
    },
};
use lyra_db::{
    Lyrics,
    lyrics::{
        LyricsDetail,
        LyricsInput,
        LyricsKind,
    },
};

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_BODY_BYTES",
    value: lyrics_service::LYRICS_UPLOAD_MAX_BYTES,
    description: "The most bytes the `body` of a `set_personal` upload may have.",
}]);

#[harmony_macros::string_enum(
    name = "LyricsScope",
    description = "Who sees a lyrics entry: only its owner (`personal`) or every caller who can see its track (`shared`)."
)]
#[derive(Clone, Copy)]
enum LyricsScope {
    Personal,
    Shared,
}

#[harmony_macros::string_enum(
    name = "LyricsSource",
    description = "Where a lyrics entry came from: a user's upload (`manual`) or a metadata provider (`provider`)."
)]
#[derive(Clone, Copy)]
enum LyricsSource {
    Manual,
    Provider,
}

/// Serializes a string enum as its string.
fn as_str<S: serde::Serializer, T: StringEnum>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(value.as_str())
}

#[derive(Clone, Debug, Serialize)]
struct ParsedLyricsInput {
    language: String,
    plain_text: String,
    lines: Vec<PluginLyricLineInput>,
}

impl ParsedLyricsInput {
    fn from_lyrics_input(input: LyricsInput) -> Self {
        Self {
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalLyricsUploadInput {
    content_type: String,
    body: String,
    language: Option<String>,
}

/// The `options` argument of `by_track`.
#[derive(Default)]
struct ByTrackOptions {
    language: Option<String>,
    require_synced: bool,
    provider_id: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for ByTrackOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(
            &options,
            "options",
            &["language", "require_synced", "provider_id"],
        )?;
        Ok(Self {
            language: options.optional_string_field("language")?,
            require_synced: args::optional_bool(&options, "require_synced", "options")?
                .unwrap_or(false),
            provider_id: options.optional_string_field("provider_id")?,
        })
    }
}

impl LuauTypeInfo for ByTrackOptions {
    fn luau_type() -> LuauType {
        LuauType::named("LyricsByTrackOptions")
    }
}

/// The `options` argument of `parse_lrc`.
#[derive(Default)]
struct ParseLrcOptions {
    language: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for ParseLrcOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["language"])?;
        Ok(Self {
            language: options.optional_string_field("language")?,
        })
    }
}

impl LuauTypeInfo for ParseLrcOptions {
    fn luau_type() -> LuauType {
        LuauType::named("ParseLrcOptions")
    }
}

#[derive(Serialize)]
struct LyricsSummary {
    id: String,
    provider_id: Option<String>,
    provider_lyrics_id: Option<String>,
    language: String,
    #[serde(serialize_with = "as_str")]
    scope: LyricsScope,
    #[serde(serialize_with = "as_str")]
    source: LyricsSource,
    synced: bool,
    has_word_cues: bool,
    updated_at_ms: u64,
}

impl LyricsSummary {
    fn new(lyrics: &Lyrics, duration_ms: Option<u64>) -> Self {
        Self {
            id: lyrics.id.clone(),
            provider_id: lyrics.provider_id.clone(),
            provider_lyrics_id: lyrics.provider_lyrics_id.clone(),
            language: lyrics.language.clone(),
            scope: match lyrics.kind {
                LyricsKind::Personal => LyricsScope::Personal,
                LyricsKind::Shared | LyricsKind::Provider => LyricsScope::Shared,
            },
            source: match lyrics.kind {
                LyricsKind::Provider => LyricsSource::Provider,
                LyricsKind::Personal | LyricsKind::Shared => LyricsSource::Manual,
            },
            synced: lyrics_service::has_meaningful_synced(lyrics, duration_ms),
            has_word_cues: lyrics.has_word_cues,
            updated_at_ms: lyrics.updated_at_ms,
        }
    }
}

#[derive(Serialize)]
struct LyricsInfo {
    #[serde(flatten)]
    summary: LyricsSummary,
    track_id: String,
    plain_text: String,
    lines: Vec<LyricLineInfo>,
}

impl LyricsInfo {
    fn new(detail: LyricsDetail, track_id: String, duration_ms: Option<u64>) -> Self {
        let LyricsDetail { lyrics, lines } = detail;
        Self {
            summary: LyricsSummary::new(&lyrics, duration_ms),
            track_id,
            plain_text: lyrics.plain_text,
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

pub(crate) struct LyricsModule;

fn parse_lrc_input(text: String, language: Option<String>) -> anyhow::Result<ParsedLyricsInput> {
    let language = lyra_db::lyrics::normalize_language(language.as_deref().unwrap_or_default())?;
    // The check time is storage bookkeeping that the parsed content leaves out.
    let input = lyrics_service::lrc_to_input(&text, language, 0)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(ParsedLyricsInput::from_lyrics_input(input))
}

#[harmony_macros::module(
    id = "lyra/lyrics",
    description = "Track lyrics: the entries metadata providers fetch, the one shared entry users with the manage_metadata permission upload, and each user's own personal entry. An entry is ranked by its scope and source (personal, then shared, then provider), whether its language matches a requested one, whether its synced lines cover at least half the track, its provider's position, then how recently it changed. Provider entries count while their provider is enabled.",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
impl LyricsModule {
    #[harmony(
        description = "The lyrics entries the caller can see on each of the tracks, best ranked first, keyed by track id. A track without lyrics has an empty list; tracks that are missing or hidden from the caller, and ids of anything but a track, are absent. Other users' personal entries are left out. Raises on a `language` that is not a language.",
        returns(HashMap<String, Vec<LyricsSummary>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        options: Option<ByTrackOptions>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let options = options.unwrap_or_default();
        let language_hint = lyrics_service::normalize_language_hint(options.language.as_deref())
            .map_err(crate::plugins::runtime_error)?;
        let ranking = lyrics_service::Ranking {
            provider_id: options.provider_id.as_deref(),
            language_hint: language_hint.as_deref(),
            require_synced: options.require_synced,
        };
        let db = db.read().await;
        let tracks = Resolved::of(&*db, track_ids.0)?;
        let ranked = lyrics_service::ranked_for_tracks(&db, tracks.db_ids(), &principal, ranking)
            .map_err(crate::plugins::runtime_error)?;
        let mut values = Vec::with_capacity(ranked.len());
        for (track_db_id, ranked) in ranked {
            let summaries = ranked
                .lyrics
                .iter()
                .map(|lyrics| LyricsSummary::new(lyrics, ranked.duration_ms))
                .collect::<Vec<_>>();
            values.push((
                track_db_id,
                harmony_luau::serializable_to_luau_owned(summaries)?,
            ));
        }
        Ok(tracks.keyed(values))
    }

    #[harmony(
        description = "The lyrics entries with these ids, in full, keyed by id. Entries whose track is missing or hidden from the caller, other users' personal entries, and entries of disabled providers are absent.",
        returns(HashMap<String, LyricsInfo>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let ids = Resolved::of(&*db, ids.0)?;
        let entries = lyrics_service::entries_by_ids(&db, ids.db_ids(), &principal)
            .map_err(crate::plugins::runtime_error)?;
        let mut values = Vec::with_capacity(entries.len());
        for (lyrics_db_id, entry) in entries {
            values.push((
                lyrics_db_id,
                harmony_luau::serializable_to_luau_owned(LyricsInfo::new(
                    entry.detail,
                    entry.track_id,
                    entry.duration_ms,
                ))?,
            ));
        }
        Ok(ids.keyed(values))
    }

    #[harmony(
        description = "Parses an LRC payload into lyrics content without storing it. Raises when the payload is not LRC or `options.language` is not a language.",
        returns(ParsedLyricsInput)
    )]
    fn parse_lrc(
        text: String,
        options: Option<ParseLrcOptions>,
    ) -> luau::runtime::Result<luau::Value> {
        let language = options.unwrap_or_default().language;
        harmony_luau::serializable_to_luau_owned(parse_lrc_input(text, language)?)
    }

    #[harmony(
        description = "Stores the upload as the caller's personal lyrics for the track, replacing any earlier ones, and returns the stored entry, which keeps its id across replacements. Returns nil when the track is missing or hidden from the caller. Raises when `content_type` is not `application/json`, `application/lrc`, `text/x-lrc` or `text/plain`, when `body` is over `MAX_BODY_BYTES` or does not parse, and on an invalid `language`.",
        returns(Option<LyricsInfo>)
    )]
    async fn set_personal(
        track_id: String,
        upload: Json<PersonalLyricsUploadInput>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let now = lyrics_service::now_ms().map_err(crate::plugins::runtime_error)?;
        let PersonalLyricsUploadInput {
            content_type,
            body,
            language,
        } = upload.0;
        let input =
            lyrics_service::input_from_upload(&content_type, body.as_bytes(), language, now)
                .map_err(crate::plugins::runtime_error)?;

        let mut db = db.write().await;
        let Some(track_db_id) = ids::visible_to(&db, &principal, &track_id, "Track")? else {
            return Ok(luau::Value::Nil);
        };
        let detail = lyrics_service::upsert_personal_lyrics_by_db_id(
            &mut db,
            track_db_id,
            &track_id,
            &principal,
            input,
        )
        .map_err(crate::plugins::runtime_error)?;
        let duration_ms = lyra_db::tracks::get_by_id(&*db, track_db_id)
            .map_err(crate::plugins::runtime_error)?
            .and_then(|track| track.duration_ms);
        harmony_luau::serializable_to_luau_owned(LyricsInfo::new(detail, track_id, duration_ms))
    }

    #[harmony(
        description = "Removes the caller's personal lyrics for the track, leaving shared and provider lyrics. Returns false when the track is missing, hidden from the caller, or has no personal lyrics of the caller's; those of a track the caller can no longer see are still removed, and still answer false."
    )]
    async fn remove_personal(
        track_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        let Some(track_db_id) = ids::db_id(&*db, &track_id)? else {
            return Ok(false);
        };
        let visible = ids::visible_to(&db, &principal, &track_id, "Track")?.is_some();
        let removed = lyrics_service::delete_personal_lyrics_for_track_by_db_id(
            &mut db,
            track_db_id,
            &principal,
        )
        .map_err(crate::plugins::runtime_error)?;
        Ok(visible && removed)
    }

    #[harmony(
        description = "Removes every lyrics entry of the track: the providers', the shared one and every user's personal ones. Returns false when the track is missing, hidden from the caller, or has no lyrics. Raises when the caller lacks the manage_metadata permission. Needs the `lyra.lyrics.all` scope too."
    )]
    async fn remove_all(
        track_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        crate::services::auth::require_permission(&principal, lyra_db::Permission::ManageMetadata)
            .map_err(crate::plugins::errors::auth_error)?;
        let mut db = db.write().await;
        let Some(track_db_id) = ids::visible_to(&db, &principal, &track_id, "Track")? else {
            return Ok(false);
        };
        lyrics_service::delete_all_lyrics_for_track(&mut db, track_db_id)
            .map_err(crate::plugins::runtime_error)
    }
}

impl LuauTypeInfo for ParsedLyricsInput {
    fn luau_type() -> LuauType {
        LuauType::literal("ParsedLyricsInput")
    }
}

impl DescribeInterface for ParsedLyricsInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ParsedLyricsInput", None);
        descriptor.fields.extend([
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
                ty: Vec::<LuauPluginLyricLineInput>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for PersonalLyricsUploadInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PersonalLyricsUploadInput")
    }
}

impl DescribeInterface for PersonalLyricsUploadInput {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PersonalLyricsUploadInput", None);
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

impl LuauTypeInfo for LyricsSummary {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricsSummary")
    }
}

impl LuauTypeInfo for LyricsInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("LyricsInfo")
    }
}

/// The fields a [`LyricsSummary`] and a [`LyricsInfo`] share.
fn summary_fields() -> Vec<FieldDescriptor> {
    let field = |name, ty, description| FieldDescriptor {
        name,
        ty,
        description,
    };
    vec![
        field(
            "id",
            String::luau_type(),
            Some("Kept while the entry is rewritten, as when its provider fetches it again."),
        ),
        field(
            "provider_id",
            Option::<String>::luau_type(),
            Some("The metadata provider of a `provider` entry; nil for a `manual` one."),
        ),
        field(
            "provider_lyrics_id",
            Option::<String>::luau_type(),
            Some("The provider's own ID for a `provider` entry's lyrics; nil for a `manual` one."),
        ),
        field(
            "language",
            String::luau_type(),
            Some("Lowercase ISO 639-3 code; `und` when unknown."),
        ),
        field("scope", LyricsScope::luau_type(), None),
        field("source", LyricsSource::luau_type(), None),
        field(
            "synced",
            bool::luau_type(),
            Some(
                "Whether at least two lines carry a time and the last of them reaches half the track's duration, when it is known. Lines of unsynced lyrics have `ts_ms` 0.",
            ),
        ),
        field(
            "has_word_cues",
            bool::luau_type(),
            Some("Whether any line times its words."),
        ),
        field(
            "updated_at_ms",
            u64::luau_type(),
            Some("When the content last changed."),
        ),
    ]
}

impl DescribeInterface for LyricsSummary {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LyricsSummary", None);
        descriptor.fields.extend(summary_fields());
        descriptor
    }
}

impl DescribeInterface for LyricsInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("LyricsInfo", None);
        descriptor.fields.extend(summary_fields());
        descriptor.fields.insert(
            1,
            FieldDescriptor {
                name: "track_id",
                ty: String::luau_type(),
                description: Some("The track the lyrics belong to."),
            },
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "plain_text",
                ty: String::luau_type(),
                description: Some(
                    "May be empty when the lyrics came only as synced lines; their texts joined with newlines stand in for it.",
                ),
            },
            FieldDescriptor {
                name: "lines",
                ty: Vec::<LyricLineInfo>::luau_type(),
                description: Some("In playback order; empty for plain-text-only lyrics."),
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::{
        DescribeModule,
        DescribeTypeAlias,
    };

    let mut by_track_options = InterfaceDescriptor::new("LyricsByTrackOptions", None);
    by_track_options.fields.extend([
        FieldDescriptor {
            name: "language",
            ty: Option::<String>::luau_type(),
            description: Some(
                "Ranks entries in this language, an ISO 639 code or a language name, above others of their scope and source. Default none.",
            ),
        },
        FieldDescriptor {
            name: "require_synced",
            ty: Option::<bool>::luau_type(),
            description: Some("Keeps only `synced` entries. Default `false`."),
        },
        FieldDescriptor {
            name: "provider_id",
            ty: Option::<String>::luau_type(),
            description: Some(
                "Keeps only this metadata provider's entries, leaving out manual ones. Default none.",
            ),
        },
    ]);
    let mut parse_lrc_options = InterfaceDescriptor::new("ParseLrcOptions", None);
    parse_lrc_options.fields.push(FieldDescriptor {
        name: "language",
        ty: Option::<String>::luau_type(),
        description: Some(
            "The language of the lyrics: an ISO 639 code or a language name, normalized as stored lyrics are. Default `und`.",
        ),
    });

    harmony_luau::render_definition_file_with_support(
        &LyricsModule::module_descriptor(),
        &[
            LyricsScope::type_alias_descriptor(),
            LyricsSource::type_alias_descriptor(),
        ],
        &[
            by_track_options,
            parse_lrc_options,
            ParsedLyricsInput::interface_descriptor(),
            LuauPluginLyricLineInput::interface_descriptor(),
            LuauPluginLyricWordInput::interface_descriptor(),
            PersonalLyricsUploadInput::interface_descriptor(),
            LyricsSummary::interface_descriptor(),
            LyricsInfo::interface_descriptor(),
            LyricLineInfo::interface_descriptor(),
            LyricWordInfo::interface_descriptor(),
        ],
        &[],
    )
}

/// [`PluginLyricWordInput`] as described to Luau.
pub(crate) struct LuauPluginLyricWordInput;

/// [`PluginLyricLineInput`] as described to Luau.
pub(crate) struct LuauPluginLyricLineInput;

impl LuauTypeInfo for LuauPluginLyricWordInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginLyricWordInput")
    }
}

impl DescribeInterface for LuauPluginLyricWordInput {
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

impl LuauTypeInfo for LuauPluginLyricLineInput {
    fn luau_type() -> LuauType {
        LuauType::literal("PluginLyricLineInput")
    }
}

impl DescribeInterface for LuauPluginLyricLineInput {
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
                ty: Vec::<LuauPluginLyricWordInput>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

const _: fn(&PluginLyricWordInput) = |value| {
    let _ = (&value.ts_ms, &value.char_start, &value.char_end);
};

const _: fn(&PluginLyricLineInput) = |value| {
    let _ = (&value.ts_ms, &value.text, &value.words);
};
