// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use axum::http::StatusCode;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::args::{
    self,
    UniqueIds,
};
use crate::plugins::auth::ResponseCaller;
use crate::plugins::errors::{
    ErrorKind,
    auth_error_kind,
    kinded,
};
use crate::plugins::{
    ids::Resolved,
    types::EntryInfo,
};
use crate::{
    plugins::db::{
        SystemDb,
        ViewerDb,
    },
    services::{
        auth::{
            AuthError,
            Principal,
        },
        catalog::Viewer,
        delivery::{
            self,
            DeliveryError,
            SourceDescription,
        },
        playback_sources as playback_source_service,
    },
};

use std::collections::HashMap;

#[cfg(feature = "docgen")]
struct PlaybackSourceInfo;

#[cfg(feature = "docgen")]
struct TrackStreamOptions;

#[cfg(feature = "docgen")]
struct DeliveryDescription;

/// The `options` argument of the source reads.
#[derive(Default)]
struct SourceOptions {
    include_entry: bool,
}

impl<'vm> luau::FromLuau<'vm> for SourceOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["include_entry"])?;
        Ok(Self {
            include_entry: args::optional_bool(&options, "include_entry", "options")?
                .unwrap_or(false),
        })
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for SourceOptions {
    fn luau_type() -> LuauType {
        LuauType::named("SourceOptions")
    }
}

pub(crate) struct PlaybackSourcesModule;

#[harmony_macros::module(id = "lyra/playback_sources", docs_feature = "docgen")]
impl PlaybackSourcesModule {
    #[harmony(
        description = "The playback source of each of these tracks, keyed by track id: the first of a track's available sources whose file is in a library the caller can see, its primary one first, as the last scan found it. The file itself is not checked: when it is gone, a stream plays the track's next source or fails, as `decide` tells. Tracks without such a source and tracks the caller cannot see are absent.",
        returns(HashMap<String, PlaybackSourceInfo>)
    )]
    async fn get(
        track_ids: UniqueIds,
        options: Option<SourceOptions>,
        #[context] db: ViewerDb,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let include_entry = options.unwrap_or_default().include_entry;
        let include_full_path = lyra_db::roles::has_permission(
            &principal.permissions,
            lyra_db::Permission::ManageLibraries,
        );
        let (db, viewer) = db.read().await?;
        let tracks = Resolved::of(&*db, track_ids.0)?;
        let sources = sources(&db, &viewer, &tracks, include_entry)?;
        keyed_sources(&db, tracks, sources, include_entry, include_full_path)
    }

    #[harmony(
        description = "`get` read as the system, without the entries' full paths. Ignores library visibility, so it is only for public streaming routes, which must serve nothing from it but the audio.",
        returns(HashMap<String, PlaybackSourceInfo>)
    )]
    async fn streamable(
        track_ids: UniqueIds,
        options: Option<SourceOptions>,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let include_entry = options.unwrap_or_default().include_entry;
        let (db, viewer) = db.read().await?;
        let tracks = Resolved::of(&*db, track_ids.0)?;
        let sources = sources(&db, &viewer, &tracks, include_entry)?;
        keyed_sources(&db, tracks, sources, include_entry, false)
    }

    #[harmony(
        description = "What `api.response.stream_track(track_id, options)` would deliver, without serving anything: whether the source file is served as it is or transcoded, why, and the delivered format. It checks access and the source file as the stream would, for the caller the dispatch acts for as of this call. When the request cannot be served, raises the error the stream would answer with: kind `invalid` for options the stream refuses, `unauthenticated` for a credential that no longer verifies, `forbidden` for a refused caller, and `not_found` for a track that is missing, hidden, without a source or whose file is gone; other failures have no kind. A stream can still fail once served, as when Lyra is out of transcoding capacity.",
        args(track_id: String, options: Option<TrackStreamOptions>),
        returns(DeliveryDescription)
    )]
    async fn decide(
        track_id: String,
        options: Option<luau::Value>,
        #[context] caller: ResponseCaller,
    ) -> luau::runtime::Result<luau::Value> {
        let request = crate::plugins::api::options::stream_request(
            &options.unwrap_or(luau::Value::Nil),
            "decide options",
        )?;
        let access =
            delivery::stream_track_access(caller.auth.as_ref(), caller.client, request.public)
                .map_err(delivery_runtime_error)?;
        let description = delivery::describe_stream(&track_id, &access, request.options)
            .await
            .map_err(delivery_runtime_error)?;
        harmony_serde::json_to_luau_owned(
            serde_json::to_value(description).map_err(crate::plugins::runtime_error)?,
            0,
        )
    }
}

/// The source each of `tracks` the viewer sees plays from, as stored: the first whose file the
/// viewer sees too, as streaming picks among the files in the viewer's libraries.
fn sources(
    db: &agdb::DbAny,
    viewer: &Viewer,
    tracks: &Resolved,
    include_entry: bool,
) -> luau::runtime::Result<Vec<(DbId, playback_source_service::PlaybackSource)>> {
    let found = lyra_db::tracks::get_existing_by_ids(db, tracks.db_ids())?;
    let mut sources = Vec::with_capacity(found.len());
    for &track_id in tracks.db_ids() {
        let Some(track) = found.get(&track_id) else {
            continue;
        };
        if !viewer.sees(db, track_id)? {
            continue;
        }
        for source in playback_source_service::candidates(db, track, include_entry)? {
            if viewer.sees(db, source.entry_db_id)? {
                sources.push((track_id, source));
                break;
            }
        }
    }
    Ok(sources)
}

fn keyed_sources(
    db: &agdb::DbAny,
    tracks: Resolved,
    sources: Vec<(DbId, playback_source_service::PlaybackSource)>,
    include_entry: bool,
    include_full_path: bool,
) -> luau::runtime::Result<luau::OwnedTable> {
    let parents = EntryInfo::parents(
        db,
        sources
            .iter()
            .filter_map(|(_, source)| source.entry.as_ref()),
    )?;
    let mut rows = Vec::with_capacity(sources.len());
    for (track_id, source) in sources {
        rows.push((
            track_id,
            source_to_table(source, &parents, include_entry, include_full_path)?,
        ));
    }
    Ok(tracks.keyed(rows))
}

fn delivery_runtime_error(error: DeliveryError) -> luau::Error {
    let (message, kind) = delivery_error(error);
    match kind {
        Some(kind) => kinded(kind, message),
        None => crate::plugins::runtime_error(message),
    }
}

/// The error a stream route would answer `error` with, status first, as `decide` raises it.
#[cfg(test)]
pub(crate) fn delivery_error_message(error: DeliveryError) -> String {
    delivery_error(error).0
}

/// The message a stream route would answer `error` with, status first, and the kind of error it
/// is to a plugin, when an error kind says so.
fn delivery_error(error: DeliveryError) -> (String, Option<ErrorKind>) {
    let (status, message, kind) = match error {
        DeliveryError::BadRequest(message) => {
            (StatusCode::BAD_REQUEST, message, Some(ErrorKind::Invalid))
        }
        DeliveryError::Unauthorized(message) => (
            StatusCode::UNAUTHORIZED,
            message,
            Some(ErrorKind::Unauthenticated),
        ),
        DeliveryError::NotFound(message) => {
            (StatusCode::NOT_FOUND, message, Some(ErrorKind::NotFound))
        }
        DeliveryError::Gone(message) => (StatusCode::GONE, message, Some(ErrorKind::NotFound)),
        DeliveryError::ServiceUnavailable(message) => {
            (StatusCode::SERVICE_UNAVAILABLE, message, None)
        }
        DeliveryError::Auth(error) => {
            let kind = auth_error_kind(&error);
            let (status, message) = auth_error_status(error);
            (status, message, kind)
        }
        DeliveryError::Internal(error) => {
            let kind = error.downcast_ref::<AuthError>().and_then(auth_error_kind);
            let (status, message) = internal_error_status(error);
            (status, message, kind)
        }
    };
    (format!("{status}: {message}"), kind)
}

fn auth_error_status(error: AuthError) -> (StatusCode, String) {
    match error {
        AuthError::MissingBearerCredential
        | AuthError::InvalidBearerCredential
        | AuthError::SessionExpired => (StatusCode::UNAUTHORIZED, error.to_string()),
        AuthError::InvalidClientName(error) => (StatusCode::BAD_REQUEST, error.to_string()),
        AuthError::Forbidden(message) => (StatusCode::FORBIDDEN, message),
        AuthError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        AuthError::RateLimited(_) => (StatusCode::TOO_MANY_REQUESTS, error.to_string()),
        AuthError::Internal(error) => internal_error_status(error),
    }
}

fn internal_error_status(error: anyhow::Error) -> (StatusCode, String) {
    match error.downcast::<AuthError>() {
        Ok(error) => auth_error_status(error),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

fn source_to_table(
    source: playback_source_service::PlaybackSource,
    parents: &HashMap<DbId, String>,
    include_entry: bool,
    include_full_path: bool,
) -> luau::runtime::Result<luau::OwnedTable> {
    let SourceDescription {
        container,
        codec,
        bitrate_bps,
        sample_rate_hz,
        channels,
        bit_depth,
    } = SourceDescription::from(&source.format);

    let mut table = luau::OwnedTable::with_capacity(0, 9);
    table.set_field("container", container);
    table.set_field("codec", codec);
    for (name, value) in [
        ("bitrate_bps", bitrate_bps),
        ("sample_rate_hz", sample_rate_hz),
        ("channels", channels),
        ("bit_depth", bit_depth),
    ] {
        table.set_field(name, optional_u64(value.map(u64::from)));
    }
    table.set_field("start_ms", optional_u64(source.start_ms));
    table.set_field("end_ms", optional_u64(source.end_ms));
    if include_entry {
        table.set_field(
            "entry",
            source
                .entry
                .map(|entry| {
                    harmony_luau::serializable_to_luau_owned(EntryInfo::new(
                        entry,
                        parents,
                        include_full_path,
                    )?)
                })
                .transpose()?
                .unwrap_or(luau::Value::Nil),
        );
    }
    Ok(table)
}

fn optional_u64(value: Option<u64>) -> luau::Value {
    value.map(luau::Value::from).unwrap_or(luau::Value::Nil)
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for PlaybackSourceInfo {
    fn luau_type() -> LuauType {
        LuauType::named("PlaybackSourceInfo")
    }
}

#[cfg(feature = "docgen")]
impl PlaybackSourceInfo {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "PlaybackSourceInfo",
            LuauType::intersection(vec![
                SourceDescription::luau_type(),
                LuauType::object(vec![
                    field("start_ms", Option::<u64>::luau_type(), None),
                    field("end_ms", Option::<u64>::luau_type(), None),
                    field("entry", Option::<EntryInfo>::luau_type(), None),
                ]),
            ]),
            Some(
                "A track's playback source: the format of the file it plays from, as the last scan read it, and with `include_entry` the file itself. `start_ms` and `end_ms` mark the part of the file the track plays, as when a CUE sheet splits one; without them the track starts or ends with the file.",
            ),
        )
    }
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType, description: Option<&'static str>) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description,
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for TrackStreamOptions {
    fn luau_type() -> LuauType {
        LuauType::named("TrackStreamOptions")
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for DeliveryDescription {
    fn luau_type() -> LuauType {
        LuauType::named("DeliveryDescription")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for DeliveryDescription {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("DeliveryDescription", None);
        descriptor.fields.extend([
            field(
                "transcode",
                bool::luau_type(),
                Some("`false` when the source file is served as it is."),
            ),
            field(
                "reasons",
                Vec::<String>::luau_type(),
                Some(
                    "Why the source is re-encoded: `container`, `codec`, `bitrate`, `sample_rate`, `channels`, `bit_depth`, or `range`. Empty for the source file.",
                ),
            ),
            field(
                "container",
                String::luau_type(),
                Some("The delivered format, as the `format` option names it."),
            ),
            field("codec", Option::<String>::luau_type(), None),
            field("content_type", String::luau_type(), None),
            field("bitrate_bps", Option::<u32>::luau_type(), None),
            field("vbr_quality", Option::<i32>::luau_type(), None),
            field("sample_rate_hz", Option::<u32>::luau_type(), None),
            field("channels", Option::<u32>::luau_type(), None),
            field(
                "bit_depth",
                Option::<u32>::luau_type(),
                Some("The bits per sample of lossless audio; absent for lossy audio and when unknown."),
            ),
            field("source", SourceDescription::luau_type(), None),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for SourceDescription {
    fn luau_type() -> LuauType {
        LuauType::named("SourceDescription")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for SourceDescription {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "SourceDescription",
            Some("A source file's format, read from its contents when it was scanned."),
        );
        descriptor.fields.extend([
            field(
                "container",
                LuauType::optional(LuauType::named("SourceContainer")),
                Some("The file format holding the audio."),
            ),
            field(
                "codec",
                LuauType::optional(LuauType::named("SourceCodec")),
                Some("The audio's encoding inside its container, named as FFmpeg names codecs."),
            ),
            field(
                "bitrate_bps",
                Option::<u32>::luau_type(),
                Some("The file's overall bitrate."),
            ),
            field("sample_rate_hz", Option::<u32>::luau_type(), None),
            field("channels", Option::<u32>::luau_type(), None),
            field(
                "bit_depth",
                Option::<u32>::luau_type(),
                Some("The audio's bits per sample; absent for lossy codecs."),
            ),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    let string_enum = |values: &[&'static str]| {
        LuauType::union(
            values
                .iter()
                .copied()
                .map(LuauType::string_literal)
                .collect(),
        )
    };
    let mut aliases = crate::plugins::api::options::delivery_aliases();
    aliases.extend([
        PlaybackSourceInfo::type_alias_descriptor(),
        TypeAliasDescriptor::new(
            "SourceContainer",
            string_enum(lyra_db::track_sources::SourceContainer::VALUES),
            None,
        ),
        TypeAliasDescriptor::new(
            "SourceCodec",
            string_enum(lyra_db::track_sources::SourceCodec::VALUES),
            None,
        ),
    ]);
    let mut source_options = InterfaceDescriptor::new("SourceOptions", None);
    source_options.fields.push(field(
        "include_entry",
        LuauType::optional(bool::luau_type()),
        Some("Include each source's `entry`. Default `false`."),
    ));
    render_definition_file_with_support(
        &PlaybackSourcesModule::module_descriptor(),
        &aliases,
        &[
            EntryInfo::interface_descriptor(),
            source_options,
            crate::plugins::api::options::track_stream_options_interface(),
            DeliveryDescription::interface_descriptor(),
            SourceDescription::interface_descriptor(),
        ],
        &[],
    )
}
