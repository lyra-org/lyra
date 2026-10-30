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
    JsonArg,
    PositiveId,
    ResolveIdArg,
    UniqueIds,
};
use crate::plugins::auth::DispatchPrincipal;
use crate::plugins::types::EntryInfo;
use crate::{
    plugins::db::Db,
    services::{
        auth::{
            AuthError,
            Principal,
        },
        delivery::{
            self,
            DeliveryError,
        },
        playback_sources as playback_source_service,
    },
};
use lyra_db::ResolveId;

#[cfg(feature = "docgen")]
use std::collections::HashMap;

#[cfg(feature = "docgen")]
struct PlaybackSourceInfo;

#[cfg(feature = "docgen")]
struct DeliveryRequest;

#[cfg(feature = "docgen")]
struct DeliveryDescription;

#[cfg(feature = "docgen")]
struct SourceDescription;

pub(crate) struct PlaybackSourcesModule;

#[harmony_macros::module(id = "lyra/playback_sources", docs_feature = "docgen")]
impl PlaybackSourcesModule {
    #[harmony(returns(Vec<PlaybackSourceInfo>))]
    async fn get(
        id: Option<ResolveIdArg>,
        include_entry: Option<bool>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let id = id
            .map(|id| id.0)
            .unwrap_or_else(|| ResolveId::alias("tracks"));
        let include_entry = include_entry.unwrap_or(false);
        let db = db.read().await;
        let include_full_path = lyra_db::roles::has_permission(
            &principal.permissions,
            lyra_db::Permission::ManageLibraries,
        );
        let query_id = id
            .to_query_id(&db)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let tracks = lyra_db::tracks::get(&db, query_id)?;

        let mut rows = luau::OwnedTable::new();
        for track in tracks {
            let Some(track_id) = track.db_id.map(DbId::from) else {
                continue;
            };
            if !crate::services::auth::access::entity_accessible(&db, &principal, track_id)? {
                continue;
            }
            let Some(source) = playback_source_service::resolve(&db, track_id, include_entry)?
            else {
                continue;
            };
            rows.push_array(luau::Value::TableData(source_to_table(
                source,
                include_entry,
                include_full_path,
            )?));
        }

        Ok(rows)
    }

    #[harmony(
        description = "A track's playback source read as the system, without the entry's full path. Ignores library visibility, so it is only for public streaming routes, which must serve nothing from it but the audio.",
        returns(Option<PlaybackSourceInfo>)
    )]
    async fn streamable(
        track_id: PositiveId,
        include_entry: Option<bool>,
        #[context] db: Db,
    ) -> luau::runtime::Result<luau::Value> {
        let include_entry = include_entry.unwrap_or(false);
        let db = db.read().await;
        Ok(
            playback_source_service::resolve(&db, track_id.0, include_entry)?
                .map(|source| source_to_table(source, include_entry, false))
                .transpose()?
                .map_or(luau::Value::Nil, luau::Value::TableData),
        )
    }

    #[harmony(
        description = "What `api.response.stream_track(track_id, options)` would deliver, without serving anything: whether the source file is served as it is or transcoded, why, and the delivered format. Raises the error the stream would answer with when the request cannot be served.",
        args(track_id: i64, options: Option<DeliveryRequest>),
        returns(DeliveryDescription)
    )]
    async fn decide(
        track_id: PositiveId,
        options: Option<JsonArg>,
        #[context] principal: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::Value> {
        let options = options.map(|options| options.0);
        let (public, options) =
            crate::plugins::api::options::stream_track_options(options.as_ref())?;
        let (track_id, access) = delivery::stream_track_access(
            principal.0.as_ref(),
            Default::default(),
            track_id.0,
            public,
        )
        .await
        .map_err(delivery_runtime_error)?;
        let description = delivery::describe_stream(&track_id, &access, options)
            .await
            .map_err(delivery_runtime_error)?;
        harmony_serde::json_to_luau_owned(
            serde_json::to_value(description).map_err(crate::plugins::runtime_error)?,
            0,
        )
    }

    #[harmony(returns(HashMap<u64, Option<PlaybackSourceInfo>>))]
    async fn get_many(
        track_ids: UniqueIds,
        include_entry: Option<bool>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let track_ids = track_ids.0;
        let include_entry = include_entry.unwrap_or(false);
        let db = db.read().await;
        let include_full_path = lyra_db::roles::has_permission(
            &principal.permissions,
            lyra_db::Permission::ManageLibraries,
        );
        let mut rows = luau::OwnedTable::with_entry_capacity(0, 0, track_ids.len());
        for track_id in track_ids {
            let value =
                if crate::services::auth::access::entity_accessible(&db, &principal, track_id)? {
                    playback_source_service::resolve(&db, track_id, include_entry)?
                        .map(|source| source_to_table(source, include_entry, include_full_path))
                        .transpose()?
                        .map_or(luau::Value::Nil, luau::Value::TableData)
                } else {
                    luau::Value::Nil
                };
            rows.set_key(track_id.0, value);
        }

        Ok(rows)
    }
}

fn delivery_runtime_error(error: DeliveryError) -> luau::Error {
    crate::plugins::runtime_error(delivery_error_message(error))
}

/// The error a stream route would answer `error` with, status first, as `decide` raises it.
pub(crate) fn delivery_error_message(error: DeliveryError) -> String {
    let (status, message) = match error {
        DeliveryError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        DeliveryError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
        DeliveryError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        DeliveryError::Gone(message) => (StatusCode::GONE, message),
        DeliveryError::ServiceUnavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        DeliveryError::Auth(error) => auth_error_status(error),
        DeliveryError::Internal(error) => internal_error_status(error),
    };
    format!("{status}: {message}")
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
    include_entry: bool,
    include_full_path: bool,
) -> luau::runtime::Result<luau::OwnedTable> {
    let playback_source_service::PlaybackSource {
        track_db_id,
        source_id,
        source_kind,
        source_key,
        is_primary,
        format,
        start_ms,
        end_ms,
        entry,
        ..
    } = source;

    let mut table = luau::OwnedTable::with_capacity(0, 15);
    table.set_field("track_id", track_db_id.0);
    table.set_field("source_id", source_id.0);
    table.set_field("source_kind", source_kind);
    table.set_field("source_key", source_key);
    table.set_field("is_primary", is_primary);
    table.set_field(
        "container",
        format.container.map(|container| container.as_str()),
    );
    table.set_field("codec", format.codec.map(|codec| codec.as_str()));
    for (name, value) in [
        ("sample_rate_hz", format.sample_rate_hz),
        ("channel_count", format.channel_count),
        ("bit_depth", format.bit_depth),
        ("bitrate_bps", format.bitrate_bps),
    ] {
        table.set_field(name, optional_u64(value.map(u64::from)));
    }
    table.set_field("start_ms", optional_u64(start_ms));
    table.set_field("end_ms", optional_u64(end_ms));
    table.set_field("is_virtual", start_ms.is_some() || end_ms.is_some());
    if include_entry {
        table.set_field(
            "entry",
            entry
                .map(|entry| {
                    harmony_luau::serializable_to_luau_owned(EntryInfo::new(
                        entry,
                        include_full_path,
                    ))
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
impl DescribeInterface for PlaybackSourceInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("PlaybackSourceInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "track_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_id",
                ty: i64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_kind",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "source_key",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "is_primary",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "container",
                ty: LuauType::optional(LuauType::named("SourceContainer")),
                description: Some(
                    "The file format holding the audio, read from the file's contents when it was scanned.",
                ),
            },
            FieldDescriptor {
                name: "codec",
                ty: LuauType::optional(LuauType::named("SourceCodec")),
                description: Some(
                    "The audio's encoding inside its container, named as FFmpeg names codecs.",
                ),
            },
            FieldDescriptor {
                name: "sample_rate_hz",
                ty: Option::<u32>::luau_type(),
                description: Some("The audio's sample rate, read from the file when it was scanned."),
            },
            FieldDescriptor {
                name: "channel_count",
                ty: Option::<u32>::luau_type(),
                description: Some("The audio's channel count, read from the file when it was scanned."),
            },
            FieldDescriptor {
                name: "bit_depth",
                ty: Option::<u32>::luau_type(),
                description: Some(
                    "The audio's bits per sample, read from the file when it was scanned; absent for lossy codecs.",
                ),
            },
            FieldDescriptor {
                name: "bitrate_bps",
                ty: Option::<u32>::luau_type(),
                description: Some(
                    "The file's overall bitrate in bits per second, read when it was scanned.",
                ),
            },
            FieldDescriptor {
                name: "start_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "end_ms",
                ty: Option::<u64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "is_virtual",
                ty: bool::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "entry",
                ty: Option::<EntryInfo>::luau_type(),
                description: None,
            },
        ]);
        descriptor
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
impl LuauTypeInfo for DeliveryRequest {
    fn luau_type() -> LuauType {
        LuauType::named("DeliveryRequest")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for DeliveryRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "DeliveryRequest",
            Some("The options of an `api.response.stream_track` response."),
        );
        descriptor.fields.extend([
            field("format", Option::<String>::luau_type(), None),
            field("preferred_codecs", Option::<Vec<String>>::luau_type(), None),
            field("bitrate_bps", Option::<u32>::luau_type(), None),
            field("sample_rate_hz", Option::<u32>::luau_type(), None),
            field("channels", Option::<u32>::luau_type(), None),
            field("prefer_vbr", Option::<bool>::luau_type(), None),
            field("start_offset_ms", Option::<u64>::luau_type(), None),
            field(
                "public",
                Option::<bool>::luau_type(),
                Some("Decide without the caller's track access check, as a public stream route serves."),
            ),
        ]);
        descriptor
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
                    "Why the source is re-encoded: `container`, `codec`, `bitrate`, `sample_rate`, `channels`, or `range`. Empty for the source file.",
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
        let mut descriptor = InterfaceDescriptor::new("SourceDescription", None);
        descriptor.fields.extend([
            field(
                "container",
                LuauType::optional(LuauType::named("SourceContainer")),
                None,
            ),
            field(
                "codec",
                LuauType::optional(LuauType::named("SourceCodec")),
                None,
            ),
            field("bitrate_bps", Option::<u32>::luau_type(), None),
            field("sample_rate_hz", Option::<u32>::luau_type(), None),
            field("channels", Option::<u32>::luau_type(), None),
            field("bit_depth", Option::<u32>::luau_type(), None),
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
    render_definition_file_with_support(
        &PlaybackSourcesModule::module_descriptor(),
        &[
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
        ],
        &[
            EntryInfo::interface_descriptor(),
            PlaybackSourceInfo::interface_descriptor(),
            DeliveryRequest::interface_descriptor(),
            DeliveryDescription::interface_descriptor(),
            SourceDescription::interface_descriptor(),
        ],
        &[],
    )
}
