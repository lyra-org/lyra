// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::sync::{
    Arc,
    OnceLock,
};

use agdb::DbId;
use futures::future::BoxFuture;
use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};

use crate::db::ResolveId;
use crate::plugins::args;
use crate::{
    plugins::db,
    services::{
        auth::Principal,
        playback_sources as playback_source_service,
    },
};

struct PlaybackSourcesModule;

/// Decides what an `api.response.stream_track` response for a track and its options would
/// deliver, answering with the description `decide` returns. The HTTP layer that serves the
/// stream provides it, and the composition root installs it.
pub(crate) type DecideDelivery = fn(
    Option<Principal>,
    DbId,
    Option<serde_json::Value>,
) -> BoxFuture<'static, anyhow::Result<serde_json::Value>>;

static DECIDE_DELIVERY: OnceLock<DecideDelivery> = OnceLock::new();

pub(crate) fn install_delivery_decider(decide: DecideDelivery) {
    DECIDE_DELIVERY.get_or_init(|| decide);
}

#[cfg(feature = "docgen")]
struct EntryInfo;

#[cfg(feature = "docgen")]
struct PlaybackSourceInfo;

#[cfg(feature = "docgen")]
struct DeliveryRequest;

#[cfg(feature = "docgen")]
struct DeliveryDescription;

#[cfg(feature = "docgen")]
struct SourceDescription;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/playback_sources")
        .capability("lyra.playback_sources")
        .function(get_spec())
        .function(get_many_spec())
        .function(streamable_spec())
        .function(decide_spec())
        .install(|_| Ok(ModuleExport::new(PlaybackSourcesModule)))
}

fn get_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("id")
        .args::<Option<ResolveId>>()
        .arg_name("include_entry")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_callback))
}

fn get_many_spec() -> FunctionSpec {
    FunctionSpec::async_fn("get_many")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_ids")
        .args::<luau::Table>()
        .arg_name("include_entry")
        .args::<Option<bool>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(get_many_callback))
}

fn streamable_spec() -> FunctionSpec {
    FunctionSpec::async_fn("streamable")
        .arg_name("track_id")
        .args::<i64>()
        .arg_name("include_entry")
        .args::<Option<bool>>()
        .returns::<Option<luau::Table>>()
        .call_async(Arc::new(streamable_callback))
}

fn decide_spec() -> FunctionSpec {
    FunctionSpec::async_fn("decide")
        .context::<crate::plugins::auth::DispatchAuth>()
        .arg_name("track_id")
        .args::<i64>()
        .arg_name("options")
        .args::<Option<luau::Value>>()
        .returns::<luau::Table>()
        .call_async(Arc::new(decide_callback))
}

fn decide_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id = args::positive_id(frame.args.read_named::<i64>("track_id")?, "track_id")?;
    let options = frame
        .args
        .read_optional_named::<luau::Value>("options")?
        .map(|options| harmony_serde::luau_to_json(frame.vm, &options, 0))
        .transpose()?;
    let principal = crate::plugins::auth::dispatch_principal(&frame.context)?;

    let decide = *DECIDE_DELIVERY
        .get()
        .ok_or_else(|| crate::plugins::runtime_error("no stream delivery decider is installed"))?;

    Ok(luau::ScheduledFuture::new(async move {
        let description = decide(principal, track_id, options)
            .await
            .map_err(crate::plugins::runtime_error)?;
        harmony_serde::json_to_luau_owned(description, 0)
    }))
}

fn get_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let id = frame
        .args
        .read_optional_named::<luau::Value>("id")?
        .map(args::resolve_id)
        .transpose()?
        .unwrap_or_else(|| ResolveId::alias("tracks"));
    let include_entry = frame
        .args
        .read_optional_named::<bool>("include_entry")?
        .unwrap_or(false);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let include_full_path = crate::db::roles::has_permission(
            &principal.permissions,
            crate::db::Permission::ManageLibraries,
        );
        let query_id = id
            .to_query_id(&db)
            .map_err(crate::plugins::runtime_error)?
            .ok_or_else(|| crate::plugins::runtime_error("could not resolve scope"))?;
        let tracks =
            crate::db::tracks::get(&db, query_id).map_err(crate::plugins::runtime_error)?;

        let mut rows = luau::OwnedTable::new();
        for track in tracks {
            let Some(track_id) = track.db_id.map(DbId::from) else {
                continue;
            };
            if !crate::services::auth::access::entity_accessible(&db, &principal, track_id)
                .map_err(crate::plugins::runtime_error)?
            {
                continue;
            }
            let Some(source) = playback_source_service::resolve(&db, track_id, include_entry)
                .map_err(crate::plugins::runtime_error)?
            else {
                continue;
            };
            rows.push_array(luau::Value::TableData(source_to_table(
                source,
                include_entry,
                include_full_path,
            )));
        }

        Ok(luau::Value::TableData(rows))
    }))
}

fn get_many_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_ids: luau::Table = frame.args.read_named("track_ids")?;
    let track_ids = args::unique_ids(frame.vm, &track_ids)?;
    let include_entry = frame
        .args
        .read_optional_named::<bool>("include_entry")?
        .unwrap_or(false);
    let db = db::get(frame.vm)?;
    let principal = crate::plugins::auth::require_dispatch_principal(&frame.context)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        let include_full_path = crate::db::roles::has_permission(
            &principal.permissions,
            crate::db::Permission::ManageLibraries,
        );
        let mut rows = luau::OwnedTable::with_entry_capacity(0, 0, track_ids.len());
        for track_id in track_ids {
            let value =
                if crate::services::auth::access::entity_accessible(&db, &principal, track_id)
                    .map_err(crate::plugins::runtime_error)?
                {
                    playback_source_service::resolve(&db, track_id, include_entry)
                        .map_err(crate::plugins::runtime_error)?
                        .map(|source| {
                            luau::Value::TableData(source_to_table(
                                source,
                                include_entry,
                                include_full_path,
                            ))
                        })
                        .unwrap_or(luau::Value::Nil)
                } else {
                    luau::Value::Nil
                };
            rows.set_key(luau::Value::from(track_id.0), value);
        }

        Ok(luau::Value::TableData(rows))
    }))
}

fn streamable_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let track_id = args::positive_id(frame.args.read_named::<i64>("track_id")?, "track_id")?;
    let include_entry = frame
        .args
        .read_optional_named::<bool>("include_entry")?
        .unwrap_or(false);
    let db = db::get(frame.vm)?;

    Ok(luau::ScheduledFuture::new(async move {
        let db = db.read().await;
        Ok(
            playback_source_service::resolve(&db, track_id, include_entry)
                .map_err(crate::plugins::runtime_error)?
                .map(|source| luau::Value::TableData(source_to_table(source, include_entry, false)))
                .unwrap_or(luau::Value::Nil),
        )
    }))
}

fn source_to_table(
    source: playback_source_service::PlaybackSource,
    include_entry: bool,
    include_full_path: bool,
) -> luau::OwnedTable {
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
    table.set_field("track_id", luau::Value::from(track_db_id.0));
    table.set_field("source_id", luau::Value::from(source_id.0));
    table.set_field("source_kind", luau::Value::String(source_kind.into_bytes()));
    table.set_field("source_key", luau::Value::String(source_key.into_bytes()));
    table.set_field("is_primary", luau::Value::Boolean(is_primary));
    let optional_str = |value: Option<&'static str>| {
        value
            .map(|value| luau::Value::String(value.as_bytes().to_vec()))
            .unwrap_or(luau::Value::Nil)
    };
    table.set_field(
        "container",
        optional_str(format.container.map(|container| container.as_str())),
    );
    table.set_field(
        "codec",
        optional_str(format.codec.map(|codec| codec.as_str())),
    );
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
    table.set_field(
        "is_virtual",
        luau::Value::Boolean(start_ms.is_some() || end_ms.is_some()),
    );
    if include_entry {
        table.set_field(
            "entry",
            entry
                .map(|entry| luau::Value::TableData(entry_to_table(entry, include_full_path)))
                .unwrap_or(luau::Value::Nil),
        );
    }
    table
}

fn entry_to_table(entry: crate::db::Entry, include_full_path: bool) -> luau::OwnedTable {
    let mut table = luau::OwnedTable::with_capacity(0, 8);
    table.set_field(
        "db_id",
        entry
            .db_id
            .map(|id| luau::Value::from(id.0))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("id", luau::Value::String(entry.id.into_bytes()));
    table.set_field(
        "full_path",
        if include_full_path {
            luau::Value::String(entry.full_path.to_string_lossy().into_owned().into_bytes())
        } else {
            luau::Value::Nil
        },
    );
    table.set_field(
        "kind",
        luau::Value::String(entry.kind.to_string().into_bytes()),
    );
    table.set_field("name", luau::Value::String(entry.name.into_bytes()));
    table.set_field(
        "hash",
        entry
            .hash
            .map(|hash| luau::Value::String(hash.into_bytes()))
            .unwrap_or(luau::Value::Nil),
    );
    table.set_field("size", luau::Value::from(entry.size));
    table.set_field("mtime", luau::Value::from(entry.mtime));
    table
}

fn optional_u64(value: Option<u64>) -> luau::Value {
    value.map(luau::Value::from).unwrap_or(luau::Value::Nil)
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntryInfo {
    fn luau_type() -> LuauType {
        LuauType::named("EntryInfo")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for EntryInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("EntryInfo", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "db_id",
                ty: Option::<i64>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "id",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "full_path",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "kind",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "hash",
                ty: Option::<String>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "size",
                ty: u64::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "mtime",
                ty: u64::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
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
        name: "PlaybackSources",
        local_name: "playback_sources",
        description: None,
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["get"],
                description: None,
                params: vec![
                    param("id", LuauType::optional(args::resolve_id_type())),
                    param("include_entry", Option::<bool>::luau_type()),
                ],
                returns: vec![Vec::<PlaybackSourceInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["streamable"],
                description: Some(
                    "A track's playback source read as the system, without the entry's full path. Ignores library visibility, so it is only for public streaming routes, which must serve nothing from it but the audio.",
                ),
                params: vec![
                    param("track_id", i64::luau_type()),
                    param("include_entry", Option::<bool>::luau_type()),
                ],
                returns: vec![Option::<PlaybackSourceInfo>::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["decide"],
                description: Some(
                    "What `api.response.stream_track(track_id, options)` would deliver, without serving anything: whether the source file is served as it is or transcoded, why, and the delivered format. Raises the error the stream would answer with when the request cannot be served.",
                ),
                params: vec![
                    param("track_id", i64::luau_type()),
                    param("options", Option::<DeliveryRequest>::luau_type()),
                ],
                returns: vec![DeliveryDescription::luau_type()],
                yields: true,
            },
            ModuleFunctionDescriptor {
                path: vec!["get_many"],
                description: None,
                params: vec![
                    param("track_ids", Vec::<u64>::luau_type()),
                    param("include_entry", Option::<bool>::luau_type()),
                ],
                returns: vec![LuauType::map(
                    u64::luau_type(),
                    Option::<PlaybackSourceInfo>::luau_type(),
                )],
                yields: true,
            },
        ],
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
        &module_descriptor(),
        &[
            TypeAliasDescriptor::new(
                "SourceContainer",
                string_enum(crate::db::track_sources::SourceContainer::VALUES),
                None,
            ),
            TypeAliasDescriptor::new(
                "SourceCodec",
                string_enum(crate::db::track_sources::SourceCodec::VALUES),
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
