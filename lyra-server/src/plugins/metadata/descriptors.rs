// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::{
    ClassDescriptor,
    FieldDescriptor,
    FunctionParameter,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    MethodDescriptor,
    MethodKind,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    TypeAliasDescriptor,
    render_definition_file_with_support,
};

use crate::plugins::db as server_db;
use crate::services::EntityType;

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &metadata_module_descriptor(),
        &metadata_type_aliases(),
        &metadata_interfaces(),
        &metadata_classes(),
    )
}

#[cfg(feature = "docgen")]
fn metadata_module_descriptor() -> ModuleDescriptor {
    let mut descriptor = ModuleDescriptor::new("Metadata", "metadata", None);
    descriptor.functions.extend([
        ModuleFunctionDescriptor {
            path: vec!["Provider", "new"],
            description: Some("Creates a metadata provider registration object."),
            params: vec![param("id", string())],
            returns: vec![ty("Provider")],
            yields: false,
        },
        ModuleFunctionDescriptor {
            path: vec!["ids", "for_provider"],
            description: Some("Returns external IDs for a single provider."),
            params: vec![
                param("external_ids", opt(ty("ExternalIdsByProvider"))),
                param("provider_id", string()),
            ],
            returns: vec![opt(ty("ProviderExternalIdMap"))],
            yields: false,
        },
    ]);
    descriptor
}

#[cfg(feature = "docgen")]
fn metadata_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        alias(
            "JsonValue",
            ty("(boolean | number | string | { JsonValue } | { [string]: JsonValue })?"),
        ),
        alias("ProviderExternalIdMap", map(string(), string())),
        alias(
            "ExternalIdsByProvider",
            map(string(), ty("ProviderExternalIdMap")),
        ),
        alias("ProviderCustomFieldMap", map(string(), ty("JsonValue"))),
        alias(
            "ProviderCustomFieldsByVersion",
            map(string(), ty("ProviderCustomFieldMap")),
        ),
        alias(
            "CustomFieldsByProvider",
            map(string(), ty("ProviderCustomFieldsByVersion")),
        ),
        alias("OptionValue", union([boolean(), string(), number()])),
        alias("ProviderSearchResult", map(string(), ty("JsonValue"))),
        alias(
            "ProviderSearchHandlerResult",
            opt(union([
                ty("ProviderSearchResult"),
                array(ty("ProviderSearchResult")),
            ])),
        ),
        alias(
            "ProviderSearchHandler",
            LuauType::function(
                vec![fn_param("query", string())],
                vec![ty("ProviderSearchHandlerResult")],
            ),
        ),
        alias(
            "ProviderCoverCandidate",
            union([
                string(),
                LuauType::object(vec![
                    field("url", opt(string())),
                    field("cover_url", opt(string())),
                    field("cover_image_url", opt(string())),
                    field("cover", opt(string())),
                    field("width", opt(number())),
                    field("height", opt(number())),
                ]),
            ]),
        ),
        alias(
            "ProviderCoverResult",
            union([
                ty("ProviderCoverCandidate"),
                LuauType::object(vec![
                    field("candidates", array(ty("ProviderCoverCandidate"))),
                    field("selected_index", opt(number())),
                ]),
            ]),
        ),
        alias(
            "ProviderCoverHandler",
            LuauType::function(
                vec![fn_param("ctx", ty("ProviderCoverContext"))],
                vec![opt(ty("ProviderCoverResult"))],
            ),
        ),
        alias(
            "ProviderLyricsHitResult",
            LuauType::object(vec![
                field("kind", LuauType::string_literal("hit")),
                field("candidates", array(ty("ProviderLyricsCandidate"))),
            ]),
        ),
        alias(
            "ProviderLyricsMissResult",
            LuauType::object(vec![field("kind", LuauType::string_literal("miss"))]),
        ),
        alias(
            "ProviderLyricsInstrumentalResult",
            LuauType::object(vec![field(
                "kind",
                LuauType::string_literal("instrumental"),
            )]),
        ),
        alias(
            "ProviderLyricsRateLimitedResult",
            LuauType::object(vec![
                field("kind", LuauType::string_literal("rate_limited")),
                field("retry_after_ms", opt(number())),
            ]),
        ),
        alias(
            "ProviderLyricsResult",
            union([
                ty("ProviderLyricsHitResult"),
                ty("ProviderLyricsMissResult"),
                ty("ProviderLyricsInstrumentalResult"),
                ty("ProviderLyricsRateLimitedResult"),
            ]),
        ),
        alias(
            "ProviderLyricsHandler",
            LuauType::function(
                vec![fn_param("ctx", ty("ProviderLyricsContext"))],
                vec![ty("ProviderLyricsResult")],
            ),
        ),
        alias(
            "ProviderRefreshContext",
            union([
                ty("ReleaseRefreshContext"),
                ty("ArtistRefreshContext"),
                ty("TrackRefreshContext"),
            ]),
        ),
        alias(
            "ProviderRefreshHandler",
            union([
                LuauType::function(
                    vec![fn_param("ctx", ty("ReleaseRefreshContext"))],
                    vec![ty("nil")],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("ArtistRefreshContext"))],
                    vec![ty("nil")],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("TrackRefreshContext"))],
                    vec![ty("nil")],
                ),
            ]),
        ),
        alias(
            "ProviderRefreshFilter",
            union([
                LuauType::function(
                    vec![fn_param("ctx", ty("ReleaseRefreshContext"))],
                    vec![boolean()],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("ArtistRefreshContext"))],
                    vec![boolean()],
                ),
                LuauType::function(
                    vec![fn_param("ctx", ty("TrackRefreshContext"))],
                    vec![boolean()],
                ),
            ]),
        ),
    ]
}

#[cfg(feature = "docgen")]
fn metadata_interfaces() -> Vec<InterfaceDescriptor> {
    vec![
        interface(
            "MetadataIdRow",
            vec![
                field("provider_id", string()),
                field("id_type", string()),
                field("id_value", string()),
            ],
        ),
        interface(
            "ProviderIdRegistration",
            vec![
                field("id_type", string()),
                field("entity", ty("EntityType")),
                field("unique", opt(boolean())),
            ],
        ),
        interface(
            "OptionConfig",
            vec![
                field("name", string()),
                field("label", string()),
                field("type", string()),
                field("default", opt(ty("OptionValue"))),
                field("requires_settings", opt(array(string()))),
            ],
        ),
        interface(
            "ReleaseRefreshLookupHints",
            vec![
                field("artist_name", opt(string())),
                field("release_title", opt(string())),
                field("year", opt(number())),
            ],
        ),
        interface(
            "ReleaseRefreshArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", string()),
                field("sort_name", opt(string())),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "ReleaseRefreshTrackArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", string()),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "ReleaseRefreshTrack",
            vec![
                field("db_id", opt(number())),
                field("track_title", string()),
                field("sort_title", opt(string())),
                field("disc", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
                field("external_ids", ty("ExternalIdsByProvider")),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", array(ty("ReleaseRefreshTrackArtist"))),
            ],
        ),
        interface(
            "ReleaseRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("locked", opt(boolean())),
                field("created_at", opt(number())),
                field("ctime", opt(number())),
                field("lookup_hints", opt(ty("ReleaseRefreshLookupHints"))),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", opt(array(ty("ReleaseRefreshArtist")))),
                field("tracks", opt(array(ty("ReleaseRefreshTrack")))),
                field("library_id", opt(number())),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "ArtistRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
                field("artist_type", opt(string())),
                field("description", opt(string())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "TrackRefreshRelease",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
            ],
        ),
        interface(
            "TrackRefreshContext",
            vec![
                field("db_id", opt(number())),
                field("id", opt(string())),
                field("track_title", opt(string())),
                field("sort_title", opt(string())),
                field("year", opt(number())),
                field("disc", opt(number())),
                field("disc_total", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("custom_fields", opt(ty("CustomFieldsByProvider"))),
                field("artists", opt(array(ty("ReleaseRefreshTrackArtist")))),
                field("releases", opt(array(ty("TrackRefreshRelease")))),
                field("options", opt(map(string(), ty("OptionValue")))),
            ],
        ),
        interface(
            "ProviderRequire",
            vec![
                field("all_of", opt(array(string()))),
                field("any_of", opt(array(string()))),
            ],
        ),
        interface(
            "ProviderCoverConfig",
            vec![
                field("priority", opt(number())),
                field("timeout_ms", opt(number())),
                field("require", opt(ty("ProviderRequire"))),
            ],
        ),
        interface(
            "ProviderCoverOptions",
            vec![field("force_refresh", opt(boolean()))],
        ),
        interface(
            "ProviderCoverLibrary",
            vec![
                field("db_id", opt(number())),
                field("name", opt(string())),
                field("directory", opt(string())),
                field("language", opt(string())),
                field("country", opt(string())),
            ],
        ),
        interface(
            "ProviderCoverArtist",
            vec![
                field("db_id", opt(number())),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
            ],
        ),
        interface(
            "ProviderCoverTrack",
            vec![
                field("db_id", opt(number())),
                field("track_title", string()),
                field("sort_title", opt(string())),
                field("disc", opt(number())),
                field("track", opt(number())),
                field("track_total", opt(number())),
                field("duration_ms", opt(number())),
            ],
        ),
        interface(
            "ProviderCoverContext",
            vec![
                field("db_id", opt(number())),
                field("release_title", opt(string())),
                field("sort_title", opt(string())),
                field("release_date", opt(string())),
                field("tracks", opt(array(ty("ProviderCoverTrack")))),
                field("artists", opt(array(ty("ProviderCoverArtist")))),
                field("artist_names", opt(array(string()))),
                field("ids", opt(ty("ProviderExternalIdMap"))),
                field("library", opt(ty("ProviderCoverLibrary"))),
                field("cover_options", opt(ty("ProviderCoverOptions"))),
            ],
        ),
        interface(
            "ProviderLyricsRequire",
            vec![
                field("all_of", opt(array(string()))),
                field("any_of", opt(array(string()))),
            ],
        ),
        interface(
            "ProviderLyricsConfig",
            vec![
                field("priority", opt(number())),
                field("timeout_ms", opt(number())),
                field("require", opt(ty("ProviderLyricsRequire"))),
            ],
        ),
        interface(
            "ProviderLyricsContext",
            vec![
                field("track_db_id", number()),
                field("track_name", string()),
                field("artist_name", string()),
                field("album_name", opt(string())),
                field("duration_ms", opt(number())),
                field("external_ids", opt(ty("ExternalIdsByProvider"))),
                field("force_refresh", boolean()),
            ],
        ),
        interface(
            "ProviderLyricWordInput",
            vec![
                field("ts_ms", number()),
                field("char_start", number()),
                field("char_end", number()),
            ],
        ),
        interface(
            "ProviderLyricLineInput",
            vec![
                field("ts_ms", number()),
                field("text", string()),
                field("words", array(ty("ProviderLyricWordInput"))),
            ],
        ),
        interface(
            "ProviderLyricsInput",
            vec![
                field("id", string()),
                field("language", string()),
                field("plain_text", string()),
                field("lines", array(ty("ProviderLyricLineInput"))),
            ],
        ),
        interface(
            "ProviderLyricsCandidate",
            vec![
                field("lyrics", ty("ProviderLyricsInput")),
                field("title", string()),
                field("artist", string()),
                field("duration_ms", opt(number())),
                field("language", opt(string())),
            ],
        ),
        interface(
            "EnsureArtistRequest",
            vec![
                field("id_type", string()),
                field("id_value", string()),
                field("artist_name", opt(string())),
                field("sort_name", opt(string())),
                field("artist_type", opt(ty("ArtistType"))),
                field("description", opt(string())),
            ],
        ),
    ]
}

#[cfg(feature = "docgen")]
fn metadata_classes() -> Vec<ClassDescriptor> {
    vec![
        <EntityType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::ArtistType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::CreditType as harmony_luau::DescribeUserData>::class_descriptor(),
        <server_db::ArtistRelationType as harmony_luau::DescribeUserData>::class_descriptor(),
        layer_class(),
        provider_class(),
    ]
}

#[cfg(feature = "docgen")]
fn layer_class() -> ClassDescriptor {
    let mut class = ClassDescriptor::new("Layer", None);
    class.methods.extend([
        method(
            "set_field",
            vec![param("name", string()), param("value", ty("JsonValue"))],
            vec![],
        ),
        method(
            "set_id",
            vec![param("id_type", string()), param("id_value", string())],
            vec![],
        ),
        method(
            "set_custom_field",
            vec![
                param("version", string()),
                param("name", string()),
                param("value", ty("JsonValue")),
            ],
            vec![],
        ),
        method(
            "set_custom_fields",
            vec![param("version", string()), param("fields", ty("JsonValue"))],
            vec![],
        ),
        method(
            "clear_custom_fields",
            vec![param("version", string())],
            vec![],
        ),
        method("save", vec![], vec![]),
    ]);
    class
}

#[cfg(feature = "docgen")]
fn provider_class() -> ClassDescriptor {
    let mut class = ClassDescriptor::new("Provider", None);
    class.methods.extend([
        method(
            "id",
            vec![
                param("spec", ty("ProviderIdRegistration")),
                param(
                    "generator",
                    opt(union([
                        string(),
                        LuauType::function(vec![fn_param("id", string())], vec![string()]),
                    ])),
                ),
            ],
            vec![],
        ),
        method(
            "search",
            vec![
                param("entity", ty("EntityType")),
                param("handler", ty("ProviderSearchHandler")),
            ],
            vec![],
        ),
        method(
            "cover",
            vec![
                param("entity", ty("EntityType")),
                param("config", ty("ProviderCoverConfig")),
                param("handler", ty("ProviderCoverHandler")),
            ],
            vec![],
        ),
        method(
            "lyrics",
            vec![
                param("config", ty("ProviderLyricsConfig")),
                param("handler", ty("ProviderLyricsHandler")),
            ],
            vec![],
        ),
        method(
            "refresh",
            vec![
                param("entity", ty("EntityType")),
                param("handler", ty("ProviderRefreshHandler")),
                param("filter", opt(ty("ProviderRefreshFilter"))),
            ],
            vec![],
        ),
        method(
            "declare_option",
            vec![param("config", ty("OptionConfig"))],
            vec![],
        ),
        method(
            "ensure_artist",
            vec![param("request", ty("EnsureArtistRequest"))],
            vec![opt(number())],
        ),
        method(
            "mark_unmatched",
            vec![
                param("node_id", number()),
                param("id_types", array(string())),
            ],
            vec![],
        ),
        method(
            "link_credit",
            vec![
                param("owner_id", number()),
                param("artist_id", number()),
                param("credit_type", opt(ty("CreditType"))),
                param("detail", opt(string())),
            ],
            vec![],
        ),
        method(
            "link_artist_relation",
            vec![
                param("from_artist_id", number()),
                param("to_artist_id", number()),
                param("relation_type", ty("ArtistRelationType")),
                param("attributes", opt(string())),
            ],
            vec![],
        ),
        method("layer", vec![param("node_id", number())], vec![ty("Layer")]),
    ]);
    class
}

#[cfg(feature = "docgen")]
fn alias(name: &'static str, ty: LuauType) -> TypeAliasDescriptor {
    TypeAliasDescriptor::new(name, ty, None)
}

#[cfg(feature = "docgen")]
fn interface(name: &'static str, fields: Vec<FieldDescriptor>) -> InterfaceDescriptor {
    InterfaceDescriptor {
        name,
        description: None,
        fields,
    }
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn method(
    name: &'static str,
    params: Vec<ParameterDescriptor>,
    returns: Vec<LuauType>,
) -> MethodDescriptor {
    MethodDescriptor {
        name,
        description: None,
        params,
        returns,
        yields: false,
        kind: MethodKind::Instance,
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
fn fn_param(name: &'static str, ty: LuauType) -> FunctionParameter {
    FunctionParameter {
        name: Some(name),
        ty,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn boolean() -> LuauType {
    bool::luau_type()
}

#[cfg(feature = "docgen")]
fn string() -> LuauType {
    String::luau_type()
}

#[cfg(feature = "docgen")]
fn number() -> LuauType {
    LuauType::literal("number")
}

#[cfg(feature = "docgen")]
fn ty(name: &'static str) -> LuauType {
    LuauType::literal(name)
}

#[cfg(feature = "docgen")]
fn opt(ty: LuauType) -> LuauType {
    LuauType::optional(ty)
}

#[cfg(feature = "docgen")]
fn array(ty: LuauType) -> LuauType {
    LuauType::array(ty)
}

#[cfg(feature = "docgen")]
fn map(key: LuauType, value: LuauType) -> LuauType {
    LuauType::map(key, value)
}

#[cfg(feature = "docgen")]
fn union<const N: usize>(types: [LuauType; N]) -> LuauType {
    LuauType::union(types.into())
}
