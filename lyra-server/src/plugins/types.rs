// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How the database and service types plugins exchange look from Luau.
//! Records cross as serialized tables and are only described here; enums get
//! Luau-side mirrors, as userdata or as the strings they serialize to, that
//! convert to the types they mirror.

#[cfg(feature = "docgen")]
use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeUserData,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::services::{
    catalog::Viewer,
    covers::ResolvedCover,
    entries::PlacedEntry,
};

/// Describes `$source` to Luau as the userdata class `$name`. The field list
/// is checked against `$source`, so a renamed or removed field fails to build.
macro_rules! describe_record {
    (
        $marker:ident for $source:path,
        $name:literal,
        fields { $($field:ident : $field_ty:ty as $key:literal),* $(,)? }
    ) => {
        #[cfg(feature = "docgen")]
        pub(crate) struct $marker;

        #[cfg(feature = "docgen")]
        impl LuauTypeInfo for $marker {
            fn luau_type() -> LuauType {
                LuauType::literal($name)
            }
        }

        #[cfg(feature = "docgen")]
        impl DescribeUserData for $marker {
            fn class_descriptor() -> ClassDescriptor {
                ClassDescriptor {
                    name: $name,
                    description: None,
                    fields: vec![
                        $(
                            FieldDescriptor {
                                name: $key,
                                ty: <$field_ty as LuauTypeInfo>::luau_type(),
                                description: None,
                            }
                        ),*
                    ],
                    methods: vec![],
                }
            }
        }

        const _: fn(&$source) = |value| {
            $(let _ = &value.$field;)*
        };
    };
}

/// Mirrors the unit enum `$source` as the Luau userdata enum `$name`, with
/// conversions both ways; the exhaustive matches keep the variants in step.
macro_rules! mirror_userdata_enum {
    ($mirror:ident for $source:path, $name:literal, [$($variant:ident),+ $(,)?]) => {
        #[harmony_macros::userdata(name = $name)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum $mirror {
            $($variant),+
        }

        impl From<$mirror> for $source {
            fn from(value: $mirror) -> Self {
                match value {
                    $($mirror::$variant => Self::$variant),+
                }
            }
        }

        impl From<$source> for $mirror {
            fn from(value: $source) -> Self {
                match value {
                    $(<$source>::$variant => Self::$variant),+
                }
            }
        }
    };
}

/// Mirrors the unit enum `$source` as the Luau string union `$name`, spelled
/// as `$source` serializes, with conversions both ways; the exhaustive
/// matches keep the variants in step.
macro_rules! mirror_string_enum {
    (
        $mirror:ident for $source:path,
        $name:literal,
        $(description = $description:literal,)?
        [$($(#[$attr:meta])* $variant:ident),+ $(,)?]
    ) => {
        #[harmony_macros::string_enum(name = $name $(, description = $description)?)]
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum $mirror {
            $($(#[$attr])* $variant),+
        }

        impl From<$mirror> for $source {
            fn from(value: $mirror) -> Self {
                match value {
                    $($mirror::$variant => Self::$variant),+
                }
            }
        }

        impl From<$source> for $mirror {
            fn from(value: $source) -> Self {
                match value {
                    $(<$source>::$variant => Self::$variant),+
                }
            }
        }
    };
}

mirror_userdata_enum!(LuauEntityType for crate::services::EntityType, "EntityType", [
    Release, Artist, Track,
]);

mirror_userdata_enum!(LuauMixSeedType for crate::services::mix::MixSeedType, "SeedType", [
    Track, Release, Artist, Genre, Playlist,
]);

mirror_userdata_enum!(LuauArtistType for lyra_db::ArtistType, "ArtistType", [
    Person, Group, Character, Orchestra, Choir,
]);

mirror_userdata_enum!(LuauArtistRelationType for lyra_db::ArtistRelationType, "ArtistRelationType", [
    VoiceActor, MemberOf,
]);

mirror_userdata_enum!(LuauCreditType for lyra_db::CreditType, "CreditType", [
    Artist,
    Vocalist,
    Instrumentalist,
    Composer,
    Lyricist,
    Arranger,
    Writer,
    Producer,
    Conductor,
    Engineer,
    Mixer,
    Remixer,
]);

/// [`lyra_db::releases::ReleaseType`] as described to Luau.
#[cfg(feature = "docgen")]
pub(crate) struct LuauReleaseType;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LuauReleaseType {
    fn luau_type() -> LuauType {
        LuauType::literal("ReleaseType")
    }
}

#[cfg(feature = "docgen")]
impl DescribeUserData for LuauReleaseType {
    fn class_descriptor() -> ClassDescriptor {
        let fields = [
            "Album",
            "Single",
            "EP",
            "Compilation",
            "Soundtrack",
            "Live",
            "Remix",
            "Broadcast",
            "Other",
            "Unknown",
        ]
        .into_iter()
        .map(|name| FieldDescriptor {
            name,
            ty: Self::luau_type(),
            description: None,
        })
        .collect();
        ClassDescriptor {
            name: "ReleaseType",
            description: None,
            fields,
            methods: vec![],
        }
    }
}

const _: fn(lyra_db::releases::ReleaseType) = |value| match value {
    lyra_db::releases::ReleaseType::Album
    | lyra_db::releases::ReleaseType::Single
    | lyra_db::releases::ReleaseType::EP
    | lyra_db::releases::ReleaseType::Compilation
    | lyra_db::releases::ReleaseType::Soundtrack
    | lyra_db::releases::ReleaseType::Live
    | lyra_db::releases::ReleaseType::Remix
    | lyra_db::releases::ReleaseType::Broadcast
    | lyra_db::releases::ReleaseType::Other
    | lyra_db::releases::ReleaseType::Unknown => {}
};

/// [`lyra_db::NodeId`]: a database id, a number in Luau.
pub(crate) struct LuauNodeId;

impl LuauTypeInfo for LuauNodeId {
    fn luau_type() -> LuauType {
        f64::luau_type()
    }
}

mirror_string_enum!(LuauPlaybackState for lyra_db::PlaybackState, "PlaybackState", [
    Playing, Paused, Stopped, Buffering, Completed,
]);

mirror_string_enum!(
    LuauPlaybackEvent for crate::services::playback_sessions::PlaybackEvent,
    "PlaybackEvent",
    description = "Playback event reported by plugin helpers.",
    [Started, Progress, Stopped, Completed]
);

describe_record!(
    LuauRelease for lyra_db::Release,
    "Release",
    fields {
        db_id: Option<LuauNodeId> as "db_id",
        id: String as "id",
        release_title: String as "release_title",
        sort_title: Option<String> as "sort_title",
        release_type: Option<LuauReleaseType> as "release_type",
        release_date: Option<String> as "release_date",
        media_formats: Option<Vec<String>> as "media_formats",
        barcode: Option<String> as "barcode",
        locked: Option<bool> as "locked",
        created_at_ms: Option<u64> as "created_at_ms",
        file_created_at_ms: Option<u64> as "file_created_at_ms",
    }
);

describe_record!(
    LuauTrack for lyra_db::Track,
    "Track",
    fields {
        db_id: Option<LuauNodeId> as "db_id",
        id: String as "id",
        track_title: String as "track_title",
        sort_title: Option<String> as "sort_title",
        year: Option<u32> as "year",
        disc: Option<u32> as "disc",
        disc_total: Option<u32> as "disc_total",
        track: Option<u32> as "track",
        track_total: Option<u32> as "track_total",
        duration_ms: Option<u64> as "duration_ms",
        track_gain_db: Option<f64> as "track_gain_db",
        album_gain_db: Option<f64> as "album_gain_db",
        locked: Option<bool> as "locked",
        created_at_ms: Option<u64> as "created_at_ms",
        file_created_at_ms: Option<u64> as "file_created_at_ms",
    }
);

describe_record!(
    LuauArtist for lyra_db::Artist,
    "Artist",
    fields {
        db_id: Option<LuauNodeId> as "db_id",
        id: String as "id",
        artist_name: String as "artist_name",
        scan_name: String as "scan_name",
        sort_name: Option<String> as "sort_name",
        artist_type: Option<LuauArtistType> as "artist_type",
        description: Option<String> as "description",
        locked: Option<bool> as "locked",
        created_at_ms: Option<u64> as "created_at_ms",
    }
);

/// [`crate::services::entries::PlacedEntry`] as plugins read it. Only callers who manage
/// libraries get `full_path`.
#[derive(serde::Serialize)]
pub(crate) struct EntryInfo {
    db_id: Option<i64>,
    id: String,
    parent_id: i64,
    kind: String,
    file_kind: Option<String>,
    name: String,
    full_path: Option<String>,
    hash: Option<String>,
    size: u64,
    file_modified_at_ms: u64,
    file_created_at_ms: u64,
}

impl EntryInfo {
    pub(crate) fn new(placed: PlacedEntry, include_full_path: bool) -> Self {
        let PlacedEntry { entry, parent_id } = placed;
        Self {
            db_id: entry.db_id.map(|id| id.0),
            id: entry.id,
            parent_id: parent_id.0,
            kind: entry.kind.to_string(),
            file_kind: entry.file_kind,
            name: entry.name,
            full_path: include_full_path.then(|| entry.full_path.to_string_lossy().into_owned()),
            hash: entry.hash,
            size: entry.size,
            file_modified_at_ms: entry.file_modified_at_ms,
            file_created_at_ms: entry.file_created_at_ms,
        }
    }

    /// The entry as `viewer` reads it.
    pub(crate) fn shown_to(placed: PlacedEntry, viewer: &Viewer) -> Self {
        let include_full_path = match viewer {
            Viewer::User { principal, .. } => lyra_db::roles::has_permission(
                &principal.permissions,
                lyra_db::Permission::ManageLibraries,
            ),
            Viewer::System => false,
        };
        Self::new(placed, include_full_path)
    }
}

impl LuauTypeInfo for EntryInfo {
    fn luau_type() -> LuauType {
        LuauType::named("EntryInfo")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for EntryInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name, ty, description| FieldDescriptor {
            name,
            ty,
            description,
        };
        InterfaceDescriptor {
            name: "EntryInfo",
            description: Some("A file or directory in a library."),
            fields: vec![
                field("db_id", Option::<LuauNodeId>::luau_type(), None),
                field("id", String::luau_type(), None),
                field(
                    "parent_id",
                    LuauNodeId::luau_type(),
                    Some("The library or directory entry holding this entry."),
                ),
                field(
                    "kind",
                    LuauType::union(vec![
                        LuauType::string_literal("file"),
                        LuauType::string_literal("dir"),
                    ]),
                    None,
                ),
                field(
                    "file_kind",
                    LuauType::optional(LuauType::union(vec![
                        LuauType::string_literal("audio"),
                        LuauType::string_literal("cue"),
                    ])),
                    None,
                ),
                field("name", String::luau_type(), None),
                field(
                    "full_path",
                    Option::<String>::luau_type(),
                    Some("The path on disk, for callers who manage libraries."),
                ),
                field("hash", Option::<String>::luau_type(), None),
                field("size", u64::luau_type(), None),
                field("file_modified_at_ms", u64::luau_type(), None),
                field("file_created_at_ms", u64::luau_type(), None),
            ],
        }
    }
}

/// [`crate::services::covers::ResolvedCover`] as plugins read it.
#[derive(serde::Serialize)]
pub(crate) struct CoverInfo {
    path: String,
    mime_type: String,
    hash: String,
    blurhash: Option<String>,
    release_id: i64,
}

impl From<ResolvedCover> for CoverInfo {
    fn from(resolved: ResolvedCover) -> Self {
        let ResolvedCover { owner_db_id, cover } = resolved;
        Self {
            path: cover.path,
            mime_type: cover.mime_type,
            hash: cover.hash,
            blurhash: cover.blurhash,
            release_id: owner_db_id.0,
        }
    }
}

impl LuauTypeInfo for CoverInfo {
    fn luau_type() -> LuauType {
        LuauType::named("Cover")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for CoverInfo {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name, ty, description| FieldDescriptor {
            name,
            ty,
            description,
        };
        InterfaceDescriptor {
            name: "Cover",
            description: Some("An item's cover image."),
            fields: vec![
                field("path", String::luau_type(), Some("The image file on disk.")),
                field("mime_type", String::luau_type(), None),
                field(
                    "hash",
                    String::luau_type(),
                    Some("Changes whenever the image does."),
                ),
                field("blurhash", Option::<String>::luau_type(), None),
                field(
                    "release_id",
                    LuauNodeId::luau_type(),
                    Some(
                        "The node the cover belongs to: the item itself, a track's release, or the release a playlist or genre shows.",
                    ),
                ),
            ],
        }
    }
}

/// [`crate::services::providers::IdLink`] as described to Luau.
#[cfg(feature = "docgen")]
pub(crate) struct LuauIdLink;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LuauIdLink {
    fn luau_type() -> LuauType {
        LuauType::literal("IdLink")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauIdLink {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name, ty, description| FieldDescriptor {
            name,
            ty,
            description,
        };
        InterfaceDescriptor {
            name: "IdLink",
            description: Some(
                "Web link for one external ID, from the `links` include. Computed when read.",
            ),
            fields: vec![
                field("provider_id", String::luau_type(), None),
                field(
                    "label",
                    String::luau_type(),
                    Some(
                        "Name of the site the link points to: the registration's `label`, else the provider's display name.",
                    ),
                ),
                field("scheme", Option::<String>::luau_type(), None),
                field("id_type", String::luau_type(), None),
                field("id", String::luau_type(), None),
                field("url", String::luau_type(), Some("Absolute http(s) URL.")),
            ],
        }
    }
}

const _: fn(&crate::services::providers::IdLink) = |value| {
    let _ = (
        &value.provider_id,
        &value.label,
        &value.scheme,
        &value.id_type,
        &value.id,
        &value.url,
    );
};

#[cfg(test)]
mod tests {
    use harmony_luau::StringEnum;
    use serde::de::DeserializeOwned;

    use super::*;

    /// Each string `M` accepts deserializes to the `S` that converts back to it.
    fn assert_spelled_as_serialized<M, S>()
    where
        M: StringEnum + From<S>,
        S: DeserializeOwned,
    {
        for value in M::VALUES {
            let source: S = serde_json::from_value(serde_json::Value::from(*value))
                .unwrap_or_else(|error| panic!("{} \"{value}\": {error}", M::NAME));
            assert_eq!(M::from(source).as_str(), *value);
        }
    }

    #[test]
    fn string_enum_mirrors_match_their_serialized_spelling() {
        assert_spelled_as_serialized::<LuauPlaybackState, lyra_db::PlaybackState>();
        assert_spelled_as_serialized::<
            LuauPlaybackEvent,
            crate::services::playback_sessions::PlaybackEvent,
        >();
    }
}
