// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How the database and service types plugins exchange look from Luau.
//! Records cross as serialized tables and are only described here; the enums
//! plugins pass back as userdata get Luau-side mirrors that convert to the
//! types they mirror.

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

mirror_userdata_enum!(LuauEntityType for crate::services::EntityType, "EntityType", [
    Release, Artist, Track,
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

/// [`lyra_db::PlaybackState`] as the strings it serializes to.
pub(crate) struct LuauPlaybackState;

impl LuauTypeInfo for LuauPlaybackState {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            LuauType::literal("\"playing\""),
            LuauType::literal("\"paused\""),
            LuauType::literal("\"stopped\""),
            LuauType::literal("\"buffering\""),
            LuauType::literal("\"completed\""),
        ])
    }
}

const _: fn(lyra_db::PlaybackState) = |value| match value {
    lyra_db::PlaybackState::Playing
    | lyra_db::PlaybackState::Paused
    | lyra_db::PlaybackState::Stopped
    | lyra_db::PlaybackState::Buffering
    | lyra_db::PlaybackState::Completed => {}
};

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
        created_at: Option<u64> as "created_at",
        ctime: Option<u64> as "ctime",
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
        created_at: Option<u64> as "created_at",
        ctime: Option<u64> as "ctime",
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
        verified: bool as "verified",
        locked: Option<bool> as "locked",
        created_at: Option<u64> as "created_at",
    }
);

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
