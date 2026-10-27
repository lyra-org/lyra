// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! How entity projections look from Luau.

use std::sync::OnceLock;

use harmony_luau::{
    ClassDescriptor,
    DescribeInterface,
    DescribeTypeAlias,
    DescribeUserData,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};

use crate::plugins::types::{
    LuauArtist,
    LuauCreditType,
    LuauIdLink,
    LuauNodeId,
    LuauRelease,
    LuauTrack,
};
use crate::services::{
    entities::{
        ArtistCreditSource,
        ArtistProjectionIncludes,
        ArtistProjectionInfo,
        ArtistProjectionKind,
        CreditProjectionInfo,
        CreditedArtistProjectionInfo,
        EntityLookupHints,
        ExternalIdsByProvider,
        ProjectionEntryInfo,
        ReleaseProjectionIncludes,
        ReleaseProjectionInfo,
        ReleaseProjectionKind,
        ReleaseProjectionTrack,
        TrackProjectionIncludes,
        TrackProjectionInfo,
        TrackProjectionKind,
    },
    providers::IdentifiersByScheme,
};

/// Describes `$source` to Luau as the interface `$name`. The field list is
/// checked against `$source`, so a renamed or removed field fails to build.
macro_rules! describe_interface {
    ($marker:ident for $source:ty, $name:literal, {$($field:tt : $field_ty:ty as $key:literal),* $(,)?}) => {
                pub(crate) struct $marker;

                impl LuauTypeInfo for $marker {
            fn luau_type() -> LuauType {
                LuauType::literal($name)
            }
        }

                impl DescribeInterface for $marker {
            fn interface_descriptor() -> InterfaceDescriptor {
                InterfaceDescriptor {
                    name: $name,
                    description: None,
                    fields: vec![
                        $(FieldDescriptor {
                            name: $key,
                            ty: <$field_ty as LuauTypeInfo>::luau_type(),
                            description: None,
                        }),*
                    ],
                }
            }
        }

        const _: fn(&$source) = |value| {
            $(let _ = &value.$field;)*
        };
    };
}

/// Describes `$source`'s serialized tag as the string literal type `$name`.
macro_rules! describe_projection_kind {
    ($marker:ident for $source:ty, $name:literal, $doc:literal) => {
        pub(crate) struct $marker;

        impl LuauTypeInfo for $marker {
            fn luau_type() -> LuauType {
                static QUOTED: OnceLock<&'static str> = OnceLock::new();
                LuauType::literal(QUOTED.get_or_init(|| {
                    Box::leak(format!("\"{}\"", <$source>::LITERAL).into_boxed_str())
                }))
            }
        }

        impl DescribeTypeAlias for $marker {
            fn type_alias_descriptor() -> TypeAliasDescriptor {
                TypeAliasDescriptor::new($name, Self::luau_type(), Some($doc))
            }
        }
    };
}

describe_projection_kind!(
    LuauReleaseProjectionKind for ReleaseProjectionKind,
    "ReleaseProjectionKind",
    "Release entity projection kind."
);

describe_projection_kind!(
    LuauTrackProjectionKind for TrackProjectionKind,
    "TrackProjectionKind",
    "Track entity projection kind."
);

describe_projection_kind!(
    LuauArtistProjectionKind for ArtistProjectionKind,
    "ArtistProjectionKind",
    "Artist entity projection kind."
);

describe_interface!(LuauEntityLookupHints for EntityLookupHints, "EntityLookupHints", {
    artist_name: Option<String> as "artist_name",
    release_title: Option<String> as "release_title",
    year: Option<u32> as "year",
});

describe_interface!(LuauProjectionEntryInfo for ProjectionEntryInfo, "ProjectionEntryInfo", {
    db_id: Option<LuauNodeId> as "db_id",
    id: String as "id",
    full_path: String as "full_path",
    kind: String as "kind",
    name: String as "name",
    hash: Option<String> as "hash",
    size: u64 as "size",
    mtime: u64 as "mtime",
});

describe_interface!(LuauCreditProjectionInfo for CreditProjectionInfo, "CreditProjectionInfo", {
    r#type: LuauCreditType as "type",
    detail: Option<String> as "detail",
});

describe_interface!(LuauCreditedArtistProjectionInfo for CreditedArtistProjectionInfo, "CreditedArtistProjectionInfo", {
    artist: LuauArtist as "artist",
    credit: LuauCreditProjectionInfo as "credit",
    source: LuauArtistCreditSource as "source",
});

describe_interface!(LuauReleaseProjectionTrack for ReleaseProjectionTrack, "ReleaseProjectionTrack", {
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
    external_ids: ExternalIdsByProvider as "external_ids",
    identifiers: IdentifiersByScheme as "identifiers",
    links: Option<Vec<LuauIdLink>> as "links",
    artists: Vec<LuauArtist> as "artists",
    lookup_hints: LuauEntityLookupHints as "lookup_hints",
});

describe_interface!(LuauReleaseProjectionIncludes for ReleaseProjectionIncludes, "ReleaseProjectionIncludes", {
    external_ids: Option<ExternalIdsByProvider> as "external_ids",
    identifiers: Option<IdentifiersByScheme> as "identifiers",
    links: Option<Vec<LuauIdLink>> as "links",
    artists: Option<Vec<LuauArtist>> as "artists",
    tracks: Option<Vec<LuauReleaseProjectionTrack>> as "tracks",
    credits: Option<Vec<LuauCreditedArtistProjectionInfo>> as "credits",
});

describe_interface!(LuauTrackProjectionIncludes for TrackProjectionIncludes, "TrackProjectionIncludes", {
    external_ids: Option<ExternalIdsByProvider> as "external_ids",
    identifiers: Option<IdentifiersByScheme> as "identifiers",
    links: Option<Vec<LuauIdLink>> as "links",
    releases: Option<Vec<LuauRelease>> as "releases",
    artists: Option<Vec<LuauArtist>> as "artists",
    entries: Option<Vec<LuauProjectionEntryInfo>> as "entries",
    credits: Option<Vec<LuauCreditedArtistProjectionInfo>> as "credits",
});

describe_interface!(LuauArtistProjectionIncludes for ArtistProjectionIncludes, "ArtistProjectionIncludes", {
    external_ids: Option<ExternalIdsByProvider> as "external_ids",
    identifiers: Option<IdentifiersByScheme> as "identifiers",
    links: Option<Vec<LuauIdLink>> as "links",
    releases: Option<Vec<LuauRelease>> as "releases",
    tracks: Option<Vec<LuauTrack>> as "tracks",
});

describe_interface!(LuauReleaseProjectionInfo for ReleaseProjectionInfo, "ReleaseProjectionInfo", {
    entity_type: LuauReleaseProjectionKind as "entity_type",
    entity: LuauRelease as "entity",
    lookup_hints: LuauEntityLookupHints as "lookup_hints",
    includes: LuauReleaseProjectionIncludes as "includes",
});

describe_interface!(LuauTrackProjectionInfo for TrackProjectionInfo, "TrackProjectionInfo", {
    entity_type: LuauTrackProjectionKind as "entity_type",
    entity: LuauTrack as "entity",
    includes: LuauTrackProjectionIncludes as "includes",
});

describe_interface!(LuauArtistProjectionInfo for ArtistProjectionInfo, "ArtistProjectionInfo", {
    entity_type: LuauArtistProjectionKind as "entity_type",
    entity: LuauArtist as "entity",
    includes: LuauArtistProjectionIncludes as "includes",
});

/// [`ArtistCreditSource`] as described to Luau.
pub(crate) struct LuauArtistCreditSource;

impl LuauTypeInfo for LuauArtistCreditSource {
    fn luau_type() -> LuauType {
        LuauType::literal("ArtistCreditSource")
    }
}

impl DescribeUserData for LuauArtistCreditSource {
    fn class_descriptor() -> ClassDescriptor {
        ClassDescriptor {
            name: "ArtistCreditSource",
            description: None,
            fields: vec![
                FieldDescriptor {
                    name: "Track",
                    ty: Self::luau_type(),
                    description: None,
                },
                FieldDescriptor {
                    name: "Release",
                    ty: Self::luau_type(),
                    description: None,
                },
            ],
            methods: vec![],
        }
    }
}

const _: fn(ArtistCreditSource) = |value| match value {
    ArtistCreditSource::Track | ArtistCreditSource::Release => {}
};
