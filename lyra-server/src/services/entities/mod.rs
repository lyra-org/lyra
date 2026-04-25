// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod context;
mod projection;

use std::collections::{
    BTreeMap,
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};
use harmony_luau::{
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use lyra_metadata::LookupHints;
use serde::Serialize;

use crate::db::{
    self,
    Artist,
    Credit,
    NodeId,
    Release,
    Track,
};

pub(crate) use context::{
    EntityContextError,
    build_entity_provider_context,
    build_release_context,
};
pub(crate) use projection::{
    project_entities,
    project_entity,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum EntityInclude {
    ExternalIds,
    Releases,
    Artists,
    Tracks,
    Entries,
}

impl EntityInclude {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "external_ids" => Some(Self::ExternalIds),
            "releases" => Some(Self::Releases),
            "artists" => Some(Self::Artists),
            "tracks" => Some(Self::Tracks),
            "entries" => Some(Self::Entries),
            _ => None,
        }
    }

    pub(crate) const fn as_key(self) -> &'static str {
        match self {
            Self::ExternalIds => "external_ids",
            Self::Releases => "releases",
            Self::Artists => "artists",
            Self::Tracks => "tracks",
            Self::Entries => "entries",
        }
    }

    pub(crate) const ALL: &[Self] = &[
        Self::ExternalIds,
        Self::Releases,
        Self::Artists,
        Self::Tracks,
        Self::Entries,
    ];
}

macro_rules! projection_kind {
    ($name:ident, $variant:ident, $literal:literal, $doc:literal) => {
        #[derive(Clone, Copy, Debug, Serialize)]
        pub(crate) enum $name {
            #[serde(rename = $literal)]
            $variant,
        }

        impl LuauTypeInfo for $name {
            fn luau_type() -> LuauType {
                LuauType::literal(concat!("\"", $literal, "\""))
            }
        }

        impl DescribeTypeAlias for $name {
            fn type_alias_descriptor() -> TypeAliasDescriptor {
                TypeAliasDescriptor::new(stringify!($name), Self::luau_type(), Some($doc))
            }
        }
    };
}

projection_kind!(
    ReleaseProjectionKind,
    Release,
    "release",
    "Release entity projection kind."
);
projection_kind!(
    TrackProjectionKind,
    Track,
    "track",
    "Track entity projection kind."
);
projection_kind!(
    ArtistProjectionKind,
    Artist,
    "artist",
    "Artist entity projection kind."
);

#[derive(Clone, Debug, Default, Serialize)]
#[harmony_macros::interface]
pub(crate) struct EntityLookupHints {
    pub(crate) artist_name: Option<String>,
    pub(crate) release_title: Option<String>,
    pub(crate) year: Option<u32>,
}

impl From<LookupHints> for EntityLookupHints {
    fn from(value: LookupHints) -> Self {
        Self {
            artist_name: value.artist_name,
            release_title: value.album_title,
            year: value.year,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ProjectionEntryInfo {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) full_path: String,
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) hash: Option<String>,
    pub(crate) size: u64,
    pub(crate) mtime: u64,
}

impl From<db::Entry> for ProjectionEntryInfo {
    fn from(value: db::Entry) -> Self {
        Self {
            db_id: value.db_id.map(NodeId::from),
            id: value.id,
            full_path: value.full_path.to_string_lossy().to_string(),
            kind: value.kind.to_string(),
            name: value.name,
            hash: value.hash,
            size: value.size,
            mtime: value.mtime,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ReleaseProjectionTrack {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) track_title: String,
    pub(crate) sort_title: Option<String>,
    pub(crate) year: Option<u32>,
    pub(crate) disc: Option<u32>,
    pub(crate) disc_total: Option<u32>,
    pub(crate) track: Option<u32>,
    pub(crate) track_total: Option<u32>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) sample_rate_hz: Option<u32>,
    pub(crate) channel_count: Option<u32>,
    pub(crate) bit_depth: Option<u32>,
    pub(crate) bitrate_bps: Option<u32>,
    pub(crate) locked: Option<bool>,
    pub(crate) created_at: Option<u64>,
    pub(crate) ctime: Option<u64>,
    pub(crate) external_ids: BTreeMap<String, String>,
    pub(crate) artists: Vec<Artist>,
    pub(crate) lookup_hints: EntityLookupHints,
}

impl ReleaseProjectionTrack {
    pub(super) fn from_track(
        track: Track,
        external_ids: BTreeMap<String, String>,
        artists: Vec<Artist>,
        lookup_hints: EntityLookupHints,
    ) -> Self {
        let track_db_id = track.db_id;
        Self {
            db_id: track_db_id,
            id: track.id,
            track_title: track.track_title,
            sort_title: track.sort_title,
            year: track.year,
            disc: track.disc,
            disc_total: track.disc_total,
            track: track.track,
            track_total: track.track_total,
            duration_ms: track.duration_ms,
            sample_rate_hz: track.sample_rate_hz,
            channel_count: track.channel_count,
            bit_depth: track.bit_depth,
            bitrate_bps: track.bitrate_bps,
            locked: track.locked,
            created_at: track.created_at,
            ctime: track.ctime,
            external_ids,
            artists,
            lookup_hints,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ReleaseProjectionIncludes {
    pub(crate) external_ids: Option<BTreeMap<String, String>>,
    pub(crate) artists: Option<Vec<Artist>>,
    pub(crate) tracks: Option<Vec<ReleaseProjectionTrack>>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[harmony_macros::interface]
pub(crate) struct TrackProjectionIncludes {
    pub(crate) external_ids: Option<BTreeMap<String, String>>,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) artists: Option<Vec<Artist>>,
    pub(crate) entries: Option<Vec<ProjectionEntryInfo>>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ArtistProjectionIncludes {
    pub(crate) external_ids: Option<BTreeMap<String, String>>,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) tracks: Option<Vec<Track>>,
}

#[derive(Clone, Debug, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ReleaseProjectionInfo {
    pub(crate) entity_type: ReleaseProjectionKind,
    pub(crate) entity: Release,
    pub(crate) lookup_hints: EntityLookupHints,
    pub(crate) includes: ReleaseProjectionIncludes,
}

#[derive(Clone, Debug, Serialize)]
#[harmony_macros::interface]
pub(crate) struct TrackProjectionInfo {
    pub(crate) entity_type: TrackProjectionKind,
    pub(crate) entity: Track,
    pub(crate) includes: TrackProjectionIncludes,
}

#[derive(Clone, Debug, Serialize)]
#[harmony_macros::interface]
pub(crate) struct ArtistProjectionInfo {
    pub(crate) entity_type: ArtistProjectionKind,
    pub(crate) entity: Artist,
    pub(crate) includes: ArtistProjectionIncludes,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum EntityProjectionInfo {
    Release(ReleaseProjectionInfo),
    Track(TrackProjectionInfo),
    Artist(ArtistProjectionInfo),
}

impl LuauTypeInfo for EntityProjectionInfo {
    fn luau_type() -> LuauType {
        LuauType::literal("EntityProjectionInfo")
    }
}

impl DescribeTypeAlias for EntityProjectionInfo {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "EntityProjectionInfo",
            LuauType::union(vec![
                LuauType::literal("ReleaseProjectionInfo"),
                LuauType::literal("TrackProjectionInfo"),
                LuauType::literal("ArtistProjectionInfo"),
            ]),
            Some("Typed entity projection keyed by entity_type."),
        )
    }
}

pub(super) fn dedupe_artists(artists: Vec<Artist>) -> Vec<Artist> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for artist in artists {
        let Some(artist_id) = artist.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        if seen.insert(artist_id.0) {
            deduped.push(artist);
        }
    }

    deduped
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtistCreditSource {
    Track,
    Release,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedCreditedArtist {
    pub(crate) artist: Artist,
    pub(crate) credit: Credit,
    pub(crate) source: ArtistCreditSource,
}

pub(crate) fn credited_artists_with_source(
    credited: Vec<db::CreditedArtist>,
    source: ArtistCreditSource,
) -> Vec<ResolvedCreditedArtist> {
    credited
        .into_iter()
        .map(|c| ResolvedCreditedArtist {
            artist: c.artist,
            credit: c.credit,
            source,
        })
        .collect()
}

pub(crate) fn credited_artist_map_with_source(
    map: HashMap<DbId, Vec<db::CreditedArtist>>,
    source: ArtistCreditSource,
) -> HashMap<DbId, Vec<ResolvedCreditedArtist>> {
    map.into_iter()
        .map(|(owner_id, artists)| (owner_id, credited_artists_with_source(artists, source)))
        .collect()
}

fn dedupe_credited_artists(artists: Vec<ResolvedCreditedArtist>) -> Vec<ResolvedCreditedArtist> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for artist in artists {
        let Some(artist_id) = artist.artist.db_id.clone().map(Into::<DbId>::into) else {
            continue;
        };
        let key = (
            artist_id.0,
            artist.credit.credit_type,
            artist.credit.detail.clone(),
        );
        if seen.insert(key) {
            deduped.push(artist);
        }
    }

    deduped
}

pub(crate) fn resolve_track_artists(db: &DbAny, track_id: DbId) -> anyhow::Result<Vec<Artist>> {
    let direct = db::artists::get(db, track_id)?;
    if !direct.is_empty() {
        return Ok(dedupe_artists(direct));
    }

    let mut artists = Vec::new();
    let mut release_ids: Vec<DbId> = db::releases::get_by_track(db, track_id)?
        .into_iter()
        .filter_map(|release| release.db_id.map(Into::<DbId>::into))
        .collect();
    release_ids.sort_by_key(|release_id| release_id.0);
    for release_id in release_ids {
        artists.extend(db::artists::get(db, release_id)?);
    }

    Ok(dedupe_artists(artists))
}

pub(crate) fn resolve_track_credited_artists(
    db: &DbAny,
    track_id: DbId,
) -> anyhow::Result<Vec<ResolvedCreditedArtist>> {
    let direct = credited_artists_with_source(
        db::artists::get_credited(db, track_id)?,
        ArtistCreditSource::Track,
    );
    if !direct.is_empty() {
        return Ok(dedupe_credited_artists(direct));
    }

    let mut artists = Vec::new();
    let mut release_ids: Vec<DbId> = db::releases::get_by_track(db, track_id)?
        .into_iter()
        .filter_map(|release| release.db_id.map(Into::<DbId>::into))
        .collect();
    release_ids.sort_by_key(|release_id| release_id.0);
    for release_id in release_ids {
        artists.extend(credited_artists_with_source(
            db::artists::get_credited(db, release_id)?,
            ArtistCreditSource::Release,
        ));
    }

    Ok(dedupe_credited_artists(artists))
}

fn dedupe_db_ids(ids: &[DbId]) -> Vec<DbId> {
    let mut unique = Vec::new();
    let mut seen = HashSet::new();
    for id in ids {
        if seen.insert(*id) {
            unique.push(*id);
        }
    }
    unique
}

/// Pre-fetched context to avoid redundant DB queries in track artist resolution.
#[derive(Default)]
pub(crate) struct TrackArtistContext<'a> {
    pub(crate) releases_by_track: Option<&'a HashMap<DbId, Vec<Release>>>,
    pub(crate) artists_by_release: Option<&'a HashMap<DbId, Vec<Artist>>>,
}

#[derive(Default)]
pub(crate) struct TrackCreditedArtistContext<'a> {
    pub(crate) releases_by_track: Option<&'a HashMap<DbId, Vec<Release>>>,
    pub(crate) credited_artists_by_release: Option<&'a HashMap<DbId, Vec<ResolvedCreditedArtist>>>,
    /// When set, the release fallback only considers this specific release,
    /// preventing credits from other releases bleeding into the response.
    pub(crate) scope_release_id: Option<DbId>,
}

pub(crate) fn resolve_track_artists_with_context(
    db: &DbAny,
    track_ids: &[DbId],
    ctx: &TrackArtistContext<'_>,
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    let track_ids = dedupe_db_ids(track_ids);
    let mut artists_by_track = db::artists::get_many_by_owner(db, &track_ids)?;
    let unresolved: Vec<DbId> = track_ids
        .iter()
        .copied()
        .filter(|track_id| {
            artists_by_track
                .get(track_id)
                .is_none_or(|artists| artists.is_empty())
        })
        .collect();

    if unresolved.is_empty() {
        return Ok(artists_by_track);
    }

    let fetched_releases_by_track;
    let releases_by_track: &HashMap<DbId, Vec<Release>> = match ctx.releases_by_track {
        Some(pre) => pre,
        None => {
            fetched_releases_by_track = db::releases::get_by_tracks(db, &unresolved)?;
            &fetched_releases_by_track
        }
    };

    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for track_id in &unresolved {
        let Some(releases) = releases_by_track.get(track_id) else {
            continue;
        };
        for release in releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }

    let fetched_artists_by_release;
    let artists_by_release: &HashMap<DbId, Vec<Artist>> = match ctx.artists_by_release {
        Some(pre) => {
            let missing: Vec<DbId> = release_ids
                .iter()
                .copied()
                .filter(|id| !pre.contains_key(id))
                .collect();
            if missing.is_empty() {
                pre
            } else {
                let mut merged = pre.clone();
                merged.extend(db::artists::get_many_by_owner(db, &missing)?);
                fetched_artists_by_release = merged;
                &fetched_artists_by_release
            }
        }
        None => {
            fetched_artists_by_release = db::artists::get_many_by_owner(db, &release_ids)?;
            &fetched_artists_by_release
        }
    };

    for track_id in unresolved {
        let mut artists = artists_by_track.remove(&track_id).unwrap_or_default();
        if artists.is_empty() {
            if let Some(releases) = releases_by_track.get(&track_id) {
                for release in releases {
                    let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                        continue;
                    };
                    if let Some(release_artists) = artists_by_release.get(&release_id) {
                        artists.extend(release_artists.clone());
                    }
                }
            }
        }
        artists_by_track.insert(track_id, dedupe_artists(artists));
    }

    for track_id in track_ids {
        artists_by_track.entry(track_id).or_default();
    }

    Ok(artists_by_track)
}

pub(crate) fn resolve_track_credited_artists_with_context(
    db: &DbAny,
    track_ids: &[DbId],
    ctx: &TrackCreditedArtistContext<'_>,
) -> anyhow::Result<HashMap<DbId, Vec<ResolvedCreditedArtist>>> {
    let track_ids = dedupe_db_ids(track_ids);
    let mut artists_by_track = credited_artist_map_with_source(
        db::artists::get_credited_many_by_owner(db, &track_ids)?,
        ArtistCreditSource::Track,
    );
    let unresolved: Vec<DbId> = track_ids
        .iter()
        .copied()
        .filter(|track_id| {
            artists_by_track
                .get(track_id)
                .is_none_or(|artists| artists.is_empty())
        })
        .collect();

    if unresolved.is_empty() {
        return Ok(artists_by_track);
    }

    // Scoped path: fallback artists come from a single known release, so we
    // skip the per-track release lookup and only fetch that release's artists
    // (if not already in context).
    if let Some(scope_id) = ctx.scope_release_id {
        let fetched;
        let scope_artists: Option<&Vec<ResolvedCreditedArtist>> = match ctx
            .credited_artists_by_release
            .and_then(|pre| pre.get(&scope_id))
        {
            Some(artists) => Some(artists),
            None => {
                fetched = credited_artists_with_source(
                    db::artists::get_credited(db, scope_id)?,
                    ArtistCreditSource::Release,
                );
                Some(&fetched)
            }
        };

        for track_id in unresolved {
            let mut artists = artists_by_track.remove(&track_id).unwrap_or_default();
            if artists.is_empty()
                && let Some(release_artists) = scope_artists
            {
                artists.extend(release_artists.clone());
            }
            artists_by_track.insert(track_id, dedupe_credited_artists(artists));
        }

        for track_id in track_ids {
            artists_by_track.entry(track_id).or_default();
        }

        return Ok(artists_by_track);
    }

    // Unscoped path: a track may belong to multiple releases; pull artists from
    // all of them and dedupe.
    let fetched_releases_by_track;
    let releases_by_track: &HashMap<DbId, Vec<Release>> = match ctx.releases_by_track {
        Some(pre) => pre,
        None => {
            fetched_releases_by_track = db::releases::get_by_tracks(db, &unresolved)?;
            &fetched_releases_by_track
        }
    };

    let mut release_ids = Vec::new();
    let mut seen_release_ids = HashSet::new();
    for track_id in &unresolved {
        let Some(releases) = releases_by_track.get(track_id) else {
            continue;
        };
        for release in releases {
            let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                continue;
            };
            if seen_release_ids.insert(release_id) {
                release_ids.push(release_id);
            }
        }
    }

    let fetched_artists_by_release;
    let artists_by_release: &HashMap<DbId, Vec<ResolvedCreditedArtist>> =
        match ctx.credited_artists_by_release {
            Some(pre) => {
                let missing: Vec<DbId> = release_ids
                    .iter()
                    .copied()
                    .filter(|id| !pre.contains_key(id))
                    .collect();
                if missing.is_empty() {
                    pre
                } else {
                    let mut merged = pre.clone();
                    merged.extend(credited_artist_map_with_source(
                        db::artists::get_credited_many_by_owner(db, &missing)?,
                        ArtistCreditSource::Release,
                    ));
                    fetched_artists_by_release = merged;
                    &fetched_artists_by_release
                }
            }
            None => {
                fetched_artists_by_release = credited_artist_map_with_source(
                    db::artists::get_credited_many_by_owner(db, &release_ids)?,
                    ArtistCreditSource::Release,
                );
                &fetched_artists_by_release
            }
        };

    for track_id in unresolved {
        let mut artists = artists_by_track.remove(&track_id).unwrap_or_default();
        if artists.is_empty()
            && let Some(releases) = releases_by_track.get(&track_id)
        {
            for release in releases {
                let Some(release_id) = release.db_id.clone().map(Into::<DbId>::into) else {
                    continue;
                };
                if let Some(release_artists) = artists_by_release.get(&release_id) {
                    artists.extend(release_artists.clone());
                }
            }
        }
        artists_by_track.insert(track_id, dedupe_credited_artists(artists));
    }

    for track_id in track_ids {
        artists_by_track.entry(track_id).or_default();
    }

    Ok(artists_by_track)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;
    use crate::services::EntityType;
    use nanoid::nanoid;

    use agdb::QueryBuilder;

    #[test]
    fn build_entity_provider_context_detects_release() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = Release {
            db_id: None,
            id: nanoid!(),
            release_title: "Test Release".to_string(),
            sort_title: None,
            release_type: None,
            release_date: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let release_id = db
            .exec_mut(QueryBuilder::insert().element(&release).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_id)
                .query(),
        )?;

        let (entity_type, _) = build_entity_provider_context(&db, release_id, None)?;
        assert_eq!(entity_type, EntityType::Release);

        Ok(())
    }

    #[test]
    fn build_entity_provider_context_detects_track() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track = Track {
            db_id: None,
            id: nanoid!(),
            track_title: "Test Track".to_string(),
            sort_title: None,
            year: None,
            disc: None,
            disc_total: None,
            track: None,
            track_total: None,
            duration_ms: None,
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track_id = db
            .exec_mut(QueryBuilder::insert().element(&track).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_id)
                .query(),
        )?;

        let (entity_type, _) = build_entity_provider_context(&db, track_id, None)?;
        assert_eq!(entity_type, EntityType::Track);

        Ok(())
    }

    #[test]
    fn build_entity_provider_context_detects_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist = Artist {
            db_id: None,
            id: nanoid!(),
            artist_name: "Test Artist".to_string(),
            scan_name: "test artist".to_string(),
            sort_name: None,
            artist_type: None,
            description: None,
            verified: false,
            locked: None,
            created_at: None,
        };
        let artist_id = db
            .exec_mut(QueryBuilder::insert().element(&artist).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("artists")
                .to(artist_id)
                .query(),
        )?;

        let (entity_type, _) = build_entity_provider_context(&db, artist_id, None)?;
        assert_eq!(entity_type, EntityType::Artist);

        Ok(())
    }

    #[test]
    fn build_entity_provider_context_errors_for_unknown_entity() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = build_entity_provider_context(&db, DbId(999999), None);
        assert!(result.is_err());

        Ok(())
    }
}

#[cfg(test)]
mod benches {
    extern crate test;

    use test::Bencher;

    use super::*;
    use crate::db::test_db::{
        connect,
        connect_artist,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };

    #[bench]
    fn resolve_track_credited_direct(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        let track_id = insert_track(&mut db, "Track").unwrap();
        let artist_id = insert_artist(&mut db, "Artist").unwrap();
        connect(&mut db, release_id, track_id).unwrap();
        connect_artist(&mut db, track_id, artist_id).unwrap();

        b.iter(|| resolve_track_credited_artists(&db, track_id).unwrap());
    }

    #[bench]
    fn resolve_track_credited_release_fallback(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        let track_id = insert_track(&mut db, "Track").unwrap();
        let artist_id = insert_artist(&mut db, "Release Artist").unwrap();
        connect(&mut db, release_id, track_id).unwrap();
        connect_artist(&mut db, release_id, artist_id).unwrap();

        b.iter(|| resolve_track_credited_artists(&db, track_id).unwrap());
    }

    #[bench]
    fn resolve_track_credited_with_context_20_tracks(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        let release_artist = insert_artist(&mut db, "Release Artist").unwrap();
        connect_artist(&mut db, release_id, release_artist).unwrap();

        let mut track_ids = Vec::new();
        for i in 0..20 {
            let track_id = insert_track(&mut db, &format!("Track {i}")).unwrap();
            connect(&mut db, release_id, track_id).unwrap();
            if i % 2 == 0 {
                let a = insert_artist(&mut db, &format!("Track Artist {i}")).unwrap();
                connect_artist(&mut db, track_id, a).unwrap();
            }
            track_ids.push(track_id);
        }

        b.iter(|| {
            let ctx = TrackCreditedArtistContext {
                releases_by_track: None,
                credited_artists_by_release: None,
                scope_release_id: Some(release_id),
            };
            resolve_track_credited_artists_with_context(&db, &track_ids, &ctx).unwrap()
        });
    }
}
