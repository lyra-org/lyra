// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use super::{
    Candidates,
    CatalogError,
    CreditRole,
    Direction,
    SortSpec,
    Viewer,
    all_releases,
    credited_owners,
    pipeline::{
        Catalog,
        KeyKind,
        Row,
        SortKey,
        SortValue,
    },
    scoped_releases,
    track_totals,
    tracks_of_releases,
};
use crate::db::{
    self,
    Artist,
    ArtistType,
    CreditType,
    Release,
    Track,
    favorites::FavoriteKind,
    ratings::RatingFilter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtistKey {
    Name,
    SortName,
    DateCreated,
    /// The visible releases credited to the artist.
    ReleaseCount,
    /// The visible tracks credited to the artist or on its releases.
    TrackCount,
    TotalDuration,
    ListenCount,
    LastPlayedAt,
    Relevance,
    Random,
    Id,
}

impl SortKey for ArtistKey {
    const KEYS: &'static [(&'static str, Self)] = &[
        ("name", Self::Name),
        ("sort_name", Self::SortName),
        ("date_created", Self::DateCreated),
        ("release_count", Self::ReleaseCount),
        ("track_count", Self::TrackCount),
        ("total_duration", Self::TotalDuration),
        ("listen_count", Self::ListenCount),
        ("last_played_at", Self::LastPlayedAt),
        ("relevance", Self::Relevance),
        ("random", Self::Random),
        ("id", Self::Id),
    ];
    const RELEVANCE: Self = Self::Relevance;

    fn kind(self) -> KeyKind {
        match self {
            Self::Relevance => KeyKind::Relevance,
            Self::Random => KeyKind::Random,
            Self::Id => KeyKind::Id,
            _ => KeyKind::Field,
        }
    }
}

/// Artists are found through the credits on releases and tracks. Every filter below except
/// `ids`, `exclude_ids`, `artist_type`, `favorite`, `listened` and `rating` narrows the credits
/// an artist must hold.
#[derive(Clone, Debug, Default)]
pub(crate) struct ArtistFilter {
    pub(crate) ids: Option<Vec<DbId>>,
    pub(crate) exclude_ids: Vec<DbId>,
    pub(crate) library: Option<DbId>,
    /// Credits on these releases or their tracks.
    pub(crate) releases: Option<Vec<DbId>>,
    /// Credits on these tracks, or on their releases for tracks without credits of their own.
    pub(crate) tracks: Option<Vec<DbId>>,
    /// Credits on releases in any of these genres, or on their tracks.
    pub(crate) genres: Vec<DbId>,
    pub(crate) credit_role: Option<CreditRole>,
    pub(crate) exclude_credit_role: Option<CreditRole>,
    pub(crate) credit_types: Option<Vec<CreditType>>,
    pub(crate) exclude_credit_types: Vec<CreditType>,
    pub(crate) artist_type: Option<ArtistType>,
    pub(crate) favorite: Option<bool>,
    /// Artists with at least one listened track among their credited tracks or releases.
    pub(crate) listened: Option<bool>,
    pub(crate) rating: RatingFilter,
}

impl ArtistFilter {
    fn narrows_credits(&self) -> bool {
        self.library.is_some()
            || self.releases.is_some()
            || self.tracks.is_some()
            || !self.genres.is_empty()
            || self.credit_role.is_some_and(|role| role != CreditRole::Any)
            || self.exclude_credit_role.is_some()
            || self.credit_types.is_some()
            || !self.exclude_credit_types.is_empty()
    }
}

pub(crate) struct Artists;

impl Catalog for Artists {
    type Key = ArtistKey;
    type Filter = ArtistFilter;
    type Item = Artist;

    fn id_filter(ids: Vec<DbId>) -> ArtistFilter {
        ArtistFilter {
            ids: Some(ids),
            ..ArtistFilter::default()
        }
    }

    fn default_sort(_filter: &ArtistFilter) -> SortSpec<ArtistKey> {
        vec![(ArtistKey::SortName, Direction::Ascending)]
    }

    fn candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &ArtistFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let mut artists = Candidates::default();
        if let Some(ids) = &filter.ids {
            let ids = db::graph::existing_ids(db, ids, "Artist")?;
            match &viewer.visible_releases(db)? {
                Some(visible) => {
                    let mut credited_visibly = Vec::new();
                    for artist in ids {
                        if credited_on_visible(db, artist, visible)? {
                            credited_visibly.push(artist);
                        }
                    }
                    artists.restrict(credited_visibly);
                }
                None => artists.restrict(ids),
            }
        }
        if filter.narrows_credits() || (!viewer.sees_everything() && filter.ids.is_none()) {
            artists.restrict(credited_artists(db, viewer, filter)?);
        }
        if let Some(artist_type) = filter.artist_type {
            artists.restrict(db::artists::ids_with_type(db, artist_type)?);
        }
        if let Some(favorite) = filter.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = db::favorites::target_ids(db, user, FavoriteKind::Artist)?;
            artists.restrict_membership(favorites, favorite, || all_artists(db))?;
        }
        if let Some(listened) = filter.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let tracks = db::listens::listened_track_ids(db, user)?;
            let mut listened_artists = HashSet::new();
            for track in &tracks {
                listened_artists.extend(credited_artist_ids(db, *track)?);
            }
            for release in super::releases_of_tracks(db, tracks)? {
                listened_artists.extend(credited_artist_ids(db, release)?);
            }
            artists.restrict_membership(listened_artists, listened, || all_artists(db))?;
        }
        if !filter.rating.is_empty() {
            let user = viewer.user_db_id("the rating filter")?;
            artists.restrict(db::ratings::target_ids_matching(db, user, filter.rating)?);
        }

        Ok(artists.resolve(&filter.exclude_ids, || all_artists(db))?)
    }

    fn rows(
        db: &DbAny,
        viewer: &Viewer,
        _filter: &ArtistFilter,
        ids: Vec<DbId>,
        keys: &[ArtistKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = db::graph::select_fields(db, &ids, &artist_fields(keys))?;
        let needs = |wanted: &[ArtistKey]| keys.iter().any(|key| wanted.contains(key));
        let visible = if needs(&[
            ArtistKey::ReleaseCount,
            ArtistKey::TrackCount,
            ArtistKey::TotalDuration,
            ArtistKey::ListenCount,
            ArtistKey::LastPlayedAt,
        ]) {
            viewer.visible_releases(db)?
        } else {
            None
        };
        let visible_tracks = match &visible {
            Some(releases) => Some(tracks_of_releases(db, releases.iter().copied())?),
            None => None,
        };

        let mut release_counts = HashMap::new();
        let mut tracks_by_artist = HashMap::new();
        if needs(&[
            ArtistKey::ReleaseCount,
            ArtistKey::TrackCount,
            ArtistKey::TotalDuration,
            ArtistKey::ListenCount,
            ArtistKey::LastPlayedAt,
        ]) {
            for id in &ids {
                let mut releases = credited_owners::<Release>(db, &[*id])?;
                if let Some(visible) = &visible {
                    releases.retain(|release| visible.contains(release));
                }
                let mut tracks = credited_owners::<Track>(db, &[*id])?;
                tracks.extend(tracks_of_releases(db, releases.iter().copied())?);
                if let Some(visible_tracks) = &visible_tracks {
                    tracks.retain(|track| visible_tracks.contains(track));
                }
                release_counts.insert(*id, releases.len() as u64);
                tracks_by_artist.insert(*id, tracks);
            }
        }
        let totals = track_totals(
            db,
            viewer,
            &tracks_by_artist,
            needs(&[ArtistKey::TotalDuration]),
            needs(&[ArtistKey::ListenCount, ArtistKey::LastPlayedAt]),
        )?;

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let artist = fields.get(&id)?;
                let name = artist.text("artist_name")?;
                let sort_name = artist.text("sort_name").unwrap_or(name).to_lowercase();
                let totals = totals.get(&id).copied().unwrap_or_default();
                let values = keys
                    .iter()
                    .map(|key| match key {
                        ArtistKey::Name => Some(SortValue::Text(name.to_lowercase())),
                        ArtistKey::SortName => Some(SortValue::Text(sort_name.clone())),
                        ArtistKey::DateCreated => artist
                            .number("ctime")
                            .or_else(|| artist.number("created_at"))
                            .map(SortValue::Number),
                        ArtistKey::ReleaseCount => Some(SortValue::Number(
                            release_counts.get(&id).copied().unwrap_or(0),
                        )),
                        ArtistKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        ArtistKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        ArtistKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        ArtistKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        ArtistKey::Relevance | ArtistKey::Random | ArtistKey::Id => None,
                    })
                    .collect();
                Some(Row {
                    id,
                    name: name.to_string(),
                    sort_name,
                    values,
                })
            })
            .collect())
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<Artist>> {
        let mut artists = db::graph::bulk_fetch_typed(db, ids.to_vec(), "Artist")?;
        Ok(ids.iter().filter_map(|id| artists.remove(id)).collect())
    }
}

/// The artists holding a credit the filter allows on an owner the viewer can see.
fn credited_artists(
    db: &DbAny,
    viewer: &Viewer,
    filter: &ArtistFilter,
) -> anyhow::Result<HashSet<DbId>> {
    let releases = scoped_releases(
        db,
        viewer,
        filter.library,
        filter.releases.as_deref(),
        &filter.genres,
    )?;
    let releases = releases.resolve(&[], || all_releases(db))?;
    let mut tracks = Candidates::default();
    tracks.restrict(tracks_of_releases(db, releases.iter().copied())?);
    if let Some(ids) = &filter.tracks {
        tracks.restrict(db::graph::existing_ids(db, ids, "Track")?);
    }
    let tracks = tracks.resolve(&[], || Ok(Vec::new()))?;

    let allowed = |credit_type: CreditType| {
        filter
            .credit_types
            .as_ref()
            .is_none_or(|types| types.contains(&credit_type))
            && !filter.exclude_credit_types.contains(&credit_type)
    };
    let credited = |owners: &[DbId]| -> anyhow::Result<(HashSet<DbId>, Vec<DbId>)> {
        let mut artists = HashSet::new();
        let mut uncredited = Vec::new();
        for owner in owners {
            let links = db::credits::links_for_owner(db, *owner)?;
            if links.is_empty() {
                uncredited.push(*owner);
            }
            artists.extend(
                links
                    .into_iter()
                    .filter(|link| allowed(link.credit.credit_type))
                    .map(|link| link.artist_id),
            );
        }
        Ok((artists, uncredited))
    };
    let (by_track, uncredited_tracks) = credited(&tracks)?;
    // A track without credits of its own takes its release's, so a track scope reads the
    // credits of the releases those tracks sit on.
    let releases = if filter.tracks.is_some() {
        let visible_releases = releases.into_iter().collect::<HashSet<_>>();
        super::releases_of_tracks(db, uncredited_tracks)?
            .into_iter()
            .filter(|release| visible_releases.contains(release))
            .collect()
    } else {
        releases
    };
    let (by_release, _) = credited(&releases)?;
    let in_role = |role| match role {
        CreditRole::Track => by_track.clone(),
        CreditRole::Release => by_release.clone(),
        CreditRole::Any => by_track.union(&by_release).copied().collect(),
    };
    let mut artists = in_role(filter.credit_role.unwrap_or(CreditRole::Any));
    if let Some(excluding) = filter.exclude_credit_role {
        let excluded = in_role(excluding);
        artists.retain(|artist| !excluded.contains(artist));
    }
    Ok(artists)
}

/// Whether the artist holds a credit on a visible release or on a track of one.
fn credited_on_visible(db: &DbAny, artist: DbId, visible: &HashSet<DbId>) -> anyhow::Result<bool> {
    for owner in db::credits::crediting_owner_ids(db, artist)? {
        if visible.contains(&owner)
            || db::graph::inbound_neighbor_ids(db, owner, "Release")?
                .iter()
                .any(|release| visible.contains(release))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn credited_artist_ids(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<DbId>> {
    Ok(db::credits::links_for_owner(db, owner)?
        .into_iter()
        .map(|link| link.artist_id)
        .collect())
}

/// The stored artist fields that the names and `keys` read.
fn artist_fields(keys: &[ArtistKey]) -> Vec<&'static str> {
    let mut fields = vec!["artist_name", "sort_name"];
    if keys.contains(&ArtistKey::DateCreated) {
        fields.extend(["ctime", "created_at"]);
    }
    fields
}

fn all_artists(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    db::graph::neighbor_ids(db, "artists", "Artist")
}
