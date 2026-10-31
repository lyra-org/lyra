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
    QueryBuilder,
};

use super::{
    Candidates,
    CatalogError,
    CreditRole,
    Direction,
    PersonalData,
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
use lyra_db::{
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
    /// The viewer's rating.
    Rating,
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
        ("rating", Self::Rating),
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

    fn personal(self) -> Option<PersonalData> {
        match self {
            Self::ListenCount | Self::LastPlayedAt => Some(PersonalData::Listens),
            Self::Rating => Some(PersonalData::Ratings),
            _ => None,
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
    /// Whether any credit on a scoped release or its tracks counts, whatever its role or type.
    fn credits_anywhere(&self) -> bool {
        self.tracks.is_none()
            && self.credit_role.is_none_or(|role| role == CreditRole::Any)
            && self.exclude_credit_role.is_none()
            && self.credit_types.is_none()
            && self.exclude_credit_types.is_empty()
    }

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

    fn personal(filter: &ArtistFilter) -> Vec<PersonalData> {
        PersonalData::of_filter(filter.favorite, filter.listened, filter.rating)
    }

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
            let ids = lyra_db::graph::existing_ids(db, ids, "Artist")?;
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
            artists.restrict(match filter.ids {
                Some(_) => lyra_db::artists::ids_with_type(db, artist_type)?,
                None => lyra_db::artists::credited_ids(db, Some(artist_type))?,
            });
        }
        if let Some(favorite) = filter.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = lyra_db::favorites::target_ids(db, user, FavoriteKind::Artist)?;
            artists.restrict_membership(favorites, favorite, || all_artists(db))?;
        }
        if let Some(listened) = filter.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let tracks = lyra_db::listens::listened_track_ids(db, user)?;
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
            artists.restrict(lyra_db::ratings::target_ids_matching(
                db,
                user,
                filter.rating,
            )?);
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
        let fields = lyra_db::graph::select_fields(db, &ids, &artist_fields(keys))?;
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
        let ratings = super::ratings(db, viewer, &ids, needs(&[ArtistKey::Rating]))?;
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
                        ArtistKey::Rating => ratings
                            .get(&id)
                            .map(|rating| SortValue::Number((*rating).into())),
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
        let mut artists = lyra_db::graph::bulk_fetch_typed(db, ids.to_vec(), "Artist")?;
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
    if filter.credits_anywhere() {
        return credited_anywhere(db, releases);
    }
    let mut tracks = Candidates::default();
    tracks.restrict(tracks_of_releases(db, releases.iter().copied())?);
    if let Some(ids) = &filter.tracks {
        tracks.restrict(lyra_db::graph::existing_ids(db, ids, "Track")?);
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
            let links = lyra_db::credits::links_for_owner(db, *owner)?;
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

/// The artists credited on `releases` or on their tracks, in any role.
///
/// Reads whichever side has fewer edges: each credited artist's credits, or every release and
/// track the releases hold.
fn credited_anywhere(db: &DbAny, releases: Vec<DbId>) -> anyhow::Result<HashSet<DbId>> {
    if releases.is_empty() {
        return Ok(HashSet::new());
    }
    let artists = all_artists(db)?;
    if reads_by_artist(db, &artists, &releases)? {
        credited_by_artist(db, artists, &releases.into_iter().collect())
    } else {
        credited_by_owner(db, releases)
    }
}

/// Whether the artists' incoming edges, which hold their credits, are fewer than the releases'
/// outgoing ones, which lead to their tracks and credits.
fn reads_by_artist(db: &DbAny, artists: &[DbId], releases: &[DbId]) -> anyhow::Result<bool> {
    let artist_edges = db
        .exec(QueryBuilder::select().edge_count_to().ids(artists).query())?
        .result;
    let release_edges = db
        .exec(
            QueryBuilder::select()
                .edge_count_from()
                .ids(releases)
                .query(),
        )?
        .result;
    Ok(artist_edges < release_edges)
}

/// The `artists` credited on `releases` or on their tracks, read from each artist's credits.
fn credited_by_artist(
    db: &DbAny,
    artists: Vec<DbId>,
    releases: &HashSet<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut credited = HashSet::new();
    for artist in artists {
        if credited_on_visible(db, artist, releases)? {
            credited.insert(artist);
        }
    }
    Ok(credited)
}

/// The artists credited on `releases` or on their tracks, read from each release and track.
fn credited_by_owner(db: &DbAny, releases: Vec<DbId>) -> anyhow::Result<HashSet<DbId>> {
    let mut credited = HashSet::new();
    for owner in tracks_of_releases(db, releases.iter().copied())?
        .into_iter()
        .chain(releases)
    {
        credited.extend(credited_artist_ids(db, owner)?);
    }
    Ok(credited)
}

/// Whether the artist holds a credit on a visible release or on a track of one.
fn credited_on_visible(db: &DbAny, artist: DbId, visible: &HashSet<DbId>) -> anyhow::Result<bool> {
    for owner in lyra_db::credits::crediting_owner_ids(db, artist)? {
        if visible.contains(&owner)
            || lyra_db::graph::inbound_neighbor_ids(db, owner, "Release")?
                .iter()
                .any(|release| visible.contains(release))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn credited_artist_ids(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<DbId>> {
    lyra_db::credits::credited_artist_ids(db, &[owner])
}

/// The stored artist fields that the names and `keys` read.
fn artist_fields(keys: &[ArtistKey]) -> Vec<&'static str> {
    let mut fields = vec!["artist_name", "sort_name"];
    if keys.contains(&ArtistKey::DateCreated) {
        fields.extend(["ctime", "created_at"]);
    }
    fields
}

/// The artists browsing reaches: those a release or track credits.
fn all_artists(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    lyra_db::artists::credited_ids(db, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        auth::Principal,
        catalog::{
            Query,
            order,
        },
    };
    use lyra_db::{
        fixtures::{
            connect,
            insert_artist,
            insert_release,
            insert_track,
        },
        test_db::{
            connect_artist,
            insert_library,
            insert_user,
            new_test_db,
        },
    };

    struct Credits {
        db: DbAny,
        viewer: Viewer,
        visible_releases: Vec<DbId>,
        visible_artists: HashSet<DbId>,
    }

    /// Artists credited in and out of the viewer's one library, with `padding` more tracks on the
    /// visible release when `pad_releases` and more credits for a hidden artist otherwise.
    fn credits(pad_releases: bool, padding: usize) -> anyhow::Result<Credits> {
        let mut db = new_test_db()?;
        let visible = insert_library(&mut db, "Visible", "/credits/visible")?;
        let hidden = insert_library(&mut db, "Hidden", "/credits/hidden")?;
        let release = |db: &mut DbAny, title: &str, libraries: &[DbId]| -> anyhow::Result<DbId> {
            let release = insert_release(db, title)?;
            for library in libraries {
                connect(db, *library, release)?;
            }
            Ok(release)
        };
        let track = |db: &mut DbAny, title: &str, releases: &[DbId]| -> anyhow::Result<DbId> {
            let track = insert_track(db, title)?;
            for release in releases {
                connect(db, *release, track)?;
            }
            Ok(track)
        };
        let shown = release(&mut db, "Shown", &[visible])?;
        let unseen = release(&mut db, "Unseen", &[hidden])?;
        let both = release(&mut db, "Both", &[visible, hidden])?;
        let shown_track = track(&mut db, "Shown Track", &[shown])?;
        let unseen_track = track(&mut db, "Unseen Track", &[unseen])?;
        let shared_track = track(&mut db, "Shared Track", &[unseen, shown])?;
        let loose_track = track(&mut db, "Loose Track", &[])?;

        let mut visible_artists = HashSet::new();
        for (name, owners, seen) in [
            ("On Shown", vec![shown], true),
            ("On Shown Track", vec![shown_track], true),
            ("On Shared Track", vec![shared_track], true),
            ("On Both", vec![both], true),
            ("On Unseen And Shown Track", vec![unseen, shown_track], true),
            ("On Unseen", vec![unseen], false),
            ("On Unseen Track", vec![unseen_track], false),
            ("On Loose Track", vec![loose_track], false),
            ("Uncredited", vec![], false),
        ] {
            let artist = insert_artist(&mut db, name)?;
            for owner in owners {
                connect_artist(&mut db, owner, artist)?;
            }
            if seen {
                visible_artists.insert(artist);
            }
        }
        let busy = insert_artist(&mut db, "Busy")?;
        for index in 0..padding {
            if pad_releases {
                track(&mut db, &format!("Padding {index}"), &[shown])?;
            } else {
                let padding = track(&mut db, &format!("Padding {index}"), &[unseen])?;
                connect_artist(&mut db, padding, busy)?;
            }
        }

        let user = insert_user(&mut db, "listener")?;
        let library_id = lyra_db::libraries::get_by_id(&db, visible)?
            .expect("library exists")
            .id;
        let principal = Principal::for_user(&db, user, Vec::new(), HashSet::from([library_id]));
        let viewer = Viewer::user(&db, principal)?;
        Ok(Credits {
            db,
            viewer,
            visible_releases: vec![shown, both],
            visible_artists,
        })
    }

    #[test]
    fn both_credit_reads_find_the_artists_credited_on_visible_releases_and_their_tracks()
    -> anyhow::Result<()> {
        let credits = credits(true, 0)?;
        let db = &credits.db;
        let by_artist = credited_by_artist(
            db,
            all_artists(db)?,
            &credits.visible_releases.iter().copied().collect(),
        )?;
        let by_owner = credited_by_owner(db, credits.visible_releases.clone())?;
        assert_eq!(by_artist, credits.visible_artists);
        assert_eq!(by_owner, credits.visible_artists);
        Ok(())
    }

    #[test]
    fn a_restricted_viewer_browses_the_same_artists_whichever_side_is_read() -> anyhow::Result<()> {
        for pad_releases in [true, false] {
            let credits = credits(pad_releases, 40)?;
            let db = &credits.db;
            assert_eq!(
                reads_by_artist(db, &all_artists(db)?, &credits.visible_releases)?,
                pad_releases
            );
            let browsed = order(
                db,
                &credits.viewer,
                &Query::<Artists>::new(Default::default()),
            )?;
            assert_eq!(browsed.len(), credits.visible_artists.len());
            assert_eq!(
                browsed.into_iter().collect::<HashSet<_>>(),
                credits.visible_artists
            );
        }
        Ok(())
    }
}
