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
    PersonalFilter,
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
    /// When the viewer last set their rating.
    RatedAt,
    /// When the viewer first favorited the artist.
    FavoritedAt,
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
        ("rated_at", Self::RatedAt),
        ("favorited_at", Self::FavoritedAt),
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
            Self::Rating | Self::RatedAt => Some(PersonalData::Ratings),
            Self::FavoritedAt => Some(PersonalData::Favorites),
            _ => None,
        }
    }
}

/// Artists are found through the credits on releases and tracks. Every filter below except
/// `ids`, `exclude_ids`, `artist_type` and `personal` narrows the credits
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
    /// `listened` keeps artists with at least one listened track among their credited tracks or
    /// releases.
    pub(crate) personal: PersonalFilter,
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
        filter.personal.reads()
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
        filter.personal.restrict(
            db,
            viewer,
            &mut artists,
            FavoriteKind::Artist,
            |user| {
                let tracks = lyra_db::listens::listened_track_ids(db, user)?;
                let mut listened = HashSet::new();
                for track in &tracks {
                    listened.extend(credited_artist_ids(db, *track)?);
                }
                for release in super::releases_of_tracks(db, tracks)? {
                    listened.extend(credited_artist_ids(db, release)?);
                }
                Ok(listened)
            },
            || all_artists(db),
        )?;

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
        let ratings = super::ratings(
            db,
            viewer,
            &ids,
            needs(&[ArtistKey::Rating, ArtistKey::RatedAt]),
        )?;
        let favorited_at = super::favorited_at(
            db,
            viewer,
            &ids,
            FavoriteKind::Artist,
            needs(&[ArtistKey::FavoritedAt]),
        )?;
        let totals = track_totals(
            db,
            viewer,
            &tracks_by_artist,
            needs(&[ArtistKey::TotalDuration]),
            needs(&[ArtistKey::ListenCount, ArtistKey::LastPlayedAt]),
        )?;

        Ok(fields
            .into_iter()
            .filter_map(|(id, mut artist)| {
                let name = artist.take_text("artist_name")?;
                let mut row = Row::new(id, name, artist.text("sort_name"));
                let totals = totals.get(&id).copied().unwrap_or_default();
                row.values = keys
                    .iter()
                    .map(|key| match key {
                        ArtistKey::Name => Some(SortValue::Text(row.lower_name().to_string())),
                        ArtistKey::SortName => Some(SortValue::Text(row.sort_name().to_string())),
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
                            .map(|edge| SortValue::Number(edge.value.get().into())),
                        ArtistKey::RatedAt => ratings
                            .get(&id)
                            .and_then(|edge| super::timestamp_value(edge.updated_at_ms)),
                        ArtistKey::FavoritedAt => favorited_at
                            .get(&id)
                            .and_then(|ms| super::timestamp_value(*ms)),
                        ArtistKey::Relevance | ArtistKey::Random | ArtistKey::Id => None,
                    })
                    .collect();
                Some(row)
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

/// The browsable artists credited on `releases` or on their tracks, in any role.
///
/// Reads whichever side has fewer edges: each browsable artist's credits, or every release and
/// track the releases hold. Both sides keep the same artists, so the choice changes only the cost.
fn credited_anywhere(db: &DbAny, releases: Vec<DbId>) -> anyhow::Result<HashSet<DbId>> {
    if releases.is_empty() {
        return Ok(HashSet::new());
    }
    let artists = all_artists(db)?;
    if reads_by_artist(db, &artists, &releases)? {
        credited_by_artist(db, artists, &releases.into_iter().collect())
    } else {
        credited_by_owner(db, &artists.into_iter().collect(), releases)
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
    'artists: for artist in artists {
        if lyra_db::credits::crediting_owner_ids(db, artist)?
            .iter()
            .any(|owner| releases.contains(owner))
        {
            credited.insert(artist);
            continue;
        }
        for track in credited_owners::<Track>(db, &[artist])? {
            if lyra_db::graph::inbound_neighbor_ids(db, track, "Release")?
                .iter()
                .any(|release| releases.contains(release))
            {
                credited.insert(artist);
                continue 'artists;
            }
        }
    }
    Ok(credited)
}

/// The `artists` credited on `releases` or on their tracks, read from each release and track.
fn credited_by_owner(
    db: &DbAny,
    artists: &HashSet<DbId>,
    releases: Vec<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut credited = HashSet::new();
    for owner in tracks_of_releases(db, releases.iter().copied())?
        .into_iter()
        .chain(releases)
    {
        credited.extend(
            credited_artist_ids(db, owner)?
                .into_iter()
                .filter(|artist| artists.contains(artist)),
        );
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
            page,
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
        user: DbId,
        visible_library: DbId,
        visible_library_id: String,
        visible_releases: Vec<DbId>,
        rock: DbId,
        names: HashMap<DbId, &'static str>,
        /// The artists credited on the visible releases or their tracks.
        visible_artists: HashSet<DbId>,
        /// The artists credited on the rock release or its tracks.
        rock_artists: HashSet<DbId>,
    }

    impl Credits {
        fn viewer(&self, admin: bool) -> anyhow::Result<Viewer> {
            let (permissions, libraries) = if admin {
                (vec![lyra_db::Permission::Admin], HashSet::new())
            } else {
                (Vec::new(), HashSet::from([self.visible_library_id.clone()]))
            };
            let principal = Principal::for_user(&self.db, self.user, permissions, libraries);
            Ok(Viewer::user(&self.db, principal)?)
        }

        /// `artists` ordered by name, as browsing orders them.
        fn by_name(&self, artists: &HashSet<DbId>) -> Vec<DbId> {
            let mut ordered = artists.iter().copied().collect::<Vec<_>>();
            ordered.sort_by_key(|artist| self.names[artist].to_lowercase());
            ordered
        }
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
        lyra_db::genres::sync_release_genres(&mut db, shown, &["Rock".to_string()])?;
        let rock = lyra_db::genres::find_by_name(&db, "Rock")?.expect("genre exists");

        let mut names = HashMap::new();
        let mut visible_artists = HashSet::new();
        let mut rock_artists = HashSet::new();
        for (name, owners, seen, on_rock) in [
            ("On Shown", vec![shown], true, true),
            ("On Shown Track", vec![shown_track], true, true),
            ("On Shared Track", vec![shared_track], true, true),
            ("On Both", vec![both], true, false),
            (
                "On Unseen And Shown Track",
                vec![unseen, shown_track],
                true,
                true,
            ),
            ("On Unseen", vec![unseen], false, false),
            ("On Unseen Track", vec![unseen_track], false, false),
            ("On Loose Track", vec![loose_track], false, false),
            ("Uncredited", vec![], false, false),
        ] {
            let artist = insert_artist(&mut db, name)?;
            for owner in owners {
                connect_artist(&mut db, owner, artist)?;
            }
            names.insert(artist, name);
            if seen {
                visible_artists.insert(artist);
            }
            if on_rock {
                rock_artists.insert(artist);
            }
        }
        let busy = insert_artist(&mut db, "Busy")?;
        names.insert(busy, "Busy");
        for index in 0..padding {
            if pad_releases {
                track(&mut db, &format!("Padding {index}"), &[shown])?;
            } else {
                let padding = track(&mut db, &format!("Padding {index}"), &[unseen])?;
                connect_artist(&mut db, padding, busy)?;
            }
        }

        let user = insert_user(&mut db, "listener")?;
        let visible_library_id = lyra_db::libraries::get_by_id(&db, visible)?
            .expect("library exists")
            .id;
        Ok(Credits {
            db,
            user,
            visible_library: visible,
            visible_library_id,
            visible_releases: vec![shown, both],
            rock,
            names,
            visible_artists,
            rock_artists,
        })
    }

    fn by_artist(credits: &Credits) -> anyhow::Result<HashSet<DbId>> {
        let db = &credits.db;
        credited_by_artist(
            db,
            all_artists(db)?,
            &credits.visible_releases.iter().copied().collect(),
        )
    }

    fn by_owner(credits: &Credits) -> anyhow::Result<HashSet<DbId>> {
        let db = &credits.db;
        credited_by_owner(
            db,
            &all_artists(db)?.into_iter().collect(),
            credits.visible_releases.clone(),
        )
    }

    fn set_credited(db: &mut DbAny, artist: DbId, credited: bool) -> anyhow::Result<()> {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("credited", credited).into()])
                .ids(artist)
                .query(),
        )?;
        Ok(())
    }

    #[test]
    fn both_credit_reads_find_the_artists_credited_on_visible_releases_and_their_tracks()
    -> anyhow::Result<()> {
        let credits = credits(true, 0)?;
        assert_eq!(by_artist(&credits)?, credits.visible_artists);
        assert_eq!(by_owner(&credits)?, credits.visible_artists);
        Ok(())
    }

    #[test]
    fn both_credit_reads_keep_only_artists_flagged_credited() -> anyhow::Result<()> {
        let mut credits = credits(true, 0)?;
        let name = |credits: &Credits, wanted: &str| {
            *credits
                .names
                .iter()
                .find(|(_, name)| **name == wanted)
                .expect("artist exists")
                .0
        };
        let unflagged = name(&credits, "On Shown");
        let flagged = name(&credits, "Uncredited");
        set_credited(&mut credits.db, unflagged, false)?;
        set_credited(&mut credits.db, flagged, true)?;
        credits.visible_artists.remove(&unflagged);

        assert_eq!(by_artist(&credits)?, credits.visible_artists);
        assert_eq!(by_owner(&credits)?, credits.visible_artists);
        Ok(())
    }

    #[test]
    fn a_restricted_viewer_browses_and_pages_the_same_artists_whichever_side_is_read()
    -> anyhow::Result<()> {
        for pad_releases in [true, false] {
            let credits = credits(pad_releases, 40)?;
            let db = &credits.db;
            assert_eq!(
                reads_by_artist(db, &all_artists(db)?, &credits.visible_releases)?,
                pad_releases
            );
            let viewer = credits.viewer(false)?;
            let expected = credits.by_name(&credits.visible_artists);
            for filter in [
                ArtistFilter::default(),
                ArtistFilter {
                    credit_role: Some(CreditRole::Any),
                    ..ArtistFilter::default()
                },
            ] {
                let query = Query::<Artists>::new(filter);
                assert_eq!(order(db, &viewer, &query)?, expected);
                let mut paged = Vec::new();
                for offset in (0..expected.len()).step_by(2) {
                    let page = page(db, &viewer, &query, offset as u64, Some(2))?;
                    assert_eq!(page.total, expected.len() as u64);
                    paged.extend(page.items.into_iter().filter_map(|artist| artist.db_id));
                }
                assert_eq!(
                    paged.into_iter().map(DbId::from).collect::<Vec<_>>(),
                    expected
                );
            }
        }
        Ok(())
    }

    #[test]
    fn an_admin_scoped_to_a_library_or_genre_browses_the_artists_credited_there()
    -> anyhow::Result<()> {
        for pad_releases in [true, false] {
            let credits = credits(pad_releases, 40)?;
            let db = &credits.db;
            let viewer = credits.viewer(true)?;
            for (filter, expected) in [
                (
                    ArtistFilter {
                        library: Some(credits.visible_library),
                        ..ArtistFilter::default()
                    },
                    &credits.visible_artists,
                ),
                (
                    ArtistFilter {
                        genres: vec![credits.rock],
                        ..ArtistFilter::default()
                    },
                    &credits.rock_artists,
                ),
            ] {
                let query = Query::<Artists>::new(filter);
                assert_eq!(order(db, &viewer, &query)?, credits.by_name(expected));
            }
        }
        Ok(())
    }
}
