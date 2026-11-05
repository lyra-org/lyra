// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbId,
};

use super::{
    CatalogError,
    CreditRole,
    Direction,
    PersonalData,
    PersonalFilter,
    SortSpec,
    favorited_at_column,
    index::{
        Scoped,
        Snapshot,
        Table,
        bits::Bits,
    },
    pipeline::{
        Catalog,
        Column,
        Context,
        KeyKind,
        SortKey,
    },
    ratings_column,
    scoped_releases,
    totals_column,
};
use lyra_db::{
    Artist,
    ArtistType,
    CreditType,
    favorites::FavoriteKind,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtistKey {
    Name,
    SortName,
    DateCreated,
    /// The visible releases credited to the artist, within the filter's release scope.
    ReleaseCount,
    /// The visible tracks credited to the artist or on its releases, within the filter's release
    /// scope.
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
    const NAME_ORDER: Self = Self::SortName;

    fn ranked(self) -> bool {
        matches!(self, Self::Name | Self::SortName)
    }
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
    /// Keeps only available entities (`true`) or only unavailable ones (`false`), narrowing
    /// what the query counts as a scope does.
    pub(crate) available: Option<bool>,
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
    pub(crate) fn credits_anywhere(&self) -> bool {
        self.tracks.is_none()
            && self.credit_role.is_none_or(|role| role == CreditRole::Any)
            && self.exclude_credit_role.is_none()
            && self.credit_types.is_none()
            && self.exclude_credit_types.is_empty()
    }

    pub(crate) fn narrows_credits(&self) -> bool {
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
    type Item = super::Listed<Artist>;

    fn personal(filter: &ArtistFilter) -> Vec<PersonalData> {
        filter.personal.reads()
    }

    fn available(filter: &ArtistFilter) -> Option<bool> {
        filter.available
    }

    fn available_mut(filter: &mut ArtistFilter) -> &mut Option<bool> {
        &mut filter.available
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

    fn table(index: &Snapshot) -> &Table {
        &index.artists
    }

    fn scoped(scope: &super::index::Scope) -> &Bits {
        &scope.artists
    }

    fn candidates(cx: &Context<'_>, filter: &ArtistFilter) -> Result<Bits, CatalogError> {
        let index = &cx.index;
        let len = index.artists.len();
        let mut artists = None::<Bits>;
        let mut restrict = |bits: Bits| match &mut artists {
            Some(artists) => artists.and(&bits),
            None => artists = Some(bits),
        };
        if let Some(ids) = &filter.ids {
            let ids = index.artists.slots(ids);
            restrict(if cx.scope.restricted {
                Bits::of(
                    len,
                    ids.iter().filter(|artist| credited_on_visible(cx, *artist)),
                )
            } else {
                ids
            });
        }
        if filter.narrows_credits() {
            restrict(credited_artists(cx, filter));
        } else if cx.scope.restricted && filter.ids.is_none() {
            restrict(cx.scope.artists.clone());
        }
        if let Some(artist_type) = filter.artist_type {
            let mut typed = Bits::of(
                len,
                (0..len as u32).filter(|artist| {
                    index.artist.artist_type[*artist as usize] == Some(artist_type)
                }),
            );
            if filter.ids.is_none() {
                typed.and(&index.artist.credited);
            }
            restrict(typed);
        }
        filter.personal.restrict(
            cx,
            &index.artists,
            &mut artists,
            &index.artist.credited,
            FavoriteKind::Artist,
            |listened| {
                let links = cx.links();
                let tracks = index.tracks.slots(listened);
                let mut listened = links.artists_of_tracks(&tracks);
                listened.or(&links.artists_of_releases(&links.releases_of_tracks(&tracks)));
                listened
            },
        )?;
        let mut artists = artists.unwrap_or_else(|| index.artist.credited.clone());
        artists.and_not(&index.artists.slots(&filter.exclude_ids));
        Ok(artists)
    }

    fn column<'a>(
        cx: &'a Context<'_>,
        filter: &ArtistFilter,
        key: ArtistKey,
        slots: &[u32],
    ) -> Result<Column<'a>, CatalogError> {
        let index = &*cx.index;
        let artists = &index.artists;
        let stored = |compute: &dyn Fn() -> Vec<u64>| Column::stored(&artists.cache, key, compute);
        let scope = counted_releases(cx, filter);
        let tracks_of = |artist| scoped_tracks(cx, scope.as_ref(), artist);
        Ok(match key {
            ArtistKey::Name => stored(&|| {
                artists
                    .name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            ArtistKey::SortName => stored(&|| {
                artists
                    .sort_name_rank
                    .iter()
                    .map(|rank| u64::from(*rank))
                    .collect()
            }),
            ArtistKey::DateCreated => Column::Fields(&index.artist.created),
            ArtistKey::ReleaseCount => Column::computed(artists.len(), slots, |artist| {
                Some(scoped_releases_of(&cx.links(), scope.as_ref(), artist).len() as u64)
            }),
            ArtistKey::TrackCount => {
                totals_column(cx, artists, slots, false, tracks_of, |totals| {
                    Some(totals.track_count)
                })?
            }
            ArtistKey::TotalDuration => {
                totals_column(cx, artists, slots, false, tracks_of, |totals| {
                    Some(totals.total_duration)
                })?
            }
            ArtistKey::ListenCount => {
                totals_column(cx, artists, slots, true, tracks_of, |totals| {
                    Some(totals.listen_count)
                })?
            }
            ArtistKey::LastPlayedAt => {
                totals_column(cx, artists, slots, true, tracks_of, |totals| {
                    totals.last_played_at
                })?
            }
            ArtistKey::Rating => ratings_column(cx, artists, slots, false)?,
            ArtistKey::RatedAt => ratings_column(cx, artists, slots, true)?,
            ArtistKey::FavoritedAt => {
                favorited_at_column(cx, artists, slots, FavoriteKind::Artist)?
            }
            ArtistKey::Relevance | ArtistKey::Random | ArtistKey::Id => {
                unreachable!("{key:?} is not a field key")
            }
        })
    }

    fn hydrate(db: &DbAny, ids: &[DbId]) -> anyhow::Result<Vec<super::Listed<Artist>>> {
        let artists = lyra_db::graph::bulk_fetch_typed(db, ids.to_vec(), "Artist")?;
        super::listed(db, ids, artists, super::index::Snapshot::artist_available)
    }
}

/// The releases the filter's scope filters keep counts and totals within, or `None` without any,
/// when they cover every release the viewer sees. Only `library`, `releases` and `genres` scope:
/// the credit filters and `tracks` choose which artists appear, as credits held there, and leave
/// what each artist's counts cover alone.
fn counted_releases(cx: &Context<'_>, filter: &ArtistFilter) -> Option<Bits> {
    if filter.library.is_none() && filter.releases.is_none() && filter.genres.is_empty() {
        return None;
    }
    scoped_releases(
        cx,
        filter.library,
        filter.releases.as_deref(),
        &filter.genres,
    )
}

/// The visible releases credited to `artist`, within `scope` when given.
fn scoped_releases_of(links: &Scoped<'_>, scope: Option<&Bits>, artist: u32) -> Vec<u32> {
    links
        .releases_of_artist(artist)
        .filter(|release| scope.is_none_or(|scope| scope.contains(*release)))
        .collect()
}

/// The visible tracks crediting `artist` or on the visible releases crediting it, keeping only
/// tracks on a release within `scope` when given.
fn scoped_tracks(cx: &Context<'_>, scope: Option<&Bits>, artist: u32) -> Vec<u32> {
    let links = cx.links();
    let credited = links.tracks_of_artist(artist).filter(|track| {
        scope.is_none_or(|scope| {
            links
                .releases_of_track(*track)
                .any(|release| scope.contains(release))
        })
    });
    let mut tracks = credited
        .chain(
            scoped_releases_of(&links, scope, artist)
                .into_iter()
                .flat_map(|release| links.tracks_of_release(release)),
        )
        .collect::<Vec<_>>();
    tracks.sort_unstable();
    tracks.dedup();
    tracks
}

/// Whether the artist holds a credit on a visible release or on a track of one.
fn credited_on_visible(cx: &Context<'_>, artist: u32) -> bool {
    let links = cx.links();
    links.releases_of_artist(artist).next().is_some()
        || links.tracks_of_artist(artist).next().is_some()
}

/// The artists holding a credit the filter allows on an owner the viewer can see.
fn credited_artists(cx: &Context<'_>, filter: &ArtistFilter) -> Bits {
    let index = &cx.index;
    let len = index.artists.len();
    let releases = scoped_releases(
        cx,
        filter.library,
        filter.releases.as_deref(),
        &filter.genres,
    )
    .unwrap_or_else(|| Bits::full(index.releases.len()));
    let links = cx.links();
    let mut tracks = links.tracks_of_releases(&releases);
    if filter.credits_anywhere() {
        let mut artists = links.artists_of_releases(&releases);
        artists.or(&links.artists_of_tracks(&tracks));
        artists.and(&index.artist.credited);
        return artists;
    }
    if let Some(ids) = &filter.tracks {
        tracks.and(&index.tracks.slots(ids));
    }

    let allowed = |credit_type: CreditType| {
        filter
            .credit_types
            .as_ref()
            .is_none_or(|types| types.contains(&credit_type))
            && !filter.exclude_credit_types.contains(&credit_type)
    };
    let mut by_track = Bits::empty(len);
    let mut uncredited_tracks = Vec::new();
    for track in tracks.iter() {
        let credits = links.track_credits(track);
        if credits.is_empty() {
            uncredited_tracks.push(track);
        }
        for credit in credits.iter().filter(|credit| allowed(credit.credit_type)) {
            by_track.insert(credit.artist);
        }
    }
    // A track without credits of its own takes its release's, so a track scope reads the
    // credits of the releases those tracks sit on.
    let releases = if filter.tracks.is_some() {
        let mut on = links.releases_of_tracks(&Bits::of(index.tracks.len(), uncredited_tracks));
        on.and(&releases);
        on
    } else {
        releases
    };
    let mut by_release = Bits::empty(len);
    for release in releases.iter() {
        for credit in links
            .release_credits(release)
            .iter()
            .filter(|credit| allowed(credit.credit_type))
        {
            by_release.insert(credit.artist);
        }
    }
    let in_role = |role| match role {
        CreditRole::Track => by_track.clone(),
        CreditRole::Release => by_release.clone(),
        CreditRole::Any => {
            let mut both = by_track.clone();
            both.or(&by_release);
            both
        }
    };
    let mut artists = in_role(filter.credit_role.unwrap_or(CreditRole::Any));
    if let Some(excluding) = filter.exclude_credit_role {
        artists.and_not(&in_role(excluding));
    }
    artists
}

#[cfg(test)]
mod tests {
    use std::collections::{
        HashMap,
        HashSet,
    };

    use agdb::QueryBuilder;

    use super::*;
    use crate::services::{
        auth::Principal,
        catalog::{
            Query,
            Viewer,
            counts,
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
            rock,
            names,
            visible_artists,
            rock_artists,
        })
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

    /// The artists a viewer browses with no filter.
    fn browsed(credits: &Credits, admin: bool) -> anyhow::Result<HashSet<DbId>> {
        let viewer = credits.viewer(admin)?;
        Ok(order(
            &credits.db,
            &viewer,
            &Query::<Artists>::new(ArtistFilter::default()),
        )?
        .into_iter()
        .collect())
    }

    #[test]
    fn a_restricted_viewer_browses_the_artists_credited_on_visible_releases_and_their_tracks()
    -> anyhow::Result<()> {
        let credits = credits(true, 0)?;
        assert_eq!(browsed(&credits, false)?, credits.visible_artists);
        Ok(())
    }

    #[test]
    fn browsing_keeps_only_artists_flagged_credited() -> anyhow::Result<()> {
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

        assert_eq!(browsed(&credits, false)?, credits.visible_artists);
        let browsed_by_admin = browsed(&credits, true)?;
        assert!(browsed_by_admin.contains(&flagged));
        assert!(!browsed_by_admin.contains(&unflagged));
        Ok(())
    }

    #[test]
    fn a_restricted_viewer_browses_and_pages_the_visible_artists() -> anyhow::Result<()> {
        for pad_releases in [true, false] {
            let credits = credits(pad_releases, 40)?;
            let db = &credits.db;
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
                    paged.extend(
                        page.items
                            .into_iter()
                            .filter_map(|artist| artist.item.db_id),
                    );
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

    /// Two artists with releases in two libraries, one of them in Rock.
    struct Scoped {
        db: DbAny,
        admin: Viewer,
        first: DbId,
        second: DbId,
        library_a: DbId,
        library_b: DbId,
        rock: DbId,
    }

    /// The first artist has "A1" (Rock, two tracks) and "A2" (one track) in library A, "B1"
    /// (three tracks) in library B, and a track credit on "B2". The second has "A3" (one track) in
    /// A, and "B2" (one track besides the first artist's) and "B3" (one track) in B.
    fn scoped() -> anyhow::Result<Scoped> {
        let mut db = new_test_db()?;
        let library_a = insert_library(&mut db, "A", "/scoped/a")?;
        let library_b = insert_library(&mut db, "B", "/scoped/b")?;
        let first = insert_artist(&mut db, "First")?;
        let second = insert_artist(&mut db, "Second")?;
        let release = |db: &mut DbAny,
                       title: &str,
                       library: DbId,
                       artist: DbId,
                       tracks: usize|
         -> anyhow::Result<Vec<DbId>> {
            let release = insert_release(db, title)?;
            connect(db, library, release)?;
            connect_artist(db, release, artist)?;
            let mut inserted = Vec::new();
            for index in 0..tracks {
                let track = insert_track(db, &format!("{title} {index}"))?;
                connect(db, release, track)?;
                inserted.push(track);
            }
            Ok(inserted)
        };
        release(&mut db, "A1", library_a, first, 2)?;
        release(&mut db, "A2", library_a, first, 1)?;
        release(&mut db, "B1", library_b, first, 3)?;
        release(&mut db, "A3", library_a, second, 1)?;
        let featured = release(&mut db, "B2", library_b, second, 2)?;
        connect_artist(&mut db, featured[0], first)?;
        release(&mut db, "B3", library_b, second, 1)?;
        let a1 = lyra_db::releases::get_by_artist(&db, first)?
            .into_iter()
            .find(|release| release.release_title == "A1")
            .and_then(|release| release.db_id)
            .map(DbId::from)
            .expect("A1 exists");
        lyra_db::genres::sync_release_genres(&mut db, a1, &["Rock".to_string()])?;
        let rock = lyra_db::genres::find_by_name(&db, "Rock")?.expect("genre exists");
        let user = insert_user(&mut db, "admin")?;
        let principal =
            Principal::for_user(&db, user, vec![lyra_db::Permission::Admin], HashSet::new());
        let admin = Viewer::user(&db, principal)?;
        Ok(Scoped {
            db,
            admin,
            first,
            second,
            library_a,
            library_b,
            rock,
        })
    }

    impl Scoped {
        /// Each artist's release and track counts under `filter`.
        fn counts(&self, filter: &ArtistFilter) -> anyhow::Result<HashMap<DbId, Vec<u64>>> {
            Ok(counts::<Artists>(
                &self.db,
                &self.admin,
                filter,
                vec![self.first, self.second],
                &[ArtistKey::ReleaseCount, ArtistKey::TrackCount],
            )?)
        }
    }

    #[test]
    fn counts_without_a_scope_cover_every_visible_release() -> anyhow::Result<()> {
        let scoped = scoped()?;
        let counts = scoped.counts(&ArtistFilter::default())?;
        assert_eq!(counts[&scoped.first], vec![3, 7]);
        assert_eq!(counts[&scoped.second], vec![3, 4]);
        // Choosing which artists appear leaves their counts alone.
        let counts = scoped.counts(&ArtistFilter {
            credit_role: Some(CreditRole::Release),
            ..ArtistFilter::default()
        })?;
        assert_eq!(counts[&scoped.first], vec![3, 7]);
        Ok(())
    }

    #[test]
    fn counts_follow_the_library_scope() -> anyhow::Result<()> {
        let scoped = scoped()?;
        let in_a = ArtistFilter {
            library: Some(scoped.library_a),
            ..ArtistFilter::default()
        };
        let counts = scoped.counts(&in_a)?;
        assert_eq!(counts[&scoped.first], vec![2, 3]);
        assert_eq!(counts[&scoped.second], vec![1, 1]);
        // A track credit counts where its release lies.
        let counts = scoped.counts(&ArtistFilter {
            library: Some(scoped.library_b),
            ..ArtistFilter::default()
        })?;
        assert_eq!(counts[&scoped.first], vec![1, 4]);
        assert_eq!(counts[&scoped.second], vec![2, 3]);
        // A credit filter still only chooses which artists appear.
        let counts = scoped.counts(&ArtistFilter {
            credit_role: Some(CreditRole::Release),
            ..in_a
        })?;
        assert_eq!(counts[&scoped.first], vec![2, 3]);
        Ok(())
    }

    #[test]
    fn counts_follow_the_genre_scope() -> anyhow::Result<()> {
        let scoped = scoped()?;
        let counts = scoped.counts(&ArtistFilter {
            genres: vec![scoped.rock],
            ..ArtistFilter::default()
        })?;
        assert_eq!(counts[&scoped.first], vec![1, 2]);
        assert_eq!(counts[&scoped.second], vec![0, 0]);
        Ok(())
    }

    #[test]
    fn sorting_by_release_count_uses_the_scoped_counts() -> anyhow::Result<()> {
        let scoped = scoped()?;
        let by_count = |library: Option<DbId>| -> anyhow::Result<Vec<DbId>> {
            let mut query = Query::<Artists>::new(ArtistFilter {
                library,
                ..ArtistFilter::default()
            });
            query.sort = vec![
                (ArtistKey::ReleaseCount, Direction::Descending),
                (ArtistKey::TrackCount, Direction::Descending),
            ];
            Ok(order(&scoped.db, &scoped.admin, &query)?)
        };
        assert_eq!(by_count(None)?, vec![scoped.first, scoped.second]);
        assert_eq!(
            by_count(Some(scoped.library_a))?,
            vec![scoped.first, scoped.second]
        );
        assert_eq!(
            by_count(Some(scoped.library_b))?,
            vec![scoped.second, scoped.first]
        );
        Ok(())
    }
}
