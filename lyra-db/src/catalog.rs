// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The whole browsable catalog in a few passes, for indexes kept outside the database.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::{
    ArtistType,
    CreditType,
    DbAccess,
    roots::{
        self,
        Root,
    },
};

pub struct CatalogTrack {
    pub id: DbId,
    pub title: String,
    pub sort_title: Option<String>,
    /// `file_created_at_ms`, or `created_at_ms` without one.
    pub created: Option<u64>,
    pub year: Option<u64>,
    pub duration_ms: Option<u64>,
    pub disc: Option<u64>,
    pub track: Option<u64>,
}

pub struct CatalogRelease {
    pub id: DbId,
    pub title: String,
    pub sort_title: Option<String>,
    pub created: Option<u64>,
    pub release_date: Option<String>,
}

pub struct CatalogArtist {
    pub id: DbId,
    pub name: String,
    pub sort_name: Option<String>,
    pub created: Option<u64>,
    pub artist_type: Option<ArtistType>,
    pub credited: bool,
    pub scan_aliases: Vec<String>,
}

pub struct CatalogGenre {
    pub id: DbId,
    pub name: String,
    pub aliases: Vec<String>,
}

pub struct CatalogLibrary {
    pub id: DbId,
    pub public_id: String,
}

pub struct CatalogCredit {
    pub owner: DbId,
    pub artist: DbId,
    pub credit_type: CreditType,
    pub artist_order: u64,
}

/// Every track, release, artist, genre and library, and the edges between them that browsing
/// follows.
pub struct Catalog {
    pub tracks: Vec<CatalogTrack>,
    pub releases: Vec<CatalogRelease>,
    pub artists: Vec<CatalogArtist>,
    pub genres: Vec<CatalogGenre>,
    pub libraries: Vec<CatalogLibrary>,
    /// `(release, track)`.
    pub release_tracks: Vec<(DbId, DbId)>,
    /// `(library, release)`.
    pub library_releases: Vec<(DbId, DbId)>,
    /// `(genre, release)`.
    pub genre_releases: Vec<(DbId, DbId)>,
    /// Credits on releases and tracks.
    pub credits: Vec<CatalogCredit>,
    /// The tracks with sources none of which can play, as `track_sources::is_available` decides
    /// for one.
    pub unavailable_tracks: HashSet<DbId>,
}

/// Reads in parallel: the reads share `db` and agdb reads take it by reference.
pub fn read(db: &(impl DbAccess + Sync)) -> anyhow::Result<Catalog> {
    std::thread::scope(|scope| {
        let tracks = scope.spawn(|| nodes(db, roots::member_ids(db, Root::Tracks)?, "Track"));
        let release_edges = scope.spawn(|| member_edges(db, Root::Releases));
        let track_edges = scope.spawn(|| member_edges(db, Root::Tracks));
        let unavailable = scope.spawn(|| unavailable_tracks(db));
        let others = scope.spawn(|| -> anyhow::Result<_> {
            Ok((
                nodes(db, roots::member_ids(db, Root::Releases)?, "Release")?,
                nodes(db, roots::member_ids(db, Root::Artists)?, "Artist")?,
                nodes(db, library_ids(db)?, "Library")?,
                nodes(db, roots::member_ids(db, Root::Genres)?, "Genre")?,
                library_edges(db)?,
                member_edges(db, Root::Genres)?,
                alias_names(db)?,
            ))
        });
        let (releases, artists, libraries, genres, library_edges, genre_edges, alias_names) =
            join(others)?;
        let release_edges = join(release_edges)?;
        let track_edges = join(track_edges)?;
        let unavailable_tracks = join(unavailable)?;

        let artists = artists
            .into_iter()
            .filter_map(|element| {
                let values = Values(&element.values);
                Some(CatalogArtist {
                    id: element.id,
                    name: values.text("artist_name")?,
                    sort_name: values.text("sort_name"),
                    created: values.created(),
                    artist_type: values
                        .get("artist_type")
                        .and_then(|value| ArtistType::try_from(value.clone()).ok()),
                    credited: values.number(super::credits::CREDITED) == Some(1),
                    scan_aliases: match values.get("scan_aliases") {
                        Some(DbValue::VecString(aliases)) => aliases.clone(),
                        _ => Vec::new(),
                    },
                })
            })
            .collect::<Vec<_>>();
        let artist_ids = artists
            .iter()
            .map(|artist| artist.id)
            .collect::<HashSet<_>>();
        let credits = std::thread::scope(|scope| {
            let release_credits = scope.spawn(|| credit_edges(db, &release_edges, &artist_ids));
            let mut credits = credit_edges(db, &track_edges, &artist_ids)?;
            credits.extend(join(release_credits)?);
            anyhow::Ok(credits)
        })?;

        let tracks = join(tracks)?
            .into_iter()
            .filter_map(|element| {
                let values = Values(&element.values);
                Some(CatalogTrack {
                    id: element.id,
                    title: values.text("track_title")?,
                    sort_title: values.text("sort_title"),
                    created: values.created(),
                    year: values.number("year"),
                    duration_ms: values.number("duration_ms"),
                    disc: values.number("disc"),
                    track: values.number("track"),
                })
            })
            .collect::<Vec<_>>();
        let releases = releases
            .into_iter()
            .filter_map(|element| {
                let values = Values(&element.values);
                Some(CatalogRelease {
                    id: element.id,
                    title: values.text("release_title")?,
                    sort_title: values.text("sort_title"),
                    created: values.created(),
                    release_date: values.text("release_date"),
                })
            })
            .collect::<Vec<_>>();
        let libraries = libraries
            .into_iter()
            .filter_map(|element| {
                Some(CatalogLibrary {
                    id: element.id,
                    public_id: Values(&element.values).text("id")?,
                })
            })
            .collect::<Vec<_>>();

        let track_ids = tracks.iter().map(|track| track.id).collect::<HashSet<_>>();
        let release_ids = releases
            .iter()
            .map(|release| release.id)
            .collect::<HashSet<_>>();
        let leading_to = |edges: &[(DbId, DbId, DbId)], targets: &HashSet<DbId>| {
            edges
                .iter()
                .filter(|(_, _, to)| targets.contains(to))
                .map(|(_, from, to)| (*from, *to))
                .collect::<Vec<_>>()
        };
        let mut aliases = HashMap::<DbId, Vec<String>>::new();
        for (_, genre, alias) in &genre_edges {
            if let Some(name) = alias_names.get(alias) {
                aliases.entry(*genre).or_default().push(name.clone());
            }
        }
        let genres = genres
            .into_iter()
            .filter_map(|element| {
                Some(CatalogGenre {
                    id: element.id,
                    name: Values(&element.values).text("name")?,
                    aliases: aliases.remove(&element.id).unwrap_or_default(),
                })
            })
            .collect();

        Ok(Catalog {
            unavailable_tracks,
            release_tracks: leading_to(&release_edges, &track_ids),
            library_releases: leading_to(&library_edges, &release_ids),
            genre_releases: leading_to(&genre_edges, &release_ids),
            tracks,
            releases,
            artists,
            genres,
            libraries,
            credits,
        })
    })
}

/// The tracks some of whose sources' entries are missing and none of whose sources can play, as
/// `track_sources::is_available` decides for each. Reads only around the missing entries.
fn unavailable_tracks(db: &impl DbAccess) -> anyhow::Result<HashSet<DbId>> {
    let mut tracks = HashSet::new();
    for entry in super::entries::missing_ids(db)? {
        for source in super::track_sources::depending_on(db, entry)? {
            tracks.extend(super::graph::inbound_neighbor_ids(db, source, "Track")?);
        }
    }
    let mut unavailable = HashSet::new();
    for track in tracks {
        if !super::track_sources::track_available(db, track)? {
            unavailable.insert(track);
        }
    }
    Ok(unavailable)
}

fn join<T>(handle: std::thread::ScopedJoinHandle<'_, anyhow::Result<T>>) -> anyhow::Result<T> {
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// The names of the genre aliases, by alias node.
fn alias_names(db: &impl DbAccess) -> anyhow::Result<HashMap<DbId, String>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .search()
                .from(Root::Genres.alias())
                .where_()
                .distance(roots::MEMBER_CHILDREN)
                .and()
                .key("db_element_id")
                .value("GenreAlias")
                .query(),
        )?
        .elements
        .into_iter()
        .filter_map(|element| Some((element.id, Values(&element.values).text("name")?)))
        .collect())
}

/// Every library.
fn library_ids(db: &impl DbAccess) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from("libraries")
                .where_()
                .neighbor()
                .query(),
        )?
        .ids())
}

/// The `kind` nodes among `ids`, with their values. Reading the ids first and their values by id
/// costs a fraction of a search that selects values as it filters.
fn nodes(
    db: &(impl DbAccess + Sync),
    mut ids: Vec<DbId>,
    kind: &str,
) -> anyhow::Result<Vec<DbElement>> {
    ids.sort_unstable();
    Ok(select(db, ids)?
        .into_iter()
        .filter(|element| super::graph::is_element_type(element, kind))
        .collect())
}

/// The elements `ids`, with their values, read in ordered chunks across threads.
fn select(db: &(impl DbAccess + Sync), ids: Vec<DbId>) -> anyhow::Result<Vec<DbElement>> {
    const CHUNK: usize = 8_192;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    if ids.len() <= CHUNK {
        return Ok(db.exec(QueryBuilder::select().ids(ids).query())?.elements);
    }
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let size = ids.len().div_ceil(threads).max(CHUNK);
    std::thread::scope(|scope| {
        let reads = ids
            .chunks(size)
            .map(|chunk| {
                scope.spawn(move || {
                    Ok(db
                        .exec(QueryBuilder::select().ids(chunk.to_vec()).query())?
                        .elements)
                })
            })
            .collect::<Vec<_>>();
        let mut elements = Vec::with_capacity(ids.len());
        for read in reads {
            elements.extend(join(read)?);
        }
        Ok(elements)
    })
}

/// The outgoing edges of `root`'s members, as `(edge, from, to)`.
fn member_edges(db: &impl DbAccess, root: Root) -> anyhow::Result<Vec<(DbId, DbId, DbId)>> {
    Ok(triples(roots::member_edges(db, root)?))
}

/// The outgoing edges of every library, as `(edge, from, to)`.
fn library_edges(db: &impl DbAccess) -> anyhow::Result<Vec<(DbId, DbId, DbId)>> {
    Ok(triples(
        db.exec(
            QueryBuilder::search()
                .from("libraries")
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(3))
                .query(),
        )?
        .elements,
    ))
}

fn triples(edges: Vec<DbElement>) -> Vec<(DbId, DbId, DbId)> {
    edges
        .into_iter()
        .map(|element| (element.id, element.from, element.to))
        .collect()
}

/// The credits among `edges`, given as `(edge, from, to)`, that lead to one of `artists`.
fn credit_edges(
    db: &(impl DbAccess + Sync),
    edges: &[(DbId, DbId, DbId)],
    artists: &HashSet<DbId>,
) -> anyhow::Result<Vec<CatalogCredit>> {
    let mut ids = edges
        .iter()
        .filter(|(_, _, to)| artists.contains(to))
        .map(|(edge, _, _)| *edge)
        .collect::<Vec<_>>();
    ids.sort_unstable_by_key(|id| std::cmp::Reverse(id.0));
    Ok(select(db, ids)?
        .into_iter()
        .filter(|element| super::graph::is_element_type(element, "Credit"))
        .filter_map(|element| {
            let values = Values(&element.values);
            Some(CatalogCredit {
                owner: element.from,
                artist: element.to,
                credit_type: values
                    .get("credit_type")
                    .and_then(|value| CreditType::try_from(value.clone()).ok())?,
                artist_order: values.number("artist_order").unwrap_or(0),
            })
        })
        .collect())
}

struct Values<'a>(&'a [agdb::DbKeyValue]);

impl Values<'_> {
    fn get(&self, key: &str) -> Option<&DbValue> {
        self.0
            .iter()
            .find(|kv| kv.key.string().is_ok_and(|stored| stored == key))
            .map(|kv| &kv.value)
    }

    fn text(&self, key: &str) -> Option<String> {
        self.get(key)?.string().ok().cloned()
    }

    fn number(&self, key: &str) -> Option<u64> {
        self.get(key)?.to_u64().ok()
    }

    fn created(&self) -> Option<u64> {
        self.number("file_created_at_ms")
            .or_else(|| self.number("created_at_ms"))
    }
}
