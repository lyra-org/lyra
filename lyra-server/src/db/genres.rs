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
    DbElement,
    DbId,
    QueryBuilder,
};
use nanoid::nanoid;
use rand::seq::SliceRandom;

use super::DbAccess;
use super::NodeId;
use super::providers::external_ids;

const MAX_EXPANDED_GENRES: usize = 20;
const MIN_EXPANSION_WEIGHT: u32 = 3;
const MAX_CHILDREN_SAMPLE: usize = 5;

#[derive(DbElement, Clone, Debug)]
pub(crate) struct Genre {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) scan_name: String,
    pub(crate) created_at: Option<u64>,
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct GenreAlias {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) scan_name: String,
    pub(crate) locale: Option<String>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock drift")
        .as_secs()
}

fn normalize(s: &str) -> String {
    s.to_lowercase()
}

pub(crate) fn find_by_name(db: &impl DbAccess, name: &str) -> anyhow::Result<Option<DbId>> {
    super::lookup::find_id_by_indexed_string_field(
        db,
        "genres",
        "scan_name",
        "scan_name",
        &normalize(name),
    )
}

pub(crate) fn find_by_alias(db: &impl DbAccess, alias: &str) -> anyhow::Result<Option<DbId>> {
    let scan = normalize(alias);
    let Ok(index_result) = db.exec(
        QueryBuilder::search()
            .index("scan_name")
            .value(&scan)
            .query(),
    ) else {
        return Ok(None);
    };
    for alias_db_id in index_result.ids().into_iter().filter(|id| id.0 > 0) {
        let owners: Vec<Genre> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Genre>()
                    .search()
                    .to(alias_db_id)
                    .where_()
                    .distance(agdb::CountComparison::Equal(2))
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        if let Some(genre) = owners.into_iter().next() {
            if let Some(db_id) = genre.db_id.map(DbId::from) {
                return Ok(Some(db_id));
            }
        }
    }
    Ok(None)
}

pub(crate) fn find_by_external_id(
    db: &DbAny,
    provider_id: &str,
    id_type: &str,
    id_value: &str,
) -> anyhow::Result<Option<DbId>> {
    external_ids::get_owner(db, provider_id, id_type, id_value, Some("Genre"))
}

fn insert_genre(db: &mut impl DbAccess, name: &str) -> anyhow::Result<DbId> {
    let genre = Genre {
        db_id: None,
        id: nanoid!(),
        name: name.to_string(),
        scan_name: normalize(name),
        created_at: Some(now_secs()),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&genre).query())?;
    let genre_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("genre creation returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("genres")
            .to(genre_id)
            .query(),
    )?;
    Ok(genre_id)
}

pub(crate) fn add_alias(
    db: &mut impl DbAccess,
    genre_id: DbId,
    alias: &str,
    locale: Option<&str>,
) -> anyhow::Result<DbId> {
    let scan = normalize(alias);

    let existing_aliases = get_aliases(db, genre_id)?;
    if let Some(existing) = existing_aliases.iter().find(|a| a.scan_name == scan) {
        return existing
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("existing alias has no db_id"));
    }

    let alias_node = GenreAlias {
        db_id: None,
        id: nanoid!(),
        name: alias.to_string(),
        scan_name: scan,
        locale: locale.map(String::from),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&alias_node).query())?;
    let alias_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("alias creation returned no id"))?;
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(genre_id)
            .to(alias_id)
            .query(),
    )?;
    Ok(alias_id)
}

pub(crate) struct ResolveGenre<'a> {
    pub(crate) name: &'a str,
    pub(crate) aliases: &'a [(&'a str, Option<&'a str>)],
    pub(crate) external_id: Option<ResolveExternalId<'a>>,
}

pub(crate) struct ResolveExternalId<'a> {
    pub(crate) provider_id: &'a str,
    pub(crate) id_type: &'a str,
    pub(crate) id_value: &'a str,
}

pub(crate) fn resolve(db: &mut DbAny, request: &ResolveGenre) -> anyhow::Result<DbId> {
    let mut genre_id = if let Some(ext) = &request.external_id {
        find_by_external_id(db, ext.provider_id, ext.id_type, ext.id_value)?
    } else {
        None
    };

    if genre_id.is_none() {
        genre_id = find_by_name(db, request.name)?;
    }

    if genre_id.is_none() {
        genre_id = find_by_alias(db, request.name)?;
    }

    if genre_id.is_none() {
        for (alias, _) in request.aliases {
            genre_id = find_by_name(db, alias)?;
            if genre_id.is_some() {
                break;
            }
            genre_id = find_by_alias(db, alias)?;
            if genre_id.is_some() {
                break;
            }
        }
    }

    let genre_id = match genre_id {
        Some(id) => id,
        None => insert_genre(db, request.name)?,
    };

    if let Some(ext) = &request.external_id {
        external_ids::upsert(
            db,
            genre_id,
            ext.provider_id,
            ext.id_type,
            ext.id_value,
            super::IdSource::Plugin,
        )?;
    }

    for (alias, locale) in request.aliases {
        add_alias(db, genre_id, alias, *locale)?;
    }

    Ok(genre_id)
}

/// Like [`resolve`] but without external ID matching, so it works with any
/// `DbAccess` impl (including transaction contexts).
pub(crate) fn resolve_by_name(db: &mut impl DbAccess, name: &str) -> anyhow::Result<DbId> {
    if let Some(id) = find_by_name(db, name)? {
        return Ok(id);
    }
    if let Some(id) = find_by_alias(db, name)? {
        return Ok(id);
    }
    insert_genre(db, name)
}

pub(crate) fn sync_release_genres(
    db: &mut impl DbAccess,
    release_id: DbId,
    genre_names: &[String],
) -> anyhow::Result<()> {
    let existing = get_for_release(db, release_id)?;

    let mut desired_genre_ids = HashSet::new();
    for name in genre_names {
        let genre_id = resolve_by_name(db, name)?;
        if desired_genre_ids.insert(genre_id) {
            link_to_release(db, genre_id, release_id)?;
        }
    }

    for genre in existing {
        let Some(genre_db_id) = genre.db_id.map(DbId::from) else {
            continue;
        };
        if !desired_genre_ids.contains(&genre_db_id) {
            super::graph::remove_edges_between(db, genre_db_id, release_id)?;
        }
    }

    Ok(())
}

pub(crate) fn link_to_release(
    db: &mut impl DbAccess,
    genre_id: DbId,
    release_id: DbId,
) -> anyhow::Result<()> {
    if !super::graph::edge_exists(db, genre_id, release_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(genre_id)
                .to(release_id)
                .query(),
        )?;
    }
    Ok(())
}

/// Link a child genre to a parent genre (child → parent direction).
/// Rejects self-links and direct two-node cycles (parent already points to
/// child). Does not detect transitive cycles (A → B → C → A) — callers that
/// walk ancestors must bound depth or track visited nodes.
pub(crate) fn link_to_parent(
    db: &mut impl DbAccess,
    child_id: DbId,
    parent_id: DbId,
) -> anyhow::Result<()> {
    if child_id == parent_id {
        anyhow::bail!("genre cannot be its own parent");
    }
    // Reject direct back-edge (parent → child already exists).
    if super::graph::edge_exists(db, parent_id, child_id)? {
        anyhow::bail!("adding this parent would create a direct cycle");
    }
    if !super::graph::edge_exists(db, child_id, parent_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(child_id)
                .to(parent_id)
                .query(),
        )?;
    }
    Ok(())
}

pub(crate) fn get_parents(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<Genre>> {
    let genres: Vec<Genre> = db
        .exec(
            QueryBuilder::select()
                .elements::<Genre>()
                .search()
                .from(genre_id)
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(genres)
}

pub(crate) fn get_children(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<Genre>> {
    let genres: Vec<Genre> = db
        .exec(
            QueryBuilder::select()
                .elements::<Genre>()
                .search()
                .to(genre_id)
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(genres)
}

pub(crate) fn get_aliases(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<GenreAlias>> {
    let aliases: Vec<GenreAlias> = db
        .exec(
            QueryBuilder::select()
                .elements::<GenreAlias>()
                .search()
                .from(genre_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(aliases)
}

pub(crate) fn get_all(db: &impl DbAccess) -> anyhow::Result<Vec<Genre>> {
    let genres: Vec<Genre> = db
        .exec(
            QueryBuilder::select()
                .elements::<Genre>()
                .search()
                .from("genres")
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(genres)
}

/// Get a single genre by its database ID.
pub(crate) fn get_by_id(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Option<Genre>> {
    super::graph::fetch_typed_by_id(db, genre_id, "Genre")
}

pub(crate) fn get_names_for_release(
    db: &impl DbAccess,
    release_id: DbId,
) -> anyhow::Result<Option<Vec<String>>> {
    let genres = get_for_release(db, release_id)?;
    if genres.is_empty() {
        return Ok(None);
    }
    Ok(Some(genres.into_iter().map(|g| g.name).collect()))
}

pub(crate) fn release_ids_matching_genres(
    db: &impl DbAccess,
    genre_names: &[String],
) -> anyhow::Result<HashSet<DbId>> {
    let mut release_ids = HashSet::new();
    for name in genre_names {
        let genre_id = find_by_name(db, name)?.or(find_by_alias(db, name)?);
        let Some(genre_id) = genre_id else {
            continue;
        };
        let releases: Vec<super::Release> = db
            .exec(
                QueryBuilder::select()
                    .elements::<super::Release>()
                    .search()
                    .from(genre_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for release in releases {
            if let Some(db_id) = release.db_id.map(DbId::from) {
                release_ids.insert(db_id);
            }
        }
    }
    Ok(release_ids)
}

/// Return the release IDs linked to a genre (inverse of `get_for_release`).
pub(crate) fn get_releases(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<DbId>> {
    let result = db.exec(
        QueryBuilder::search()
            .from(genre_id)
            .where_()
            .distance(agdb::CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value("Release")
            .query(),
    )?;
    Ok(result.ids().into_iter().filter(|id| id.0 > 0).collect())
}

/// Return release IDs for multiple genres in a single pass.
pub(crate) fn get_releases_many(
    db: &impl DbAccess,
    genre_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<DbId>>> {
    let unique_genre_ids = super::dedup_positive_ids(genre_ids);
    let mut result: HashMap<DbId, Vec<DbId>> = unique_genre_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    if unique_genre_ids.is_empty() {
        return Ok(result);
    }

    for &genre_id in &unique_genre_ids {
        let release_ids = get_releases(db, genre_id)?;
        result.insert(genre_id, release_ids);
    }

    Ok(result)
}

pub(crate) fn get_for_release(db: &impl DbAccess, release_id: DbId) -> anyhow::Result<Vec<Genre>> {
    let genres: Vec<Genre> = db
        .exec(
            QueryBuilder::select()
                .elements::<Genre>()
                .search()
                .to(release_id)
                .where_()
                .distance(agdb::CountComparison::Equal(2))
                .end_where()
                .query(),
        )?
        .try_into()?;
    Ok(genres)
}

pub(crate) fn get_for_releases_many(
    db: &impl DbAccess,
    release_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Genre>>> {
    let unique_release_ids = super::dedup_positive_ids(release_ids);
    let mut result: HashMap<DbId, Vec<Genre>> = unique_release_ids
        .iter()
        .copied()
        .map(|id| (id, Vec::new()))
        .collect();
    if unique_release_ids.is_empty() {
        return Ok(result);
    }

    let mut genre_ids_by_release: HashMap<DbId, Vec<DbId>> = HashMap::new();
    let mut all_genre_ids = Vec::new();
    let mut seen_genres = HashSet::new();

    for &release_id in &unique_release_ids {
        let edges = db.exec(
            QueryBuilder::search()
                .to(release_id)
                .where_()
                .edge()
                .and()
                .distance(agdb::CountComparison::Equal(1))
                .query(),
        )?;
        let mut release_genre_ids = Vec::new();
        for edge in edges.elements {
            let Some(genre_id) = edge.from else {
                continue;
            };
            if genre_id.0 <= 0 {
                continue;
            }
            release_genre_ids.push(genre_id);
            if seen_genres.insert(genre_id) {
                all_genre_ids.push(genre_id);
            }
        }
        genre_ids_by_release.insert(release_id, release_genre_ids);
    }

    if all_genre_ids.is_empty() {
        return Ok(result);
    }

    let genres_by_id: HashMap<DbId, Genre> =
        super::graph::bulk_fetch_typed(db, all_genre_ids, "Genre")?;

    for &release_id in &unique_release_ids {
        let Some(genre_ids) = genre_ids_by_release.get(&release_id) else {
            continue;
        };
        let Some(release_genres) = result.get_mut(&release_id) else {
            continue;
        };
        for genre_id in genre_ids {
            if let Some(genre) = genres_by_id.get(genre_id) {
                release_genres.push(genre.clone());
            }
        }
    }

    Ok(result)
}

pub(crate) fn expand_related(
    db: &impl DbAccess,
    seed_ids: &[DbId],
    max_depth: u32,
) -> anyhow::Result<Vec<(DbId, u32)>> {
    if seed_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut result: Vec<(DbId, u32)> = Vec::new();
    let mut visited: HashSet<DbId> = HashSet::new();

    // Level 0: seeds at weight 10
    for &seed_id in seed_ids {
        if !visited.insert(seed_id) {
            continue;
        }
        if get_by_id(db, seed_id)?.is_none() {
            continue;
        }
        result.push((seed_id, 10));
    }

    // Level 1: parents + children of seeds at weight 5 / 3
    let mut seed_parents: Vec<DbId> = Vec::new();

    for &seed_id in seed_ids {
        for parent in get_parents(db, seed_id)? {
            let Some(db_id) = parent.db_id.map(DbId::from) else {
                continue;
            };
            if visited.insert(db_id) {
                seed_parents.push(db_id);
                result.push((db_id, 5));
            }
        }

        let mut children = get_children(db, seed_id)?;
        if children.len() > MAX_CHILDREN_SAMPLE {
            children.shuffle(&mut rand::rng());
            children.truncate(MAX_CHILDREN_SAMPLE);
        }
        for child in children {
            let Some(db_id) = child.db_id.map(DbId::from) else {
                continue;
            };
            if visited.insert(db_id) {
                result.push((db_id, 3));
            }
        }
    }

    if result.len() >= MAX_EXPANDED_GENRES {
        result.truncate(MAX_EXPANDED_GENRES);
        return Ok(result);
    }

    // Level 2: grandparents + siblings at weight 3
    if max_depth >= 2 {
        for parent_id in &seed_parents {
            for grandparent in get_parents(db, *parent_id)? {
                let Some(db_id) = grandparent.db_id.map(DbId::from) else {
                    continue;
                };
                if visited.insert(db_id) {
                    result.push((db_id, 3));
                }
            }

            for sibling in get_children(db, *parent_id)? {
                let Some(db_id) = sibling.db_id.map(DbId::from) else {
                    continue;
                };
                if visited.insert(db_id) {
                    result.push((db_id, 3));
                }
            }
        }

        if result.len() >= MAX_EXPANDED_GENRES {
            result.truncate(MAX_EXPANDED_GENRES);
        }
    }

    result.retain(|(_, weight)| *weight >= MIN_EXPANSION_WEIGHT);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::new_test_db;

    #[test]
    fn resolve_creates_genre_on_first_call() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        assert!(find_by_name(&db, "Rock")?.is_some());
        Ok(())
    }

    #[test]
    fn resolve_deduplicates_by_scan_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let second = resolve(
            &mut db,
            &ResolveGenre {
                name: "rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_matches_by_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = resolve(
            &mut db,
            &ResolveGenre {
                name: "electronic",
                aliases: &[("electronic music", None)],
                external_id: None,
            },
        )?;
        let second = resolve(
            &mut db,
            &ResolveGenre {
                name: "electronic music",
                aliases: &[],
                external_id: None,
            },
        )?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_matches_by_provided_aliases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = resolve(
            &mut db,
            &ResolveGenre {
                name: "electronic",
                aliases: &[],
                external_id: None,
            },
        )?;
        let second = resolve(
            &mut db,
            &ResolveGenre {
                name: "musique électronique",
                aliases: &[("electronic", Some("en"))],
                external_id: None,
            },
        )?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_matches_by_external_id() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let first = resolve(
            &mut db,
            &ResolveGenre {
                name: "electronic",
                aliases: &[],
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "genre_id",
                    id_value: "89255676-1f14-4dd8-bbad-fca839d6aff4",
                }),
            },
        )?;
        let second = resolve(
            &mut db,
            &ResolveGenre {
                name: "elektronische Musik",
                aliases: &[],
                external_id: Some(ResolveExternalId {
                    provider_id: "musicbrainz",
                    id_type: "genre_id",
                    id_value: "89255676-1f14-4dd8-bbad-fca839d6aff4",
                }),
            },
        )?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn resolve_accumulates_aliases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let genre_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "hip hop",
                aliases: &[("Hip-Hop", None)],
                external_id: None,
            },
        )?;
        resolve(
            &mut db,
            &ResolveGenre {
                name: "hip hop",
                aliases: &[("hiphop", None), ("rap", Some("fi"))],
                external_id: None,
            },
        )?;

        let aliases = get_aliases(&db, genre_id)?;
        assert_eq!(aliases.len(), 3);
        let alias_names: HashSet<&str> = aliases.iter().map(|a| a.name.as_str()).collect();
        assert!(alias_names.contains("Hip-Hop"));
        assert!(alias_names.contains("hiphop"));
        assert!(alias_names.contains("rap"));
        Ok(())
    }

    #[test]
    fn add_alias_deduplicates_by_scan_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;

        let genre_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        let first = add_alias(&mut db, genre_id, "Rock Music", None)?;
        let second = add_alias(&mut db, genre_id, "rock music", None)?;

        assert_eq!(first, second);
        assert_eq!(get_aliases(&db, genre_id)?.len(), 1);
        Ok(())
    }

    #[test]
    fn link_to_release_creates_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Jazz",
                aliases: &[],
                external_id: None,
            },
        )?;
        let release_id = crate::db::test_db::insert_release(&mut db, "Blue Train")?;

        link_to_release(&mut db, genre_id, release_id)?;

        let genres = get_for_release(&db, release_id)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Jazz");
        Ok(())
    }

    #[test]
    fn multiple_genres_per_album() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let blues_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Blues",
                aliases: &[],
                external_id: None,
            },
        )?;
        let release_id = crate::db::test_db::insert_release(&mut db, "Crossroads")?;

        link_to_release(&mut db, rock_id, release_id)?;
        link_to_release(&mut db, blues_id, release_id)?;

        let genres = get_for_release(&db, release_id)?;
        assert_eq!(genres.len(), 2);
        let names: HashSet<&str> = genres.iter().map(|g| g.name.as_str()).collect();
        assert!(names.contains("Rock"));
        assert!(names.contains("Blues"));
        Ok(())
    }

    #[test]
    fn sync_release_genres_removes_stale_links() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = crate::db::test_db::insert_release(&mut db, "Shifting Genres")?;

        sync_release_genres(
            &mut db,
            release_id,
            &["Rock".to_string(), "Jazz".to_string()],
        )?;
        let genres = get_for_release(&db, release_id)?;
        assert_eq!(genres.len(), 2);

        sync_release_genres(&mut db, release_id, &["Jazz".to_string()])?;
        let genres = get_for_release(&db, release_id)?;
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].name, "Jazz");

        // Rock node still exists, just unlinked
        assert!(find_by_name(&db, "Rock")?.is_some());
        Ok(())
    }

    #[test]
    fn sync_release_genres_replaces_all_on_empty() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = crate::db::test_db::insert_release(&mut db, "Gone")?;

        sync_release_genres(&mut db, release_id, &["Rock".to_string()])?;
        assert_eq!(get_for_release(&db, release_id)?.len(), 1);

        sync_release_genres(&mut db, release_id, &[])?;
        assert_eq!(get_for_release(&db, release_id)?.len(), 0);
        Ok(())
    }

    #[test]
    fn link_to_parent_creates_hierarchy() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let alt_rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Alternative Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let shoegaze_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Shoegaze",
                aliases: &[],
                external_id: None,
            },
        )?;

        link_to_parent(&mut db, shoegaze_id, alt_rock_id)?;
        link_to_parent(&mut db, alt_rock_id, rock_id)?;

        let parents = get_parents(&db, shoegaze_id)?;
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].name, "Alternative Rock");

        let children = get_children(&db, alt_rock_id)?;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, "Shoegaze");

        let rock_children = get_children(&db, rock_id)?;
        assert_eq!(rock_children.len(), 1);
        assert_eq!(rock_children[0].name, "Alternative Rock");

        Ok(())
    }

    #[test]
    fn link_to_parent_rejects_self_link() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        let result = link_to_parent(&mut db, rock_id, rock_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("own parent"));
        Ok(())
    }

    #[test]
    fn link_to_parent_rejects_direct_cycle() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let alt_rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Alternative Rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        link_to_parent(&mut db, alt_rock_id, rock_id)?;
        let result = link_to_parent(&mut db, rock_id, alt_rock_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cycle"));
        Ok(())
    }

    #[test]
    fn link_to_parent_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let alt_rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Alternative Rock",
                aliases: &[],
                external_id: None,
            },
        )?;

        link_to_parent(&mut db, alt_rock_id, rock_id)?;
        link_to_parent(&mut db, alt_rock_id, rock_id)?;

        let parents = get_parents(&db, alt_rock_id)?;
        assert_eq!(parents.len(), 1);
        Ok(())
    }

    #[test]
    fn genre_hierarchy_does_not_affect_album_links() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let alt_rock_id = resolve(
            &mut db,
            &ResolveGenre {
                name: "Alternative Rock",
                aliases: &[],
                external_id: None,
            },
        )?;
        let release_id = crate::db::test_db::insert_release(&mut db, "OK Computer")?;

        link_to_parent(&mut db, alt_rock_id, rock_id)?;
        link_to_release(&mut db, alt_rock_id, release_id)?;

        // Parents should only return genre nodes, not releases.
        let parents = get_parents(&db, alt_rock_id)?;
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].name, "Rock");

        // Release genres should only return genre nodes, not parent genres.
        let release_genres = get_for_release(&db, release_id)?;
        assert_eq!(release_genres.len(), 1);
        assert_eq!(release_genres[0].name, "Alternative Rock");

        Ok(())
    }

    fn resolve_simple(db: &mut agdb::DbAny, name: &str) -> anyhow::Result<DbId> {
        resolve(
            db,
            &ResolveGenre {
                name,
                aliases: &[],
                external_id: None,
            },
        )
    }

    #[test]
    fn expand_related_returns_seeds_at_max_weight() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = resolve_simple(&mut db, "GenreA")?;
        let b = resolve_simple(&mut db, "GenreB")?;

        let result = expand_related(&db, &[a, b], 2)?;

        assert_eq!(result.len(), 2);
        for g in &result {
            assert_eq!(g.1, 10);
            assert!(g.0 == a || g.0 == b);
        }
        Ok(())
    }

    #[test]
    fn expand_related_finds_parents_and_children() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock = resolve_simple(&mut db, "Rock")?;
        let alt_rock = resolve_simple(&mut db, "Alternative Rock")?;
        let shoegaze = resolve_simple(&mut db, "Shoegaze")?;

        link_to_parent(&mut db, alt_rock, rock)?;
        link_to_parent(&mut db, shoegaze, alt_rock)?;

        let result = expand_related(&db, &[alt_rock], 2)?;

        let map: std::collections::HashMap<DbId, u32> = result.iter().copied().collect();
        assert_eq!(map[&alt_rock], 10);
        assert_eq!(map[&rock], 5);
        assert_eq!(map[&shoegaze], 3);
        assert_eq!(result.len(), 3);
        Ok(())
    }

    #[test]
    fn expand_related_finds_siblings() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock = resolve_simple(&mut db, "Rock")?;
        let alt_rock = resolve_simple(&mut db, "Alternative Rock")?;
        let indie_rock = resolve_simple(&mut db, "Indie Rock")?;

        link_to_parent(&mut db, alt_rock, rock)?;
        link_to_parent(&mut db, indie_rock, rock)?;

        let result = expand_related(&db, &[alt_rock], 2)?;

        let map: std::collections::HashMap<DbId, u32> = result.iter().copied().collect();
        assert_eq!(map[&alt_rock], 10);
        assert_eq!(map[&rock], 5);
        assert_eq!(map[&indie_rock], 3);
        Ok(())
    }

    #[test]
    fn expand_related_terminates_on_cycle() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = resolve_simple(&mut db, "CycleA")?;
        let b = resolve_simple(&mut db, "CycleB")?;
        let c = resolve_simple(&mut db, "CycleC")?;

        // A→B, B→C, C→A (transitive cycle, each step is valid)
        link_to_parent(&mut db, a, b)?;
        link_to_parent(&mut db, b, c)?;
        link_to_parent(&mut db, c, a)?;

        let result = expand_related(&db, &[a], 2)?;

        assert!(result.len() <= MAX_EXPANDED_GENRES);
        let ids: HashSet<DbId> = result.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&a));
        Ok(())
    }

    #[test]
    fn expand_related_deduplicates_by_max_weight() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = resolve_simple(&mut db, "TopA")?;
        let b = resolve_simple(&mut db, "MidB")?;
        let c = resolve_simple(&mut db, "MidC")?;
        let d = resolve_simple(&mut db, "BottomD")?;

        // Diamond: D→B→A, D→C→A
        link_to_parent(&mut db, d, b)?;
        link_to_parent(&mut db, d, c)?;
        link_to_parent(&mut db, b, a)?;
        link_to_parent(&mut db, c, a)?;

        let result = expand_related(&db, &[d], 2)?;

        let a_entries: Vec<_> = result.iter().filter(|(id, _)| *id == a).collect();
        assert_eq!(a_entries.len(), 1);
        assert_eq!(a_entries[0].1, 3);
        Ok(())
    }

    #[test]
    fn expand_related_caps_expanded_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let parent = resolve_simple(&mut db, "BigParent")?;

        for i in 0..25 {
            let child = resolve_simple(&mut db, &format!("Child{i}"))?;
            link_to_parent(&mut db, child, parent)?;
        }

        let result = expand_related(&db, &[parent], 2)?;

        assert!(result.len() <= MAX_EXPANDED_GENRES);
        Ok(())
    }

    #[test]
    fn expand_related_samples_children_when_many() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = resolve_simple(&mut db, "SeedGenre")?;

        for i in 0..10 {
            let child = resolve_simple(&mut db, &format!("SeedChild{i}"))?;
            link_to_parent(&mut db, child, seed)?;
        }

        let result = expand_related(&db, &[seed], 1)?;

        let children: Vec<_> = result.iter().filter(|(_, w)| *w == 3).collect();
        assert!(children.len() <= MAX_CHILDREN_SAMPLE);
        Ok(())
    }

    #[test]
    fn expand_related_empty_seeds_returns_empty() -> anyhow::Result<()> {
        let db = new_test_db()?;

        let result = expand_related(&db, &[], 2)?;

        assert!(result.is_empty());
        Ok(())
    }

    #[test]
    fn expand_related_no_hierarchy_returns_seeds_only() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let a = resolve_simple(&mut db, "Standalone1")?;
        let b = resolve_simple(&mut db, "Standalone2")?;

        let result = expand_related(&db, &[a, b], 2)?;

        assert_eq!(result.len(), 2);
        for (_, weight) in &result {
            assert_eq!(*weight, 10);
        }
        Ok(())
    }
}
