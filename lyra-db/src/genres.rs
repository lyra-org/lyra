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
use super::lookup::NameAliases;
use super::providers::external_ids;

const MAX_EXPANDED_GENRES: usize = 20;
const MIN_EXPANSION_WEIGHT: u32 = 3;
const MAX_CHILDREN_SAMPLE: usize = 5;
const NAME_ALIASES: NameAliases = NameAliases {
    index: "scan_name",
    owner: "Genre",
    alias: "GenreAlias",
};

#[derive(DbElement, Clone, Debug)]
pub struct Genre {
    pub db_id: Option<NodeId>,
    pub id: String,
    pub name: String,
    pub(crate) scan_name: String,
    pub(crate) created_at_ms: Option<u64>,
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct GenreAlias {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) scan_name: String,
    pub(crate) locale: Option<String>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock drift")
        .as_millis() as u64
}

fn normalize(s: &str) -> String {
    s.to_lowercase()
}

pub fn find_by_name(db: &impl DbAccess, name: &str) -> anyhow::Result<Option<DbId>> {
    super::lookup::find_id_by_index(db, "scan_name", &normalize(name), "Genre")
}

pub(crate) fn find_by_alias(db: &impl DbAccess, alias: &str) -> anyhow::Result<Option<DbId>> {
    Ok(NAME_ALIASES
        .holders(db, &normalize(alias))?
        .into_iter()
        .next())
}

/// Whether a genre other than `genre_id` goes by `name`, as its name or an alias.
pub fn name_taken(db: &impl DbAccess, name: &str, genre_id: DbId) -> anyhow::Result<bool> {
    NAME_ALIASES.held_by_another(db, &normalize(name.trim()), &[genre_id])
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
        created_at_ms: Some(now_ms()),
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&genre).query())?;
    let genre_id = result
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("genre creation returned no id"))?;
    super::roots::link(db, super::roots::Root::Genres, genre_id)?;
    Ok(genre_id)
}

fn insert_alias(
    db: &mut impl DbAccess,
    genre_id: DbId,
    name: &str,
    locale: Option<&str>,
) -> anyhow::Result<()> {
    let alias_node = GenreAlias {
        db_id: None,
        id: nanoid!(),
        name: name.to_string(),
        scan_name: normalize(name),
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
    Ok(())
}

/// Gives the genre `alias`, trimmed, unless it is blank, the genre has it, or another genre goes
/// by it: one name must lead to one genre.
pub(crate) fn add_alias(
    db: &mut impl DbAccess,
    genre_id: DbId,
    alias: &str,
    locale: Option<&str>,
) -> anyhow::Result<()> {
    let name = alias.trim();
    let scan = normalize(name);
    if name.is_empty()
        || get_aliases(db, genre_id)?
            .iter()
            .any(|existing| existing.scan_name == scan)
        || NAME_ALIASES.held_by_another(db, &scan, &[genre_id])?
    {
        return Ok(());
    }
    insert_alias(db, genre_id, name, locale)
}

/// Adds `loser`'s name to `winner`'s aliases and moves `loser`'s aliases there with their
/// locales, skipping blank names, `winner`'s own and names another genre goes by.
pub fn absorb_aliases(db: &mut impl DbAccess, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    let winner_genre = get_by_id(db, winner)?
        .ok_or_else(|| anyhow::anyhow!("merge winner {} missing", winner.0))?;
    let loser_genre =
        get_by_id(db, loser)?.ok_or_else(|| anyhow::anyhow!("merge loser {} missing", loser.0))?;
    let mut held: HashSet<String> = get_aliases(db, winner)?
        .into_iter()
        .map(|alias| alias.scan_name)
        .collect();
    held.insert(winner_genre.scan_name);

    let mut absorbs = |db: &mut _, name: &str| -> anyhow::Result<bool> {
        let scan = normalize(name);
        Ok(!name.is_empty()
            && !held.contains(&scan)
            && !NAME_ALIASES.held_by_another(db, &scan, &[winner, loser])?
            && held.insert(scan))
    };

    let loser_name = loser_genre.name.trim();
    if absorbs(db, loser_name)? {
        insert_alias(db, winner, loser_name, None)?;
    }
    for alias in get_aliases(db, loser)? {
        let alias_id = alias
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("genre alias has no db id"))?;
        let name = alias.name.trim();
        if !absorbs(db, name)? {
            db.exec_mut(QueryBuilder::remove().ids(alias_id).query())?;
            continue;
        }
        super::graph::remove_edges_between(db, loser, alias_id)?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(winner)
                .to(alias_id)
                .query(),
        )?;
        if name != alias.name {
            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([("name", name).into(), ("scan_name", normalize(name)).into()])
                    .ids(alias_id)
                    .query(),
            )?;
        }
    }
    Ok(())
}

/// Moves `loser`'s parents and children to `winner`, leaving out every link that would relate
/// `winner` to itself or close a cycle of any length.
pub fn absorb_relatives(db: &mut impl DbAccess, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    let ids = |genres: Vec<Genre>| -> Vec<DbId> {
        genres
            .into_iter()
            .filter_map(|genre| genre.db_id.map(DbId::from))
            .filter(|&id| id != loser)
            .collect()
    };
    let parents = ids(get_parents(db, loser)?);
    let children = ids(get_children(db, loser)?);
    // A path through `loser` ends with it, so the checks below must not walk one.
    for &parent in &parents {
        super::graph::remove_edges_between(db, loser, parent)?;
    }
    for &child in &children {
        super::graph::remove_edges_between(db, child, loser)?;
    }
    for parent in parents {
        if !has_ancestor(db, parent, winner)? {
            link_to_parent(db, winner, parent)?;
        }
    }
    for child in children {
        if !has_ancestor(db, winner, child)? {
            link_to_parent(db, child, winner)?;
        }
    }
    Ok(())
}

/// Whether `ancestor` is `genre_id` or reached from it through parents.
fn has_ancestor(db: &impl DbAccess, genre_id: DbId, ancestor: DbId) -> anyhow::Result<bool> {
    let mut seen = HashSet::from([genre_id]);
    let mut pending = vec![genre_id];
    while let Some(id) = pending.pop() {
        if id == ancestor {
            return Ok(true);
        }
        for parent in get_parents(db, id)? {
            if let Some(parent) = parent.db_id.map(DbId::from)
                && seen.insert(parent)
            {
                pending.push(parent);
            }
        }
    }
    Ok(false)
}

/// Moves a release's stored link from `loser` to `winner`, as a lock keeps it through an apply.
pub fn move_release_link(
    db: &mut impl DbAccess,
    release_id: DbId,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    link_to_release(db, winner, release_id)?;
    super::covers::display::remove_release_from_genre_covers(db, loser, release_id)?;
    super::graph::remove_edges_between(db, loser, release_id)?;
    Ok(())
}

/// Removes the genre with its external IDs, aliases and display covers.
pub fn remove_inside_tx(db: &mut impl DbAccess, genre_id: DbId) -> anyhow::Result<()> {
    external_ids::remove_all_for_owner(db, genre_id)?;
    let aliases: Vec<DbId> = get_aliases(db, genre_id)?
        .into_iter()
        .filter_map(|alias| alias.db_id.map(DbId::from))
        .collect();
    if !aliases.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(aliases).query())?;
    }
    super::covers::display::remove_genre_cover_profiles(db, genre_id)?;
    db.exec_mut(QueryBuilder::remove().ids(genre_id).query())?;
    Ok(())
}

/// Renames the genre to `name`, trimmed. An alias holding the new name goes, and when the
/// normalized name changes the old name becomes an alias.
pub fn rename(db: &mut impl DbAccess, genre_id: DbId, name: &str) -> anyhow::Result<()> {
    let genre =
        get_by_id(db, genre_id)?.ok_or_else(|| anyhow::anyhow!("genre {} missing", genre_id.0))?;
    let name = name.trim();
    let scan = normalize(name);
    let same: Vec<DbId> = get_aliases(db, genre_id)?
        .into_iter()
        .filter(|alias| normalize(alias.name.trim()) == scan)
        .filter_map(|alias| alias.db_id.map(DbId::from))
        .collect();
    if !same.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(same).query())?;
    }
    if scan != genre.scan_name {
        add_alias(db, genre_id, &genre.name, None)?;
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("name", name).into(), ("scan_name", scan).into()])
            .ids(genre_id)
            .query(),
    )?;
    Ok(())
}

pub struct ResolveGenre<'a> {
    pub name: &'a str,
    pub aliases: &'a [(&'a str, Option<&'a str>)],
    pub external_id: Option<ResolveExternalId<'a>>,
}

pub struct ResolveExternalId<'a> {
    pub provider_id: &'a str,
    pub id_type: &'a str,
    pub id_value: &'a str,
}

pub fn resolve(db: &mut DbAny, request: &ResolveGenre) -> anyhow::Result<DbId> {
    let mut genre_id = if let Some(ext) = &request.external_id {
        find_by_external_id(db, ext.provider_id, ext.id_type, ext.id_value)?
    } else {
        None
    };
    let found_by_id = genre_id.is_some();

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
        db.transaction_mut(|t| -> anyhow::Result<()> {
            // A name led here, so the genre's ID and this one both name it.
            if !found_by_id {
                external_ids::keep_replaced_inside_tx(
                    t,
                    genre_id,
                    ext.provider_id,
                    ext.id_type,
                    ext.id_value,
                )?;
            }
            external_ids::upsert_inside_tx(
                t,
                genre_id,
                ext.provider_id,
                ext.id_type,
                ext.id_value,
                super::IdSource::Resolved,
            )?;
            Ok(())
        })?;
    }

    for (alias, locale) in request.aliases {
        add_alias(db, genre_id, alias, *locale)?;
    }

    Ok(genre_id)
}

/// Like [`resolve`] but without external ID matching, so it works with any
/// `DbAccess` impl (including transaction contexts).
pub fn resolve_by_name(db: &mut impl DbAccess, name: &str) -> anyhow::Result<DbId> {
    if let Some(id) = find_by_name(db, name)? {
        return Ok(id);
    }
    if let Some(id) = find_by_alias(db, name)? {
        return Ok(id);
    }
    insert_genre(db, name)
}

#[cfg(any(test, feature = "testing"))]
pub fn sync_release_genres(
    db: &mut impl DbAccess,
    release_id: DbId,
    genre_names: &[String],
) -> anyhow::Result<()> {
    let genre_ids = resolve_names(db, genre_names)?;
    sync_release_genre_ids(db, release_id, &genre_ids)
}

/// Finds or creates the genre each name means, in order and without repeats.
pub(crate) fn resolve_names(db: &mut impl DbAccess, names: &[String]) -> anyhow::Result<Vec<DbId>> {
    let mut seen = HashSet::new();
    let mut genre_ids = Vec::with_capacity(names.len());
    for name in names {
        let genre_id = resolve_by_name(db, name)?;
        if seen.insert(genre_id) {
            genre_ids.push(genre_id);
        }
    }
    Ok(genre_ids)
}

pub fn sync_release_genre_ids(
    db: &mut impl DbAccess,
    release_id: DbId,
    genre_ids: &[DbId],
) -> anyhow::Result<()> {
    let existing = get_for_release(db, release_id)?;

    let desired_genre_ids: HashSet<DbId> = genre_ids.iter().copied().collect();
    for &genre_id in genre_ids {
        link_to_release(db, genre_id, release_id)?;
    }

    for genre in existing {
        let Some(genre_db_id) = genre.db_id.map(DbId::from) else {
            continue;
        };
        if !desired_genre_ids.contains(&genre_db_id) {
            super::covers::display::remove_release_from_genre_covers(db, genre_db_id, release_id)?;
            super::graph::remove_edges_between(db, genre_db_id, release_id)?;
        }
    }

    Ok(())
}

pub fn link_to_release(
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
    super::covers::display::ensure_genre_release_random_candidate(
        db,
        genre_id,
        release_id,
        now_ms(),
    )?;
    Ok(())
}

/// Link a child genre to a parent genre (child → parent direction).
/// Rejects self-links and direct two-node cycles (parent already points to
/// child). Does not detect transitive cycles (A → B → C → A) — callers that
/// walk ancestors must bound depth or track visited nodes.
pub fn link_to_parent(
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

pub fn get_parents(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<Genre>> {
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

pub fn get_children(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<Genre>> {
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

/// Get a single genre by its database ID.
pub fn get_by_id(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Option<Genre>> {
    super::graph::fetch_typed_by_id(db, genre_id, "Genre")
}

pub fn get_names_for_release(
    db: &impl DbAccess,
    release_id: DbId,
) -> anyhow::Result<Option<Vec<String>>> {
    let genres = get_for_release(db, release_id)?;
    if genres.is_empty() {
        return Ok(None);
    }
    Ok(Some(genres.into_iter().map(|g| g.name).collect()))
}

pub fn release_ids_matching_genre_ids(
    db: &impl DbAccess,
    genre_ids: &[DbId],
) -> anyhow::Result<HashSet<DbId>> {
    let mut release_ids = HashSet::new();
    for genre_id in super::dedup_positive_ids(genre_ids) {
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
pub fn get_releases(db: &impl DbAccess, genre_id: DbId) -> anyhow::Result<Vec<DbId>> {
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
pub fn get_releases_many(
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

pub fn get_for_release(db: &impl DbAccess, release_id: DbId) -> anyhow::Result<Vec<Genre>> {
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

pub fn get_for_releases_many(
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
            let genre_id = edge.from;
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

pub fn expand_related(
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
    use crate::test_db::new_test_db;

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

        add_alias(&mut db, genre_id, "Rock Music", None)?;
        add_alias(&mut db, genre_id, "rock music", None)?;

        assert_eq!(get_aliases(&db, genre_id)?.len(), 1);
        Ok(())
    }

    fn alias_names(db: &DbAny, genre_id: DbId) -> anyhow::Result<Vec<String>> {
        let mut names: Vec<String> = get_aliases(db, genre_id)?
            .into_iter()
            .map(|alias| alias.name)
            .collect();
        names.sort();
        Ok(names)
    }

    #[test]
    fn add_alias_skips_a_name_another_genre_goes_by() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let rock = resolve_by_name(&mut db, "Rock")?;
        let pop = resolve_by_name(&mut db, "Pop")?;
        add_alias(&mut db, pop, "Popular", None)?;

        add_alias(&mut db, rock, "pop", None)?;
        add_alias(&mut db, rock, "POPULAR", None)?;
        add_alias(&mut db, rock, "  ", None)?;
        add_alias(&mut db, rock, " Rock Music ", None)?;

        assert_eq!(alias_names(&db, rock)?, ["Rock Music"]);
        assert_eq!(alias_names(&db, pop)?, ["Popular"]);
        Ok(())
    }

    #[test]
    fn find_by_alias_passes_over_a_genre_named_the_same_with_children() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let parent = resolve_by_name(&mut db, "Rock")?;
        let child = resolve_by_name(&mut db, "Punk")?;
        link_to_parent(&mut db, child, parent)?;

        assert_eq!(find_by_alias(&db, "Rock")?, None);
        Ok(())
    }

    #[test]
    fn absorbing_moves_the_name_and_aliases_keeping_locales() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = resolve_by_name(&mut db, "J-pop")?;
        let loser = resolve_by_name(&mut db, "Jpop")?;
        let other = resolve_by_name(&mut db, "Other")?;
        add_alias(&mut db, loser, "ジェイポップ", Some("ja"))?;
        add_alias(&mut db, loser, "J-POP", None)?;
        add_alias(&mut db, other, "Taken", None)?;
        // Stored by an older build, or a resolve of the same name that came first.
        insert_alias(&mut db, loser, "taken", None)?;
        insert_alias(&mut db, loser, "  ", None)?;
        insert_alias(&mut db, loser, " Padded ", None)?;

        absorb_aliases(&mut db, winner, loser)?;

        assert_eq!(
            alias_names(&db, winner)?,
            ["Jpop", "Padded", "ジェイポップ"]
        );
        let moved = get_aliases(&db, winner)?
            .into_iter()
            .find(|alias| alias.name == "ジェイポップ")
            .expect("the alias moved");
        assert_eq!(moved.locale.as_deref(), Some("ja"));
        assert_eq!(find_by_alias(&db, " padded".trim())?, Some(winner));
        assert!(get_aliases(&db, loser)?.is_empty());
        assert_eq!(alias_names(&db, other)?, ["Taken"]);
        Ok(())
    }

    #[test]
    fn a_blank_loser_name_never_becomes_an_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = resolve_by_name(&mut db, "Soundtrack")?;
        let loser = resolve_by_name(&mut db, "   ")?;

        absorb_aliases(&mut db, winner, loser)?;

        assert!(alias_names(&db, winner)?.is_empty());
        Ok(())
    }

    #[test]
    fn removing_a_genre_leaves_nothing_of_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre = resolve(
            &mut db,
            &ResolveGenre {
                name: "Rock",
                aliases: &[("Rock Music", None)],
                external_id: Some(ResolveExternalId {
                    provider_id: "provider",
                    id_type: "genre_id",
                    id_value: "rock",
                }),
            },
        )?;
        let other = resolve_by_name(&mut db, "Other")?;
        external_ids::absorb_inside_tx(&mut db, genre, other)?;
        let nodes = |db: &DbAny, kind: &str| -> anyhow::Result<usize> {
            Ok(db
                .exec(
                    QueryBuilder::search()
                        .elements()
                        .where_()
                        .key("db_element_id")
                        .value(kind)
                        .query(),
                )?
                .elements
                .len())
        };

        remove_inside_tx(&mut db, genre)?;

        assert!(get_by_id(&db, genre)?.is_none());
        assert_eq!(nodes(&db, "GenreAlias")?, 0);
        assert_eq!(nodes(&db, "ExternalId")?, 0);
        assert_eq!(find_by_alias(&db, "Rock Music")?, None);
        assert_eq!(
            super::super::roots::member_ids(&db, super::super::roots::Root::Genres)?,
            [other]
        );
        Ok(())
    }

    fn parents_of(db: &DbAny, genre: DbId) -> anyhow::Result<Vec<DbId>> {
        let mut ids: Vec<DbId> = get_parents(db, genre)?
            .into_iter()
            .filter_map(|genre| genre.db_id.map(DbId::from))
            .collect();
        ids.sort_by_key(|id| id.0);
        Ok(ids)
    }

    #[test]
    fn relatives_move_to_the_winner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let [winner, loser, parent, child] =
            ["W", "L", "P", "C"].map(|name| resolve_by_name(&mut db, name).expect("genre"));
        link_to_parent(&mut db, loser, parent)?;
        link_to_parent(&mut db, child, loser)?;

        absorb_relatives(&mut db, winner, loser)?;

        assert_eq!(parents_of(&db, winner)?, [parent]);
        assert_eq!(parents_of(&db, child)?, [winner]);
        assert!(parents_of(&db, loser)?.is_empty());
        Ok(())
    }

    #[test]
    fn merging_into_a_parent_or_child_leaves_no_self_link() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let [parent, child, grandparent] = ["Parent", "Child", "Grandparent"]
            .map(|name| resolve_by_name(&mut db, name).expect("genre"));
        link_to_parent(&mut db, child, parent)?;
        link_to_parent(&mut db, parent, grandparent)?;

        // The child absorbs its own parent, so it moves up a level.
        absorb_relatives(&mut db, child, parent)?;
        assert_eq!(parents_of(&db, child)?, [grandparent]);

        // The grandparent absorbs its own child.
        absorb_relatives(&mut db, grandparent, child)?;
        assert!(parents_of(&db, grandparent)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_move_that_would_close_a_cycle_is_dropped() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let [winner, middle, top, loser] =
            ["W", "M", "T", "L"].map(|name| resolve_by_name(&mut db, name).expect("genre"));
        // W → M → T, and T is a child of the loser, whose move would give T → W.
        link_to_parent(&mut db, winner, middle)?;
        link_to_parent(&mut db, middle, top)?;
        link_to_parent(&mut db, top, loser)?;

        absorb_relatives(&mut db, winner, loser)?;

        assert_eq!(parents_of(&db, winner)?, [middle]);
        assert_eq!(parents_of(&db, middle)?, [top]);
        assert!(parents_of(&db, top)?.is_empty());

        // And from the other side: the loser's parent is below the winner.
        let [winner, below, loser] =
            ["W2", "B2", "L2"].map(|name| resolve_by_name(&mut db, name).expect("genre"));
        link_to_parent(&mut db, below, winner)?;
        link_to_parent(&mut db, loser, below)?;

        absorb_relatives(&mut db, winner, loser)?;

        assert!(parents_of(&db, winner)?.is_empty());
        assert_eq!(parents_of(&db, below)?, [winner]);
        Ok(())
    }

    #[test]
    fn a_stored_three_genre_cycle_loses_the_merged_link() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let [a, b, c] = ["A", "B", "C"].map(|name| resolve_by_name(&mut db, name).expect("genre"));
        link_to_parent(&mut db, a, b)?;
        link_to_parent(&mut db, b, c)?;
        link_to_parent(&mut db, c, a)?;

        absorb_relatives(&mut db, a, c)?;

        assert_eq!(parents_of(&db, a)?, [b]);
        assert!(parents_of(&db, b)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_replaced_id_reached_by_name_stays_an_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let request = |value| ResolveGenre {
            name: "Rock",
            aliases: &[],
            external_id: Some(ResolveExternalId {
                provider_id: "provider",
                id_type: "genre_id",
                id_value: value,
            }),
        };
        let genre = resolve(&mut db, &request("first"))?;

        assert_eq!(resolve(&mut db, &request("second"))?, genre);

        for value in ["first", "second"] {
            assert_eq!(
                find_by_external_id(&db, "provider", "genre_id", value)?,
                Some(genre)
            );
        }
        assert_eq!(
            external_ids::get(&db, genre, "provider", "genre_id")?
                .map(|id| id.id_value)
                .as_deref(),
            Some("second")
        );
        Ok(())
    }

    #[test]
    fn renaming_keeps_the_old_name_as_an_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre = resolve_by_name(&mut db, "Hip-Hop/Rap")?;
        add_alias(&mut db, genre, "hip hop", None)?;

        rename(&mut db, genre, "  Hip Hop ")?;

        assert_eq!(get_by_id(&db, genre)?.expect("genre").name, "Hip Hop");
        assert_eq!(find_by_name(&db, "hip hop")?, Some(genre));
        assert_eq!(alias_names(&db, genre)?, ["Hip-Hop/Rap"]);
        assert_eq!(resolve_by_name(&mut db, "Hip-Hop/Rap")?, genre);
        Ok(())
    }

    #[test]
    fn a_case_change_adds_no_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre = resolve_by_name(&mut db, "j-pop")?;

        rename(&mut db, genre, "J-Pop")?;

        assert_eq!(get_by_id(&db, genre)?.expect("genre").name, "J-Pop");
        assert!(alias_names(&db, genre)?.is_empty());
        Ok(())
    }

    #[test]
    fn a_case_change_drops_an_alias_matching_the_new_name() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre = resolve_by_name(&mut db, "Rock")?;
        add_alias(&mut db, genre, "rock", None)?;
        add_alias(&mut db, genre, "Rock Music", None)?;

        rename(&mut db, genre, "ROCK")?;

        assert_eq!(get_by_id(&db, genre)?.expect("genre").name, "ROCK");
        assert_eq!(alias_names(&db, genre)?, ["Rock Music"]);
        Ok(())
    }

    #[test]
    fn name_taken_sees_names_and_aliases_of_other_genres_only() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let genre = resolve_by_name(&mut db, "Rock")?;
        let other = resolve_by_name(&mut db, "Pop")?;
        add_alias(&mut db, other, "Popular", None)?;
        add_alias(&mut db, genre, "Rock Music", None)?;

        assert!(name_taken(&db, " POP ", genre)?);
        assert!(name_taken(&db, "popular", genre)?);
        assert!(!name_taken(&db, "rock music", genre)?);
        assert!(!name_taken(&db, "ROCK", genre)?);
        assert!(!name_taken(&db, "Free", genre)?);
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
        let release_id = crate::fixtures::insert_release(&mut db, "Blue Train")?;

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
        let release_id = crate::fixtures::insert_release(&mut db, "Crossroads")?;

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
        let release_id = crate::fixtures::insert_release(&mut db, "Shifting Genres")?;

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
        let release_id = crate::fixtures::insert_release(&mut db, "Gone")?;

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
        let release_id = crate::fixtures::insert_release(&mut db, "OK Computer")?;

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
