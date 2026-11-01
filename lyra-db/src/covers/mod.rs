// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod display;

use super::DbAccess;
use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};
use serde::Serialize;
use std::collections::HashMap;

/// A cover image. The owner's edge to it says which source it came from, and a node can have
/// several owners after duplicate entities merge.
#[derive(DbElement, Serialize, Clone, Debug)]
pub struct Cover {
    #[serde(skip_serializing)]
    pub db_id: Option<DbId>,
    pub id: String,
    pub path: String,
    pub mime_type: String,
    pub hash: String,
    pub blurhash: Option<String>,
    /// The provider whose cover handler supplied the image, for provider covers.
    pub provider_id: Option<String>,
}

/// Where an owner's cover came from. An owner keeps at most one cover per source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CoverSource {
    /// An image beside the audio files in the library.
    Local,
    /// An image a provider's cover handler supplied.
    Provider,
    /// An image a user uploaded.
    Manual,
}

impl CoverSource {
    /// The order the shown cover is picked in when the owner pins none.
    pub const PRECEDENCE: [CoverSource; 3] = [Self::Manual, Self::Provider, Self::Local];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Provider => "provider",
            Self::Manual => "manual",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "local" => Some(Self::Local),
            "provider" => Some(Self::Provider),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }
}

const OWNED_KEY: &str = "owned";
const SOURCE_KEY: &str = "cover_source";
const SELECTED_KEY: &str = "cover_selected";

/// One of an owner's stored covers.
#[derive(Clone, Debug)]
pub struct SourcedCover {
    pub source: CoverSource,
    /// Whether the owner pinned this source as the one to show.
    pub selected: bool,
    pub cover: Cover,
}

struct SourceEdge {
    edge_id: DbId,
    cover_db_id: DbId,
    source: CoverSource,
    selected: bool,
}

fn source_edges(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Vec<SourceEdge>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(owner_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .and()
            .keys([SOURCE_KEY])
            .query(),
    )?;
    let mut edges = Vec::new();
    for element in result.elements {
        if element.from != owner_db_id {
            continue;
        }
        let mut source = None;
        let mut selected = false;
        for kv in &element.values {
            match kv.key.string().map(String::as_str) {
                Ok(SOURCE_KEY) => {
                    let raw = kv.value.string()?;
                    source = Some(
                        CoverSource::parse(raw)
                            .ok_or_else(|| anyhow::anyhow!("unknown cover source '{raw}'"))?,
                    );
                }
                Ok(SELECTED_KEY) => selected = kv.value.to_u64().is_ok_and(|value| value != 0),
                _ => {}
            }
        }
        let Some(source) = source else {
            continue;
        };
        edges.push(SourceEdge {
            edge_id: element.id,
            cover_db_id: element.to,
            source,
            selected,
        });
    }
    Ok(edges)
}

fn cover_by_db_id(db: &impl DbAccess, cover_db_id: DbId) -> anyhow::Result<Option<Cover>> {
    super::graph::fetch_typed_by_id(db, cover_db_id, "Cover")
}

/// The owner's stored covers, in automatic precedence order.
pub fn sources(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Vec<SourcedCover>> {
    let mut covers = Vec::new();
    for edge in source_edges(db, owner_db_id)? {
        let Some(cover) = cover_by_db_id(db, edge.cover_db_id)? else {
            continue;
        };
        covers.push(SourcedCover {
            source: edge.source,
            selected: edge.selected,
            cover,
        });
    }
    covers.sort_by_key(|covered| {
        CoverSource::PRECEDENCE
            .iter()
            .position(|source| *source == covered.source)
    });
    Ok(covers)
}

/// The owner's cover from `source`.
pub fn get_source(
    db: &impl DbAccess,
    owner_db_id: DbId,
    source: CoverSource,
) -> anyhow::Result<Option<Cover>> {
    let Some(edge) = source_edges(db, owner_db_id)?
        .into_iter()
        .find(|edge| edge.source == source)
    else {
        return Ok(None);
    };
    cover_by_db_id(db, edge.cover_db_id)
}

/// The source the owner pinned, if any.
pub fn selection(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Option<CoverSource>> {
    Ok(source_edges(db, owner_db_id)?
        .into_iter()
        .find(|edge| edge.selected)
        .map(|edge| edge.source))
}

/// The cover the owner shows: its pinned source's, else the first source in precedence order.
pub fn get(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<Option<Cover>> {
    let edges = source_edges(db, owner_db_id)?;
    let shown = edges.iter().find(|edge| edge.selected).or_else(|| {
        CoverSource::PRECEDENCE
            .iter()
            .find_map(|source| edges.iter().find(|edge| edge.source == *source))
    });
    match shown {
        Some(edge) => cover_by_db_id(db, edge.cover_db_id),
        None => Ok(None),
    }
}

pub fn get_by_public_id(db: &impl DbAccess, id: &str) -> anyhow::Result<Option<Cover>> {
    super::graph::fetch_typed_by_index(db, "id", id, "Cover")
}

pub fn get_many(
    db: &impl super::DbAccess,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Cover>> {
    let unique_owner_db_ids = super::dedup_positive_ids(owner_db_ids);

    if unique_owner_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut covers = HashMap::new();
    for owner_db_id in unique_owner_db_ids {
        if let Some(cover) = get(db, owner_db_id)? {
            covers.insert(owner_db_id, cover);
        }
    }
    Ok(covers)
}

/// Pins `source` as the cover the owner shows, or clears the pin with `None`. Returns false,
/// changing nothing, when the owner has no cover from `source`.
pub fn select(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    source: Option<CoverSource>,
) -> anyhow::Result<bool> {
    let edges = source_edges(db, owner_db_id)?;
    if let Some(source) = source
        && !edges.iter().any(|edge| edge.source == source)
    {
        return Ok(false);
    }
    for edge in edges {
        let pin = Some(edge.source) == source;
        if pin == edge.selected {
            continue;
        }
        if pin {
            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([(SELECTED_KEY, 1).into()])
                    .ids(edge.edge_id)
                    .query(),
            )?;
        } else {
            db.exec_mut(
                QueryBuilder::remove()
                    .values([DbValue::from(SELECTED_KEY)])
                    .ids(edge.edge_id)
                    .query(),
            )?;
        }
    }
    Ok(true)
}

fn owner_count(db: &impl DbAccess, cover_db_id: DbId) -> anyhow::Result<usize> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .to(cover_db_id)
                .where_()
                .edge()
                .and()
                .distance(CountComparison::Equal(1))
                .and()
                .key(OWNED_KEY)
                .value(1)
                .query(),
        )?
        .elements
        .len())
}

fn link(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    cover_db_id: DbId,
    source: CoverSource,
    selected: bool,
) -> anyhow::Result<()> {
    let mut values = vec![(OWNED_KEY, 1).into(), (SOURCE_KEY, source.as_str()).into()];
    if selected {
        values.push((SELECTED_KEY, 1).into());
    }
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(owner_db_id)
            .to(cover_db_id)
            .values_uniform(values)
            .query(),
    )?;
    Ok(())
}

/// Links an existing cover node to another owner as its `source` cover, sharing the node, when
/// the owner has no cover from that source yet. Returns whether it linked.
pub fn share(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    source: CoverSource,
    cover_db_id: DbId,
    selected: bool,
) -> anyhow::Result<bool> {
    let had_cover = !source_edges(db, owner_db_id)?.is_empty();
    if get_source(db, owner_db_id, source)?.is_some() {
        return Ok(false);
    }
    let selected = selected && selection(db, owner_db_id)?.is_none();
    link(db, owner_db_id, cover_db_id, source, selected)?;
    if !had_cover {
        cover_gained(db, owner_db_id)?;
    }
    Ok(true)
}

fn is_release(db: &impl DbAccess, owner_db_id: DbId) -> anyhow::Result<bool> {
    super::graph::is_kind(db, owner_db_id, "Release")
}

fn cover_gained(db: &mut impl DbAccess, owner_db_id: DbId) -> anyhow::Result<()> {
    if is_release(db, owner_db_id)? {
        display::sync_release_random_candidates(db, owner_db_id)?;
        display::offer_release_to_playlist_covers(db, owner_db_id)?;
    }
    Ok(())
}

fn cover_lost(db: &mut impl DbAccess, owner_db_id: DbId) -> anyhow::Result<()> {
    if is_release(db, owner_db_id)? {
        display::mark_genre_profiles_dirty_for_release(db, owner_db_id)?;
        display::resync_playlist_covers_for_release(db, owner_db_id)?;
    }
    Ok(())
}

/// A cover that a write took away from its owner.
#[derive(Clone, Debug)]
pub struct DetachedCover {
    pub cover: Cover,
    /// Whether no owner refers to the cover's file anymore, so it may be discarded.
    pub orphaned: bool,
}

/// Removes the owner's cover from `source`. The node goes too unless another owner shares it.
pub fn remove(
    db: &mut impl DbAccess,
    owner_db_id: DbId,
    source: CoverSource,
) -> anyhow::Result<Option<DetachedCover>> {
    let edges = source_edges(db, owner_db_id)?;
    let Some(edge) = edges.iter().find(|edge| edge.source == source) else {
        return Ok(None);
    };
    let Some(cover) = cover_by_db_id(db, edge.cover_db_id)? else {
        return Ok(None);
    };
    db.exec_mut(QueryBuilder::remove().ids(edge.edge_id).query())?;
    let orphaned = owner_count(db, edge.cover_db_id)? == 0;
    if orphaned {
        db.exec_mut(QueryBuilder::remove().ids(edge.cover_db_id).query())?;
    }
    if edges.len() == 1 {
        cover_lost(db, owner_db_id)?;
    }
    Ok(Some(DetachedCover { cover, orphaned }))
}

/// The owner's cover from `source` after an upsert, and the cover it replaced.
#[derive(Clone, Debug)]
pub struct UpsertedCover {
    pub cover: Cover,
    /// The previous cover from the source, when the upsert left its file unreferenced by this
    /// owner.
    pub replaced: Option<DetachedCover>,
}

/// Records `cover` as the owner's cover from `source`, keeping whether that source is pinned.
/// A node the owner alone holds is updated in place and keeps its public id; a shared one is
/// left to its other owners and the owner gets a node of its own.
pub fn upsert(
    db: &mut DbAnyTransactionMut<'_>,
    owner_db_id: DbId,
    source: CoverSource,
    mut cover: Cover,
) -> anyhow::Result<UpsertedCover> {
    let edges = source_edges(db, owner_db_id)?;
    let existing = edges.iter().find(|edge| edge.source == source);
    let mut replaced = None;
    match existing {
        Some(edge) if owner_count(db, edge.cover_db_id)? == 1 => {
            let previous = cover_by_db_id(db, edge.cover_db_id)?;
            cover.db_id = Some(edge.cover_db_id);
            if let Some(previous) = &previous {
                cover.id.clone_from(&previous.id);
            }
            super::replace_element_in_transaction(
                db,
                edge.cover_db_id,
                [
                    ("blurhash", cover.blurhash.is_none()),
                    ("provider_id", cover.provider_id.is_none()),
                ],
                &cover,
            )?;
            replaced = previous
                .filter(|previous| previous.path != cover.path)
                .map(|cover| DetachedCover {
                    cover,
                    orphaned: true,
                });
        }
        Some(edge) => {
            let previous = cover_by_db_id(db, edge.cover_db_id)?;
            db.exec_mut(QueryBuilder::remove().ids(edge.edge_id).query())?;
            cover.db_id = Some(insert_node(db, &cover)?);
            link(
                db,
                owner_db_id,
                cover.db_id.expect("inserted"),
                source,
                edge.selected,
            )?;
            replaced = previous.map(|cover| DetachedCover {
                cover,
                orphaned: false,
            });
        }
        None => {
            cover.db_id = Some(insert_node(db, &cover)?);
            link(
                db,
                owner_db_id,
                cover.db_id.expect("inserted"),
                source,
                false,
            )?;
        }
    }

    cover_gained(db, owner_db_id)?;
    Ok(UpsertedCover { cover, replaced })
}

fn insert_node(db: &mut DbAnyTransactionMut<'_>, cover: &Cover) -> anyhow::Result<DbId> {
    let mut cover = cover.clone();
    cover.db_id = None;
    db.exec_mut(QueryBuilder::insert().element(&cover).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("cover insert returned no id"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TestDb;
    use agdb::DbAny;
    use anyhow::anyhow;

    fn new_test_db() -> anyhow::Result<DbAny> {
        Ok(TestDb::initialized()?.into_inner())
    }

    fn cover(id: &str, path: &str) -> Cover {
        Cover {
            db_id: None,
            id: id.to_string(),
            path: path.to_string(),
            mime_type: "image/jpeg".to_string(),
            hash: format!("{id}-hash"),
            blurhash: None,
            provider_id: None,
        }
    }

    fn upsert_cover(
        db: &mut DbAny,
        owner_db_id: DbId,
        source: CoverSource,
        cover: Cover,
    ) -> anyhow::Result<UpsertedCover> {
        db.transaction_mut(|t| upsert(t, owner_db_id, source, cover))
    }

    fn insert_owner(db: &mut DbAny) -> anyhow::Result<DbId> {
        let qr = db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        qr.ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow!("owner insert returned no id"))
    }

    fn shown_id(db: &DbAny, owner_db_id: DbId) -> anyhow::Result<Option<String>> {
        Ok(get(db, owner_db_id)?.map(|cover| cover.id))
    }

    #[test]
    fn upsert_new_cover_links_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;

        let upserted = upsert_cover(
            &mut db,
            owner,
            CoverSource::Local,
            cover("local", "/music/cover.jpg"),
        )?;
        let cover_id = upserted
            .cover
            .db_id
            .ok_or_else(|| anyhow!("upsert did not set db_id"))?;

        assert!(crate::graph::edge_exists(&db, owner, cover_id)?);
        assert!(upserted.replaced.is_none());
        Ok(())
    }

    #[test]
    fn get_ignores_covers_the_owner_does_not_own() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        let other = insert_owner(&mut db)?;
        let cover_id = upsert_cover(
            &mut db,
            owner,
            CoverSource::Local,
            cover("owned", "/music/cover.jpg"),
        )?
        .cover
        .db_id
        .ok_or_else(|| anyhow!("upsert did not set db_id"))?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(other)
                .to(cover_id)
                .query(),
        )?;

        assert!(get(&db, owner)?.is_some());
        assert!(get(&db, other)?.is_none());
        Ok(())
    }

    #[test]
    fn get_by_public_id_returns_cover() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        upsert_cover(
            &mut db,
            owner,
            CoverSource::Local,
            cover("cover-public", "/music/cover.jpg"),
        )?;

        let found = get_by_public_id(&db, "cover-public")?
            .ok_or_else(|| anyhow!("cover should be found by public id"))?;
        assert_eq!(found.id, "cover-public");
        Ok(())
    }

    #[test]
    fn upsert_clears_a_dropped_blurhash() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        let mut first = cover("local", "/music/cover.jpg");
        first.blurhash = Some("LKO2?U%2Tw=w]~RBVZRi};RPxuwH".to_string());
        upsert_cover(&mut db, owner, CoverSource::Local, first)?;

        let mut replacement = cover("local", "/music/cover.jpg");
        replacement.hash = "b".repeat(64);
        upsert_cover(&mut db, owner, CoverSource::Local, replacement)?;

        let stored = get(&db, owner)?.ok_or_else(|| anyhow!("cover should exist"))?;
        assert_eq!(
            stored.blurhash, None,
            "a blurhash that could not be recomputed must not describe the new artwork"
        );
        Ok(())
    }

    #[test]
    fn upsert_updates_a_sources_node_in_place() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        let first = upsert_cover(
            &mut db,
            owner,
            CoverSource::Provider,
            cover("first", "/covers/1/provider-a.jpg"),
        )?;
        let second = upsert_cover(
            &mut db,
            owner,
            CoverSource::Provider,
            cover("second", "/covers/1/provider-b.jpg"),
        )?;

        assert_eq!(first.cover.db_id, second.cover.db_id);
        assert_eq!(second.cover.id, "first", "the public id stays the same");
        let replaced = second
            .replaced
            .ok_or_else(|| anyhow!("the previous file is reported"))?;
        assert_eq!(replaced.cover.path, "/covers/1/provider-a.jpg");
        assert!(replaced.orphaned);
        assert_eq!(sources(&db, owner)?.len(), 1);
        Ok(())
    }

    #[test]
    fn sources_are_kept_apart_and_shown_by_precedence() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        upsert_cover(
            &mut db,
            owner,
            CoverSource::Local,
            cover("local", "/music/cover.jpg"),
        )?;
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("local"));

        upsert_cover(
            &mut db,
            owner,
            CoverSource::Provider,
            cover("provider", "/covers/1/provider.jpg"),
        )?;
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("provider"));

        upsert_cover(
            &mut db,
            owner,
            CoverSource::Manual,
            cover("manual", "/covers/1/manual.jpg"),
        )?;
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("manual"));
        assert_eq!(
            sources(&db, owner)?
                .into_iter()
                .map(|sourced| sourced.source)
                .collect::<Vec<_>>(),
            CoverSource::PRECEDENCE.to_vec()
        );

        remove(&mut db, owner, CoverSource::Manual)?;
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("provider"));
        Ok(())
    }

    #[test]
    fn a_pin_overrides_precedence_until_cleared_or_its_cover_goes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        upsert_cover(
            &mut db,
            owner,
            CoverSource::Local,
            cover("local", "/music/cover.jpg"),
        )?;
        upsert_cover(
            &mut db,
            owner,
            CoverSource::Manual,
            cover("manual", "/covers/1/manual.jpg"),
        )?;

        assert!(!select(&mut db, owner, Some(CoverSource::Provider))?);
        assert_eq!(selection(&db, owner)?, None);

        assert!(select(&mut db, owner, Some(CoverSource::Local))?);
        assert_eq!(selection(&db, owner)?, Some(CoverSource::Local));
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("local"));

        assert!(select(&mut db, owner, Some(CoverSource::Manual))?);
        assert_eq!(selection(&db, owner)?, Some(CoverSource::Manual));
        upsert_cover(
            &mut db,
            owner,
            CoverSource::Manual,
            cover("manual-2", "/covers/1/manual-2.jpg"),
        )?;
        assert_eq!(
            selection(&db, owner)?,
            Some(CoverSource::Manual),
            "replacing a pinned cover keeps the pin"
        );

        remove(&mut db, owner, CoverSource::Manual)?;
        assert_eq!(selection(&db, owner)?, None);
        assert_eq!(shown_id(&db, owner)?.as_deref(), Some("local"));

        assert!(select(&mut db, owner, Some(CoverSource::Local))?);
        assert!(select(&mut db, owner, None)?);
        assert_eq!(selection(&db, owner)?, None);
        Ok(())
    }

    #[test]
    fn a_shared_node_survives_one_owner_and_is_not_rewritten() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = insert_owner(&mut db)?;
        let second = insert_owner(&mut db)?;
        let shared = upsert_cover(
            &mut db,
            first,
            CoverSource::Provider,
            cover("shared", "/covers/1/provider.jpg"),
        )?
        .cover;
        let shared_db_id = shared.db_id.ok_or_else(|| anyhow!("db_id"))?;
        assert!(share(
            &mut db,
            second,
            CoverSource::Provider,
            shared_db_id,
            false
        )?);

        let rewritten = upsert_cover(
            &mut db,
            second,
            CoverSource::Provider,
            cover("own", "/covers/2/provider.jpg"),
        )?;
        assert_ne!(rewritten.cover.db_id, Some(shared_db_id));
        let replaced = rewritten
            .replaced
            .ok_or_else(|| anyhow!("the shared cover is reported"))?;
        assert!(!replaced.orphaned, "the first owner still uses the file");
        assert_eq!(shown_id(&db, first)?.as_deref(), Some("shared"));
        assert_eq!(shown_id(&db, second)?.as_deref(), Some("own"));

        assert!(share(
            &mut db,
            second,
            CoverSource::Local,
            shared_db_id,
            false
        )?);
        let detached =
            remove(&mut db, second, CoverSource::Local)?.ok_or_else(|| anyhow!("removed"))?;
        assert!(!detached.orphaned);
        assert!(get_by_public_id(&db, "shared")?.is_some());

        let detached =
            remove(&mut db, first, CoverSource::Provider)?.ok_or_else(|| anyhow!("removed"))?;
        assert!(detached.orphaned);
        assert!(get_by_public_id(&db, "shared")?.is_none());
        Ok(())
    }

    #[test]
    fn edges_without_a_source_are_not_covers() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_owner(&mut db)?;
        let node = db
            .exec_mut(
                QueryBuilder::insert()
                    .element(cover("unsourced", "/covers/1/cover.jpg"))
                    .query(),
            )?
            .ids()[0];
        crate::graph::ensure_owned_edge(&mut db, owner, node)?;

        assert!(get(&db, owner)?.is_none());
        assert!(sources(&db, owner)?.is_empty());
        Ok(())
    }
}
