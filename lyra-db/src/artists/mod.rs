// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub mod relations;

use std::cmp::Ordering;
use std::collections::{
    HashMap,
    HashSet,
};
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use crate::nanoid;
use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use anyhow::anyhow;
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;
use super::roots::{
    self,
    Root,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
pub enum ArtistType {
    #[default]
    Person,
    Group,
    Character,
    Orchestra,
    Choir,
}

impl ArtistType {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Group => "group",
            Self::Character => "character",
            Self::Orchestra => "orchestra",
            Self::Choir => "choir",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "person" => Ok(Self::Person),
            "group" => Ok(Self::Group),
            "character" => Ok(Self::Character),
            "orchestra" => Ok(Self::Orchestra),
            "choir" => Ok(Self::Choir),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid ArtistType value '{value}'"),
            )),
        }
    }
}

impl std::fmt::Display for ArtistType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<ArtistType> for DbValue {
    fn from(value: ArtistType) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&ArtistType> for DbValue {
    fn from(value: &ArtistType) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for ArtistType {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

use super::{
    Credit,
    compare_option,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub struct Artist {
    #[serde(skip_serializing)]
    pub db_id: Option<NodeId>,
    pub id: String,
    pub artist_name: String,
    pub scan_name: String,
    /// Scan names of duplicates merged into this artist, which keep resolving to it.
    #[serde(skip)]
    pub scan_aliases: Vec<String>,
    pub sort_name: Option<String>,
    pub artist_type: Option<ArtistType>,
    pub description: Option<String>,
    /// Whether a release or track credits the artist; only credited artists are browsed.
    #[serde(skip)]
    pub credited: bool,
    pub locked: Option<bool>,
    pub created_at_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct CreditedArtist {
    pub artist: Artist,
    pub credit: Credit,
}

impl Artist {
    pub fn set_artist_name(&mut self, artist_name: String) {
        self.artist_name = artist_name;
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_artist_type(&mut self, artist_type: ArtistType) {
        self.artist_type = Some(artist_type);
    }
}

pub fn get(db: &impl super::DbAccess, owner_db_id: DbId) -> anyhow::Result<Vec<Artist>> {
    let mut artists: Vec<Artist> = db
        .exec(
            QueryBuilder::select()
                .elements::<Artist>()
                .search()
                .from(owner_db_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    sort_artists_for_owner(db, Some(owner_db_id).filter(|id| id.0 > 0), &mut artists)?;
    Ok(artists)
}

/// The artists credited on the library's releases or on their tracks, once each.
pub fn get_by_library(db: &DbAny, library_id: DbId) -> anyhow::Result<Vec<Artist>> {
    let release_ids: Vec<DbId> = super::releases::get_direct(db, library_id)?
        .into_iter()
        .filter_map(|release| release.db_id.map(Into::into))
        .collect();
    get_by_releases(db, &release_ids)
}

/// The artists credited on the releases or on their tracks, once each: each release's own
/// artists in credit order, then its tracks' artists not credited before.
pub fn get_by_releases(db: &DbAny, release_db_ids: &[DbId]) -> anyhow::Result<Vec<Artist>> {
    let release_db_ids = super::dedup_positive_ids(release_db_ids);
    let mut tracks_by_release = super::tracks::get_direct_many(db, &release_db_ids)?;
    let mut owners = Vec::new();
    for release_db_id in &release_db_ids {
        owners.push(*release_db_id);
        owners.extend(
            tracks_by_release
                .remove(release_db_id)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|track| track.db_id.map(DbId::from)),
        );
    }
    let mut artists_by_owner = get_many_by_owner(db, &owners)?;
    let mut seen = HashSet::new();
    let mut artists = Vec::new();
    for owner in owners {
        for artist in artists_by_owner.remove(&owner).unwrap_or_default() {
            if artist
                .db_id
                .clone()
                .is_some_and(|id| seen.insert(DbId::from(id)))
            {
                artists.push(artist);
            }
        }
    }
    Ok(artists)
}

/// Each owner's credited artists, once each, in credit order.
pub fn get_many_by_owner(
    db: &DbAny,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    Ok(get_credited_many_by_owner(db, owner_db_ids)?
        .into_iter()
        .map(|(owner_db_id, credited)| {
            let mut seen = std::collections::HashSet::new();
            let artists = credited
                .into_iter()
                .map(|credited| credited.artist)
                .filter(|artist| {
                    artist
                        .db_id
                        .clone()
                        .is_some_and(|id| seen.insert(DbId::from(id)))
                })
                .collect();
            (owner_db_id, artists)
        })
        .collect())
}

pub fn get_credited(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Vec<CreditedArtist>> {
    Ok(get_credited_many_by_owner(db, &[owner_db_id])?
        .remove(&owner_db_id)
        .unwrap_or_default())
}

pub fn get_credited_many_by_owner(
    db: &impl super::DbAccess,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<CreditedArtist>>> {
    let mut links_by_owner = HashMap::new();
    let mut artist_ids = Vec::new();
    for owner_db_id in super::dedup_positive_ids(owner_db_ids) {
        let links = super::credits::links_for_owner(db, owner_db_id)?;
        artist_ids.extend(links.iter().map(|link| link.artist_id));
        links_by_owner.insert(owner_db_id, links);
    }
    let artists_by_id: HashMap<DbId, Artist> =
        super::graph::bulk_fetch_typed(db, super::dedup_positive_ids(&artist_ids), "Artist")?;

    links_by_owner
        .into_iter()
        .map(|(owner_db_id, links)| {
            let mut credited = links
                .into_iter()
                .map(|link| {
                    let artist = artists_by_id.get(&link.artist_id).cloned().ok_or_else(|| {
                        anyhow!(
                            "credit {} on owner {} targets non-artist {}",
                            link.edge_id.0,
                            owner_db_id.0,
                            link.artist_id.0
                        )
                    })?;
                    Ok(CreditedArtist {
                        artist,
                        credit: link.credit,
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            sort_credited_artists(&mut credited);
            Ok((owner_db_id, credited))
        })
        .collect()
}

/// Every artist of `artist_type`, credited or not.
pub fn ids_with_type(
    db: &impl super::DbAccess,
    artist_type: ArtistType,
) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .from(Root::Artists.alias())
                .where_()
                .distance(roots::MEMBERS)
                .and()
                .key("artist_type")
                .value(DbValue::from(artist_type))
                .query(),
        )?
        .ids())
}

/// The artists any path of outgoing edges reaches from a release or track.
pub fn reachable_from_catalog(db: &impl super::DbAccess) -> anyhow::Result<HashSet<DbId>> {
    let mut reached = HashSet::new();
    for root in [Root::Releases, Root::Tracks] {
        reached.extend(
            db.exec(
                QueryBuilder::search()
                    .from(root.alias())
                    .where_()
                    .key("db_element_id")
                    .value("Artist")
                    .query(),
            )?
            .ids(),
        );
    }
    Ok(reached)
}

/// The credited artists, of `artist_type` when given.
pub fn credited_ids(
    db: &impl super::DbAccess,
    artist_type: Option<ArtistType>,
) -> anyhow::Result<Vec<DbId>> {
    let mut query = QueryBuilder::search()
        .from(Root::Artists.alias())
        .where_()
        .distance(roots::MEMBERS)
        .and()
        .key(super::credits::CREDITED)
        .value(true);
    if let Some(artist_type) = artist_type {
        query = query
            .and()
            .key("artist_type")
            .value(DbValue::from(artist_type));
    }
    Ok(db.exec(query.query())?.ids())
}

/// The name of each owner's first credited artist, in credit order.
pub fn first_credited_names(
    db: &impl super::DbAccess,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, String>> {
    let mut leading = HashMap::new();
    for owner_db_id in super::dedup_positive_ids(owner_db_ids) {
        let links = super::credits::links_for_owner(db, owner_db_id)?;
        let Some(first_order) = links.iter().map(|link| link.credit.artist_order).min() else {
            continue;
        };
        let artists = links
            .into_iter()
            .filter(|link| link.credit.artist_order == first_order)
            .map(|link| link.artist_id)
            .collect::<Vec<_>>();
        leading.insert(owner_db_id, artists);
    }

    let artist_ids =
        super::dedup_positive_ids(&leading.values().flatten().copied().collect::<Vec<_>>());
    let names = super::graph::select_fields(db, &artist_ids, &["artist_name", "sort_name"])?;
    let sort_key = |artist_id: &DbId| {
        let fields = names.get(artist_id)?;
        let name = fields.text("artist_name")?;
        Some((
            fields.text("sort_name").unwrap_or(name).to_lowercase(),
            name.to_lowercase(),
            artist_id.0,
            name.to_string(),
        ))
    };
    Ok(leading
        .into_iter()
        .filter_map(|(owner_db_id, artists)| {
            let (_, _, _, name) = artists.iter().filter_map(sort_key).min()?;
            Some((owner_db_id, name))
        })
        .collect())
}

pub fn get_by_id(db: &impl super::DbAccess, artist_db_id: DbId) -> anyhow::Result<Option<Artist>> {
    super::graph::fetch_typed_by_id(db, artist_db_id, "Artist")
}

/// Indexed marker on artists that hold scan aliases, so alias lookups visit only them.
pub const SCAN_ALIAS_HOLDER: &str = "scan_alias_holder";

/// Every artist whose scan name is exactly `scan_name`; when none is, the one artist holding it as
/// a scan alias.
pub fn with_scan_name(db: &impl super::DbAccess, scan_name: &str) -> anyhow::Result<Vec<Artist>> {
    let ids = scan_name_ids(db, scan_name)?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(db
        .exec(QueryBuilder::select().ids(ids).query())?
        .try_into()?)
}

/// The lowest-id artist [`with_scan_name`] resolves `scan_name` to.
pub fn find_id_by_scan_name(
    db: &impl super::DbAccess,
    scan_name: &str,
) -> anyhow::Result<Option<DbId>> {
    Ok(scan_name_ids(db, scan_name)?
        .into_iter()
        .min_by_key(|id| id.0))
}

fn scan_name_ids(db: &impl super::DbAccess, scan_name: &str) -> anyhow::Result<Vec<DbId>> {
    let holders = scan_name_holders(db, scan_name)?;
    if !holders.is_empty() {
        return Ok(holders);
    }
    Ok(sole(scan_alias_holders(db, scan_name)?)
        .into_iter()
        .collect())
}

/// The only id in `ids`; an alias held by several artists names none of them.
fn sole(ids: Vec<DbId>) -> Option<DbId> {
    match ids.as_slice() {
        [id] => Some(*id),
        _ => None,
    }
}

fn scan_alias_holders(db: &impl super::DbAccess, scan_name: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .index(SCAN_ALIAS_HOLDER)
                .value(true)
                .where_()
                .node()
                .and()
                .key("scan_aliases")
                .contains(scan_name)
                .query(),
        )?
        .ids())
}

fn scan_name_holders(db: &impl super::DbAccess, scan_name: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .index("scan_name")
                .value(scan_name)
                .where_()
                .node()
                .and()
                .key("db_element_id")
                .value("Artist")
                .query(),
        )?
        .ids())
}

/// The artist whose scan name is `name`, ignoring case, else the one artist holding it as a scan
/// alias: an exact hit on the scan name index, else a pass over every artist's scan names.
pub fn find_by_scan_name(db: &impl super::DbAccess, name: &str) -> anyhow::Result<Option<DbId>> {
    if let Some(id) = scan_name_holders(db, name)?
        .into_iter()
        .min_by_key(|id| id.0)
    {
        return Ok(Some(id));
    }
    let lowered = name.to_lowercase();
    let rows = db.exec(
        QueryBuilder::select()
            .values(["scan_name", "scan_aliases"])
            .search()
            .from(Root::Artists.alias())
            .where_()
            .distance(roots::MEMBERS)
            .query(),
    )?;
    let mut alias_holders = Vec::new();
    for row in rows.elements {
        let value = |key: &str| {
            row.values
                .iter()
                .find(|kv| kv.key == DbValue::from(key))
                .map(|kv| kv.value.clone())
        };
        if let Some(DbValue::String(scan_name)) = value("scan_name")
            && scan_name.to_lowercase() == lowered
        {
            return Ok(Some(row.id));
        }
        if let Some(DbValue::VecString(aliases)) = value("scan_aliases")
            && aliases.iter().any(|alias| alias.to_lowercase() == lowered)
        {
            alias_holders.push(row.id);
        }
    }
    Ok(sole(alias_holders))
}

/// Adds `loser`'s scan name and scan aliases to `winner`'s scan aliases, skipping the winner's
/// own scan name and names another artist goes by or holds as an alias.
pub fn absorb_scan_names(
    db: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let winner_artist =
        get_by_id(db, winner)?.ok_or_else(|| anyhow!("merge winner {} missing", winner.0))?;
    let loser_artist =
        get_by_id(db, loser)?.ok_or_else(|| anyhow!("merge loser {} missing", loser.0))?;
    let mut aliases = winner_artist.scan_aliases.clone();
    for name in std::iter::once(loser_artist.scan_name).chain(loser_artist.scan_aliases) {
        if name == winner_artist.scan_name || aliases.contains(&name) {
            continue;
        }
        let mut holders = scan_name_holders(db, &name)?;
        holders.extend(scan_alias_holders(db, &name)?);
        if holders.into_iter().any(|id| id != loser && id != winner) {
            continue;
        }
        aliases.push(name);
    }
    if aliases == winner_artist.scan_aliases {
        return Ok(());
    }
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([
                ("scan_aliases", aliases).into(),
                (SCAN_ALIAS_HOLDER, true).into(),
            ])
            .ids(winner)
            .query(),
    )?;
    Ok(())
}

/// Creates an artist whose scan name is `artist_name`. Nothing references it yet, so it starts
/// as a candidate for collection.
pub fn create(
    db: &mut impl super::DbAccess,
    artist_name: &str,
    artist_type: Option<ArtistType>,
) -> anyhow::Result<DbId> {
    let artist = Artist {
        db_id: None,
        id: nanoid(),
        artist_name: artist_name.to_string(),
        scan_name: artist_name.to_string(),
        scan_aliases: Vec::new(),
        sort_name: None,
        artist_type,
        description: None,
        credited: false,
        locked: None,
        created_at_ms: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        ),
    };
    let artist_db_id = db
        .exec_mut(QueryBuilder::insert().element(&artist).query())?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow!("artist insert returned no id"))?;
    roots::link(db, Root::Artists, artist_db_id)?;
    roots::link(db, Root::ArtistCandidates, artist_db_id)?;
    Ok(artist_db_id)
}

/// Atomically aligns the stored row to `artist`.
pub fn update(db: &mut DbAny, artist: &Artist) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, artist))
}

pub fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    artist: &Artist,
) -> anyhow::Result<()> {
    let artist_db_id = artist
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow!("artist update missing db_id"))?;
    // Scan aliases change only through merges, and the credited flag only with credits.
    let stored =
        get_by_id(db, artist_db_id)?.ok_or_else(|| anyhow!("artist {} missing", artist_db_id.0))?;
    let artist = &Artist {
        scan_aliases: stored.scan_aliases,
        credited: stored.credited,
        ..artist.clone()
    };
    super::replace_element_in_transaction(
        db,
        artist_db_id,
        [
            ("sort_name", artist.sort_name.is_none()),
            ("artist_type", artist.artist_type.is_none()),
            ("description", artist.description.is_none()),
            ("locked", artist.locked.is_none()),
            ("created_at_ms", artist.created_at_ms.is_none()),
        ],
        artist,
    )
}

#[derive(Clone)]
struct ArtistSortEntry {
    artist: Artist,
    lower_name: String,
    lower_sort_name: Option<String>,
    db_id: Option<i64>,
}

fn artist_edge_orders(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<HashMap<DbId, u64>> {
    let mut orders: HashMap<DbId, u64> = HashMap::new();
    for link in super::credits::links_for_owner(db, owner_db_id)? {
        let order = link.credit.artist_order;
        orders
            .entry(link.artist_id)
            .and_modify(|existing| *existing = (*existing).min(order))
            .or_insert(order);
    }
    Ok(orders)
}

fn sort_credited_artists(credited: &mut Vec<CreditedArtist>) {
    let mut entries: Vec<(ArtistSortEntry, CreditedArtist)> = credited
        .drain(..)
        .map(|credited| (ArtistSortEntry::new(credited.artist.clone()), credited))
        .collect();

    entries.sort_by(|(left_artist, left), (right_artist, right)| {
        left.credit
            .artist_order
            .cmp(&right.credit.artist_order)
            .then_with(|| compare_artists_stably(left_artist, right_artist))
            .then_with(|| {
                left.credit
                    .credit_type
                    .to_string()
                    .cmp(&right.credit.credit_type.to_string())
            })
            .then_with(|| left.credit.detail.cmp(&right.credit.detail))
    });

    credited.extend(entries.into_iter().map(|(_, credited)| credited));
}

fn compare_artists_stably(a: &ArtistSortEntry, b: &ArtistSortEntry) -> Ordering {
    let a_sort = a
        .lower_sort_name
        .as_deref()
        .unwrap_or(a.lower_name.as_str());
    let b_sort = b
        .lower_sort_name
        .as_deref()
        .unwrap_or(b.lower_name.as_str());
    let sort_ord = a_sort.cmp(b_sort);
    if sort_ord != Ordering::Equal {
        return sort_ord;
    }

    let name_ord = a.lower_name.cmp(&b.lower_name);
    if name_ord != Ordering::Equal {
        return name_ord;
    }

    compare_option(&a.db_id, &b.db_id)
}

fn sort_artists_for_owner(
    db: &impl super::DbAccess,
    owner_db_id: Option<DbId>,
    artists: &mut Vec<Artist>,
) -> anyhow::Result<()> {
    if artists.len() < 2 {
        return Ok(());
    }

    let edge_orders = if let Some(owner_db_id) = owner_db_id {
        artist_edge_orders(db, owner_db_id)?
    } else {
        HashMap::new()
    };

    let mut entries: Vec<(ArtistSortEntry, Option<u64>)> = artists
        .drain(..)
        .map(|artist| {
            let edge_order = artist
                .db_id
                .as_ref()
                .map(|id| DbId::from(id.clone()))
                .and_then(|artist_db_id| edge_orders.get(&artist_db_id).copied());
            (ArtistSortEntry::new(artist), edge_order)
        })
        .collect();

    entries.sort_by(
        |(left, left_order), (right, right_order)| match (left_order, right_order) {
            (Some(left_order), Some(right_order)) if left_order != right_order => {
                left_order.cmp(right_order)
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            _ => compare_artists_stably(left, right),
        },
    );

    *artists = entries.into_iter().map(|(entry, _)| entry.artist).collect();
    Ok(())
}

impl ArtistSortEntry {
    fn new(artist: Artist) -> Self {
        Self {
            lower_name: artist.artist_name.to_lowercase(),
            lower_sort_name: artist.sort_name.as_ref().map(|value| value.to_lowercase()),
            db_id: artist.db_id.as_ref().map(|id| DbId::from(id.clone()).0),
            artist,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{
        insert_artist,
        insert_release,
    };
    use crate::test_db::{
        connect_artist as link,
        new_test_db,
    };

    #[test]
    fn with_scan_name_skips_other_scan_name_holders() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Rock")?;
        let genre_db_id = super::super::genres::resolve_by_name(&mut db, "Rock")?;
        assert_eq!(
            super::super::genres::find_by_name(&db, "Rock")?,
            Some(genre_db_id)
        );

        let artists = with_scan_name(&db, "rock")?;

        assert_eq!(
            artists
                .into_iter()
                .map(|artist| artist.db_id.map(DbId::from))
                .collect::<Vec<_>>(),
            vec![Some(artist_db_id)]
        );
        Ok(())
    }

    #[test]
    fn update_clears_absent_optional_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_db_id = insert_artist(&mut db, "Drift Guard")?;
        let db_id = Some(artist_db_id.into());

        update(
            &mut db,
            &Artist {
                db_id: db_id.clone(),
                id: "drift-guard".to_string(),
                artist_name: "Drift Guard".to_string(),
                scan_name: "drift guard".to_string(),
                scan_aliases: Vec::new(),
                sort_name: Some("Guard, Drift".to_string()),
                artist_type: Some(ArtistType::Group),
                description: Some("A band".to_string()),
                credited: false,
                locked: Some(true),
                created_at_ms: Some(1),
            },
        )?;

        update(
            &mut db,
            &Artist {
                db_id,
                id: "drift-guard".to_string(),
                artist_name: "Drift Guard".to_string(),
                scan_name: "drift guard".to_string(),
                scan_aliases: Vec::new(),
                sort_name: None,
                artist_type: None,
                description: None,
                credited: false,
                locked: None,
                created_at_ms: None,
            },
        )?;

        let keys = crate::test_db::stored_keys(&db, artist_db_id)?;
        assert_eq!(
            keys,
            [
                "db_element_id",
                "id",
                "artist_name",
                "scan_name",
                "scan_aliases",
                "credited"
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        Ok(())
    }

    #[test]
    fn find_by_scan_name_ignores_case_and_other_entities() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let lowered = insert_artist(&mut db, "Miles Davis")?;
        let cased = insert_artist(&mut db, "placeholder")?;
        let mut stored = get_by_id(&db, cased)?.ok_or_else(|| anyhow!("artist exists"))?;
        stored.artist_name = "Sade Adu".to_string();
        stored.scan_name = "Sade Adu".to_string();
        update(&mut db, &stored)?;
        let release = insert_release(&mut db, "Standards")?;
        crate::genres::sync_release_genres(&mut db, release, &["Jazz".to_string()])?;

        assert_eq!(find_by_scan_name(&db, "miles davis")?, Some(lowered));
        assert_eq!(find_by_scan_name(&db, "MILES DAVIS")?, Some(lowered));
        assert_eq!(find_by_scan_name(&db, "Sade Adu")?, Some(cased));
        assert_eq!(find_by_scan_name(&db, "sade adu")?, Some(cased));
        assert_eq!(find_by_scan_name(&db, "jazz")?, None);
        assert_eq!(find_by_scan_name(&db, "Nobody")?, None);
        Ok(())
    }

    fn merge_scan_names(db: &mut DbAny, winner: DbId, loser: DbId) -> anyhow::Result<()> {
        db.transaction_mut(|t| absorb_scan_names(t, winner, loser))?;
        db.exec_mut(QueryBuilder::remove().ids(loser).query())?;
        Ok(())
    }

    fn aliases_of(db: &DbAny, artist_db_id: DbId) -> anyhow::Result<Vec<String>> {
        Ok(get_by_id(db, artist_db_id)?
            .ok_or_else(|| anyhow!("artist exists"))?
            .scan_aliases)
    }

    #[test]
    fn a_merged_away_scan_name_resolves_to_the_winner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_artist(&mut db, "Mr.Children")?;
        let loser = insert_artist(&mut db, "Mr Children")?;

        merge_scan_names(&mut db, winner, loser)?;

        assert_eq!(aliases_of(&db, winner)?, ["mr children"]);
        assert_eq!(find_id_by_scan_name(&db, "mr children")?, Some(winner));
        let artists = with_scan_name(&db, "mr children")?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].db_id.clone().map(DbId::from), Some(winner));
        assert_eq!(find_by_scan_name(&db, "MR CHILDREN")?, Some(winner));
        assert_eq!(find_id_by_scan_name(&db, "mr.children")?, Some(winner));
        Ok(())
    }

    #[test]
    fn scan_aliases_match_whole_names_only() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_artist(&mut db, "Mr.Children")?;
        let loser = insert_artist(&mut db, "Mr Children")?;
        merge_scan_names(&mut db, winner, loser)?;

        for partial in ["mr", "children", "mr children band", "r c", ""] {
            assert_eq!(find_id_by_scan_name(&db, partial)?, None, "{partial:?}");
            assert!(with_scan_name(&db, partial)?.is_empty(), "{partial:?}");
            assert_eq!(find_by_scan_name(&db, partial)?, None, "{partial:?}");
        }
        Ok(())
    }

    #[test]
    fn a_scan_name_beats_a_scan_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_artist(&mut db, "Mr.Children")?;
        let loser = insert_artist(&mut db, "Mr Children")?;
        merge_scan_names(&mut db, winner, loser)?;
        let real = insert_artist(&mut db, "Mr Children")?;

        assert_eq!(find_id_by_scan_name(&db, "mr children")?, Some(real));
        let artists = with_scan_name(&db, "mr children")?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].db_id.clone().map(DbId::from), Some(real));
        assert_eq!(find_by_scan_name(&db, "MR CHILDREN")?, Some(real));
        Ok(())
    }

    #[test]
    fn absorbing_skips_names_other_artists_go_by() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_artist(&mut db, "Mr.Children")?;
        let loser = insert_artist(&mut db, "Mr Children")?;
        let namesake = insert_artist(&mut db, "Mr Children")?;
        let twin = insert_artist(&mut db, "Mr.Children")?;

        merge_scan_names(&mut db, winner, loser)?;
        merge_scan_names(&mut db, winner, twin)?;

        assert!(aliases_of(&db, winner)?.is_empty());
        assert_eq!(find_id_by_scan_name(&db, "mr children")?, Some(namesake));
        Ok(())
    }

    #[test]
    fn homonyms_never_share_a_scan_alias() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = insert_artist(&mut db, "X (first)")?;
        let first_loser = insert_artist(&mut db, "X")?;
        merge_scan_names(&mut db, first, first_loser)?;
        let second = insert_artist(&mut db, "X (second)")?;
        let second_loser = insert_artist(&mut db, "X")?;

        merge_scan_names(&mut db, second, second_loser)?;

        assert_eq!(aliases_of(&db, first)?, ["x"]);
        assert!(aliases_of(&db, second)?.is_empty());
        assert_eq!(find_id_by_scan_name(&db, "x")?, Some(first));
        Ok(())
    }

    #[test]
    fn an_alias_held_by_several_artists_resolves_to_none() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let first = insert_artist(&mut db, "X (first)")?;
        let second = insert_artist(&mut db, "X (second)")?;
        for artist in [first, second] {
            db.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([
                        ("scan_aliases", vec!["x".to_string()]).into(),
                        (SCAN_ALIAS_HOLDER, true).into(),
                    ])
                    .ids(artist)
                    .query(),
            )?;
        }

        assert_eq!(find_id_by_scan_name(&db, "x")?, None);
        assert!(with_scan_name(&db, "x")?.is_empty());
        assert_eq!(find_by_scan_name(&db, "X")?, None);
        Ok(())
    }

    #[test]
    fn alias_lookups_visit_only_alias_holders() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist = insert_artist(&mut db, "Mr.Children")?;
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("scan_aliases", vec!["mr children".to_string()]).into()])
                .ids(artist)
                .query(),
        )?;

        assert_eq!(find_id_by_scan_name(&db, "mr children")?, None);
        Ok(())
    }

    #[test]
    fn updates_keep_scan_aliases() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let winner = insert_artist(&mut db, "Mr.Children")?;
        let stale = get_by_id(&db, winner)?.ok_or_else(|| anyhow!("artist exists"))?;
        let loser = insert_artist(&mut db, "Mr Children")?;
        merge_scan_names(&mut db, winner, loser)?;

        let mut renamed = stale;
        renamed.set_artist_name("Mister Children".to_string());
        update(&mut db, &renamed)?;

        assert_eq!(aliases_of(&db, winner)?, ["mr children"]);
        assert_eq!(find_id_by_scan_name(&db, "mr children")?, Some(winner));
        Ok(())
    }

    #[test]
    fn get_credited_filters_out_non_credit_neighbors() -> anyhow::Result<()> {
        use crate::fixtures::{
            insert_track,
            link_track,
        };

        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Album")?;
        let artist_id = insert_artist(&mut db, "Artist One")?;
        link(&mut db, release_id, artist_id)?;

        let track1 = insert_track(&mut db, "Track 1")?;
        let track2 = insert_track(&mut db, "Track 2")?;
        link_track(&mut db, release_id, track1)?;
        link_track(&mut db, release_id, track2)?;

        let credited = get_credited(&db, release_id)?;
        assert_eq!(credited.len(), 1);
        assert_eq!(credited[0].artist.artist_name, "Artist One");
        Ok(())
    }

    #[test]
    fn get_credited_returns_artist_with_credit() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Album")?;
        let artist_id = insert_artist(&mut db, "Artist One")?;
        link(&mut db, release_id, artist_id)?;

        let credited = get_credited(&db, release_id)?;
        assert_eq!(credited.len(), 1);
        assert_eq!(credited[0].artist.artist_name, "Artist One");
        assert_eq!(
            credited[0].credit.credit_type,
            super::super::CreditType::Artist
        );
        Ok(())
    }

    #[test]
    fn credited_artists_follow_credit_order() -> anyhow::Result<()> {
        use crate::{
            CreditType,
            fixtures::connect_credit,
        };

        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Album")?;
        let zed = insert_artist(&mut db, "Zed")?;
        let abe = insert_artist(&mut db, "Abe")?;
        connect_credit(&mut db, release_id, zed, CreditType::Artist, None, 0)?;
        connect_credit(&mut db, release_id, abe, CreditType::Artist, None, 1)?;
        connect_credit(&mut db, release_id, zed, CreditType::Composer, None, 2)?;

        let credited = get_credited(&db, release_id)?;
        let credited: Vec<(&str, CreditType)> = credited
            .iter()
            .map(|c| (c.artist.artist_name.as_str(), c.credit.credit_type))
            .collect();
        assert_eq!(
            credited,
            [
                ("Zed", CreditType::Artist),
                ("Abe", CreditType::Artist),
                ("Zed", CreditType::Composer),
            ]
        );
        let names: Vec<String> = get(&db, release_id)?
            .into_iter()
            .map(|artist| artist.artist_name)
            .collect();
        assert_eq!(names, ["Zed", "Abe"]);
        Ok(())
    }

    #[test]
    fn get_by_id_returns_none_for_missing_artist() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_by_id(&db, DbId(999999))?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_by_id_returns_inserted_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Test Artist")?;

        let artist = get_by_id(&db, artist_id)?.expect("artist should exist");
        assert_eq!(artist.artist_name, "Test Artist");
        assert_eq!(artist.scan_name, "test artist");
        Ok(())
    }

    #[test]
    fn credited_ids_finds_credited_artists_of_that_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let person_id = insert_artist(&mut db, "Person Artist")?;
        let group_id = insert_artist(&mut db, "Group Artist")?;
        let uncredited_id = insert_artist(&mut db, "Uncredited Person")?;
        link(&mut db, release_id, person_id)?;
        link(&mut db, release_id, group_id)?;

        for (artist_id, artist_type) in [
            (person_id, ArtistType::Person),
            (group_id, ArtistType::Group),
            (uncredited_id, ArtistType::Person),
        ] {
            let mut artist = get_by_id(&db, artist_id)?.expect("artist should exist");
            artist.set_artist_type(artist_type);
            update(&mut db, &artist)?;
        }

        assert_eq!(
            credited_ids(&db, Some(ArtistType::Person))?,
            vec![person_id]
        );
        let mut people = ids_with_type(&db, ArtistType::Person)?;
        people.sort();
        assert_eq!(people, vec![person_id, uncredited_id]);
        let mut credited = credited_ids(&db, None)?;
        credited.sort();
        assert_eq!(credited, vec![person_id, group_id]);
        Ok(())
    }

    #[test]
    fn updates_keep_the_credited_flag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        let stale = get_by_id(&db, artist_id)?.ok_or_else(|| anyhow!("artist exists"))?;
        link(&mut db, release_id, artist_id)?;

        let mut renamed = stale;
        renamed.set_artist_name("Renamed".to_string());
        update(&mut db, &renamed)?;

        assert!(get_by_id(&db, artist_id)?.is_some_and(|artist| artist.credited));
        Ok(())
    }

    #[test]
    fn get_returns_artists_linked_to_owner() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        let artist_id = insert_artist(&mut db, "Artist One")?;
        link(&mut db, release_id, artist_id)?;

        let artists = get(&db, release_id)?;
        assert_eq!(artists.len(), 1);
        assert_eq!(artists[0].artist_name, "Artist One");
        Ok(())
    }

    #[test]
    fn get_returns_empty_when_no_artists_linked() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Empty Album")?;

        let artists = get(&db, release_id)?;
        assert!(artists.is_empty());
        Ok(())
    }

    #[test]
    fn get_many_by_owner_returns_per_owner_map() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_a = insert_release(&mut db, "Album A")?;
        let release_b = insert_release(&mut db, "Album B")?;
        let artist_1 = insert_artist(&mut db, "Artist 1")?;
        let artist_2 = insert_artist(&mut db, "Artist 2")?;

        link(&mut db, release_a, artist_1)?;
        link(&mut db, release_b, artist_2)?;

        let result = get_many_by_owner(&db, &[release_a, release_b])?;
        assert_eq!(result.get(&release_a).map(|v| v.len()), Some(1));
        assert_eq!(result.get(&release_b).map(|v| v.len()), Some(1));
        assert_eq!(result[&release_a][0].artist_name, "Artist 1");
        assert_eq!(result[&release_b][0].artist_name, "Artist 2");
        Ok(())
    }

    #[test]
    fn get_by_releases_includes_track_only_artists_once() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "Album")?;
        let track = crate::fixtures::insert_track(&mut db, "Song")?;
        crate::fixtures::link_track(&mut db, release, track)?;
        let main = insert_artist(&mut db, "Main")?;
        let featured = insert_artist(&mut db, "Featured")?;
        link(&mut db, release, main)?;
        link(&mut db, track, main)?;
        link(&mut db, track, featured)?;

        let artists = get_by_releases(&db, &[release])?;

        assert_eq!(
            artists
                .iter()
                .map(|artist| artist.artist_name.as_str())
                .collect::<Vec<_>>(),
            vec!["Main", "Featured"]
        );
        Ok(())
    }

    #[test]
    fn update_modifies_artist_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Original")?;

        let mut artist = get_by_id(&db, artist_id)?.expect("artist should exist");
        artist.artist_name = "Updated".to_string();
        update(&mut db, &artist)?;

        let updated = get_by_id(&db, artist_id)?.expect("artist should exist");
        assert_eq!(updated.artist_name, "Updated");
        Ok(())
    }
}

#[cfg(all(test, feature = "nightly"))]
mod benches {
    extern crate test;

    use test::Bencher;

    use super::*;
    use crate::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use crate::test_db::{
        connect_artist as link,
        new_test_db,
    };

    #[bench]
    fn get_credited_realistic_release_with_tracks(b: &mut Bencher) {
        // Release with 3 credits + 15 attached tracks, whose plain edges the
        // credit edge filter must skip.
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        for i in 0..3 {
            let artist_id = insert_artist(&mut db, &format!("Artist {i}")).unwrap();
            link(&mut db, release_id, artist_id).unwrap();
        }
        for i in 0..15 {
            let track_id = insert_track(&mut db, &format!("Track {i}")).unwrap();
            connect(&mut db, release_id, track_id).unwrap();
        }
        b.iter(|| get_credited(&db, release_id).unwrap());
    }

    #[bench]
    fn get_credited_single_owner_3_credits(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        for i in 0..3 {
            let artist_id = insert_artist(&mut db, &format!("Artist {i}")).unwrap();
            link(&mut db, release_id, artist_id).unwrap();
        }
        b.iter(|| get_credited(&db, release_id).unwrap());
    }

    #[bench]
    fn get_credited_single_owner_10_credits(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let release_id = insert_release(&mut db, "Album").unwrap();
        for i in 0..10 {
            let artist_id = insert_artist(&mut db, &format!("Artist {i}")).unwrap();
            link(&mut db, release_id, artist_id).unwrap();
        }
        b.iter(|| get_credited(&db, release_id).unwrap());
    }

    #[bench]
    fn get_credited_many_10_owners(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let mut ids = Vec::new();
        for i in 0..10 {
            let release_id = insert_release(&mut db, &format!("Album {i}")).unwrap();
            for j in 0..3 {
                let artist_id = insert_artist(&mut db, &format!("Artist {i}-{j}")).unwrap();
                link(&mut db, release_id, artist_id).unwrap();
            }
            ids.push(release_id);
        }
        b.iter(|| get_credited_many_by_owner(&db, &ids).unwrap());
    }

    #[bench]
    fn get_credited_many_50_owners(b: &mut Bencher) {
        let mut db = new_test_db().unwrap();
        let mut ids = Vec::new();
        for i in 0..50 {
            let release_id = insert_release(&mut db, &format!("Album {i}")).unwrap();
            for j in 0..3 {
                let artist_id = insert_artist(&mut db, &format!("Artist {i}-{j}")).unwrap();
                link(&mut db, release_id, artist_id).unwrap();
            }
            ids.push(release_id);
        }
        b.iter(|| get_credited_many_by_owner(&db, &ids).unwrap());
    }
}
