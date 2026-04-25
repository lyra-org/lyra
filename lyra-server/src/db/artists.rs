// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::cmp::Ordering;
use std::collections::HashMap;

use agdb::{
    CountComparison,
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbType,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryId,
};
use anyhow::anyhow;
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
#[harmony_macros::enumeration]
pub(crate) enum ArtistType {
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
            _ => Err(DbError::from(format!("invalid ArtistType value '{value}'"))),
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

harmony_macros::compile!(type_path = ArtistType, variants = true);
use super::{
    Credit,
    ListOptions,
    PagedResult,
    SortKey,
    SortSpec,
    apply_direction,
    compare_option,
};

#[derive(DbElement, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[harmony_macros::structure]
pub(crate) struct Artist {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) artist_name: String,
    pub(crate) scan_name: String,
    pub(crate) sort_name: Option<String>,
    pub(crate) artist_type: Option<ArtistType>,
    pub(crate) description: Option<String>,
    pub(crate) verified: bool,
    pub(crate) locked: Option<bool>,
    pub(crate) created_at: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct CreditedArtist {
    pub(crate) artist: Artist,
    pub(crate) credit: Credit,
}

#[harmony_macros::implementation]
impl Artist {
    pub(crate) fn set_artist_name(&mut self, artist_name: String) {
        self.artist_name = artist_name;
    }

    pub(crate) fn set_sort_name(&mut self, sort_name: String) {
        self.sort_name = Some(sort_name);
    }

    pub(crate) fn set_artist_type(&mut self, artist_type: ArtistType) {
        self.artist_type = Some(artist_type);
    }

    pub(crate) fn set_description(&mut self, description: String) {
        self.description = Some(description);
    }

    pub(crate) fn set_verified(&mut self, verified: bool) {
        self.verified = verified;
    }
}

harmony_macros::compile!(type_path = Artist, fields = true, methods = true);

pub(crate) fn get(
    db: &impl super::DbAccess,
    from: impl Into<QueryId>,
) -> anyhow::Result<Vec<Artist>> {
    let from = from.into();
    let owner_db_id = resolve_owner_id(db, &from);
    let mut artists = get_connected_artists(db, &from)?;
    sort_artists_for_owner(db, owner_db_id, &mut artists)?;
    Ok(artists)
}

// Two paths: Credit intermediaries for owned entities, direct neighbors for root aliases.
fn get_connected_artists(db: &impl super::DbAccess, from: &QueryId) -> anyhow::Result<Vec<Artist>> {
    use super::Credit;

    let mut seen = std::collections::HashSet::new();
    let mut artists = Vec::new();

    // Collect direct Artist neighbors (root alias paths).
    let direct: Vec<Artist> = db
        .exec(
            QueryBuilder::select()
                .elements::<Artist>()
                .search()
                .from(from.clone())
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    for entry in direct {
        if let Some(id) = entry.db_id.clone().map(DbId::from) {
            if seen.insert(id) {
                artists.push(entry);
            }
        }
    }

    // Collect Artist nodes via Credit intermediaries (owner → Credit → Artist).
    let credits: Vec<Credit> = db
        .exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .from(from.clone())
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    for credit in &credits {
        let Some(credit_db_id) = credit.db_id.clone().map(DbId::from) else {
            continue;
        };
        let artists_batch: Vec<Artist> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Artist>()
                    .search()
                    .from(credit_db_id)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        for entry in artists_batch {
            if let Some(id) = entry.db_id.clone().map(DbId::from) {
                if seen.insert(id) {
                    artists.push(entry);
                }
            }
        }
    }

    Ok(artists)
}

/// Returns all unique artists belonging to a library via its releases.
pub(crate) fn get_by_library(db: &DbAny, library_id: DbId) -> anyhow::Result<Vec<Artist>> {
    let release_ids: Vec<DbId> = super::releases::get_direct(db, library_id)?
        .into_iter()
        .filter_map(|a| a.db_id.map(Into::into))
        .collect();
    if release_ids.is_empty() {
        return Ok(Vec::new());
    }
    let artist_map = get_many_by_owner(db, &release_ids)?;
    let mut seen = std::collections::HashSet::new();
    let mut artists = Vec::new();
    for owner_artists in artist_map.into_values() {
        for artist in owner_artists {
            let artist_id = artist.db_id.as_ref().map(|id| DbId::from(id.clone()));
            if let Some(id) = artist_id {
                if seen.insert(id) {
                    artists.push(artist);
                }
            } else {
                artists.push(artist);
            }
        }
    }
    Ok(artists)
}

pub(crate) fn get_many_by_owner(
    db: &DbAny,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<Artist>>> {
    let unique_owner_db_ids = super::dedup_positive_ids(owner_db_ids);

    let mut related: HashMap<DbId, Vec<Artist>> = unique_owner_db_ids
        .iter()
        .copied()
        .map(|owner_db_id| (owner_db_id, Vec::new()))
        .collect();
    if unique_owner_db_ids.is_empty() {
        return Ok(related);
    }

    for owner_db_id in &unique_owner_db_ids {
        let query_id = QueryId::from(*owner_db_id);
        let mut artists = get_connected_artists(db, &query_id)?;
        sort_artists_for_owner(db, Some(*owner_db_id), &mut artists)?;
        related.insert(*owner_db_id, artists);
    }

    Ok(related)
}

pub(crate) fn get_credited(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Vec<CreditedArtist>> {
    let mut credited = get_connected_credited_artists(db, owner_db_id)?;
    sort_credited_artists(&mut credited);
    Ok(credited
        .into_iter()
        .map(|(credited_artist, _)| credited_artist)
        .collect())
}

pub(crate) fn get_credited_many_by_owner(
    db: &DbAny,
    owner_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<CreditedArtist>>> {
    let unique_owner_db_ids = super::dedup_positive_ids(owner_db_ids);
    let mut related: HashMap<DbId, Vec<CreditedArtist>> = unique_owner_db_ids
        .iter()
        .copied()
        .map(|owner_db_id| (owner_db_id, Vec::new()))
        .collect();
    if unique_owner_db_ids.is_empty() {
        return Ok(related);
    }

    for owner_db_id in unique_owner_db_ids {
        related.insert(owner_db_id, get_credited(db, owner_db_id)?);
    }

    Ok(related)
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    artist_db_id: DbId,
) -> anyhow::Result<Option<Artist>> {
    super::graph::fetch_typed_by_id(db, artist_db_id, "Artist")
}

pub(crate) fn update(db: &mut impl super::DbAccess, artist: &Artist) -> anyhow::Result<()> {
    db.exec_mut(QueryBuilder::insert().element(artist).query())?;
    Ok(())
}

pub(crate) fn update_with_clears(
    db: &mut DbAny,
    artist: &Artist,
    clear_sort_name: bool,
    clear_description: bool,
) -> anyhow::Result<()> {
    let artist_db_id = artist
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow!("artist update missing db_id"))?;
    db.transaction_mut(|t| -> anyhow::Result<()> {
        if clear_sort_name {
            t.exec_mut(
                QueryBuilder::remove()
                    .values(["sort_name".to_string()])
                    .ids(artist_db_id)
                    .query(),
            )?;
        }
        if clear_description {
            t.exec_mut(
                QueryBuilder::remove()
                    .values(["description".to_string()])
                    .ids(artist_db_id)
                    .query(),
            )?;
        }
        t.exec_mut(QueryBuilder::insert().element(artist).query())?;
        Ok(())
    })?;

    Ok(())
}

#[derive(Clone)]
struct ArtistSortEntry {
    artist: Artist,
    lower_name: String,
    lower_sort_name: Option<String>,
    db_id: Option<i64>,
    date_created: Option<u64>,
}

fn resolve_owner_id(db: &impl super::DbAccess, from: &QueryId) -> Option<DbId> {
    match from {
        QueryId::Id(id) if id.0 > 0 => Some(*id),
        QueryId::Alias(alias) => db
            .exec(QueryBuilder::select().ids(alias.as_str()).query())
            .ok()
            .and_then(|result| result.ids().first().copied())
            .filter(|id| id.0 > 0),
        _ => None,
    }
}

fn edge_order_value(element: &DbElement) -> Option<u64> {
    element.values.iter().find_map(|kv| {
        if matches!(&kv.key, DbValue::String(key) if key == super::credits::EDGE_ORDER_KEY) {
            match &kv.value {
                DbValue::U64(value) => Some(*value),
                DbValue::I64(value) if *value >= 0 => Some(*value as u64),
                _ => None,
            }
        } else {
            None
        }
    })
}

fn artist_edge_orders(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<HashMap<DbId, u64>> {
    let mut orders: HashMap<DbId, u64> = HashMap::new();
    // Owner edges point to Credit nodes.
    let edge_ids: Vec<DbId> = super::graph::direct_edges_from(db, owner_db_id)?
        .into_iter()
        .map(|edge| edge.id)
        .collect();
    if edge_ids.is_empty() {
        return Ok(orders);
    }
    let mut credit_orders: Vec<(DbId, u64)> = Vec::new();
    for edge in db
        .exec(QueryBuilder::select().ids(edge_ids).query())?
        .elements
    {
        let Some(credit_db_id) = edge.to else {
            continue;
        };
        let Some(order) = edge_order_value(&edge) else {
            continue;
        };
        credit_orders.push((credit_db_id, order));
    }
    // Follow each Credit→Artist edge to map order to artist.
    for (credit_db_id, order) in credit_orders {
        let credit_edges = super::graph::direct_edges_from(db, credit_db_id)?;
        for edge in credit_edges {
            let Some(artist_db_id) = edge.to.filter(|id| id.0 > 0) else {
                continue;
            };
            orders
                .entry(artist_db_id)
                .and_modify(|existing: &mut u64| *existing = (*existing).min(order))
                .or_insert(order);
        }
    }
    Ok(orders)
}

// Maps Credit node DbId to its owner edge order.
pub(crate) fn artist_edge_orders_raw(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<HashMap<DbId, u64>> {
    let mut orders = HashMap::new();
    let edge_ids: Vec<DbId> = super::graph::direct_edges_from(db, owner_db_id)?
        .into_iter()
        .map(|edge| edge.id)
        .collect();
    if edge_ids.is_empty() {
        return Ok(orders);
    }
    for edge in db
        .exec(QueryBuilder::select().ids(edge_ids).query())?
        .elements
    {
        let Some(target_db_id) = edge.to else {
            continue;
        };
        let Some(order) = edge_order_value(&edge) else {
            continue;
        };
        orders.insert(target_db_id, order);
    }
    Ok(orders)
}

fn get_connected_credited_artists(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Vec<(CreditedArtist, Option<u64>)>> {
    // BFS up to distance 4 (owner → edge → credit → edge → artist).
    let search = db.exec(
        QueryBuilder::search()
            .from(owner_db_id)
            .where_()
            .distance(CountComparison::LessThanOrEqual(4))
            .and()
            .not()
            .ids(owner_db_id)
            .and()
            .beyond()
            .edge()
            .or()
            .key("db_element_id")
            .value("Credit")
            .end_where()
            .query(),
    )?;

    let all_ids: Vec<DbId> = search.elements.iter().map(|e| e.id).collect();
    if all_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Single bulk fetch for both edges (negative IDs) and nodes (positive IDs).
    let elements = db
        .exec(QueryBuilder::select().ids(&all_ids).query())?
        .elements;

    let mut credits_by_id: HashMap<DbId, Credit> = HashMap::new();
    let mut artists_by_id: HashMap<DbId, Artist> = HashMap::new();
    let mut edges: Vec<DbElement> = Vec::new();
    for element in elements {
        if element.id.0 < 0 {
            edges.push(element);
        } else if super::graph::is_element_type(&element, "Credit") {
            credits_by_id.insert(element.id, Credit::from_db_element(&element)?);
        } else if super::graph::is_element_type(&element, "Artist") {
            artists_by_id.insert(element.id, Artist::from_db_element(&element)?);
        }
    }

    if credits_by_id.is_empty() {
        return Ok(Vec::new());
    }

    // Classify edges: owner→credit edges carry order; credit→artist edges link pairs.
    let mut order_by_credit: HashMap<DbId, Option<u64>> = HashMap::new();
    let mut credit_artist_links: Vec<(DbId, DbId)> = Vec::new();
    for edge in &edges {
        let Some(to) = edge.to.filter(|id| id.0 > 0) else {
            continue;
        };
        if edge.from == Some(owner_db_id) && credits_by_id.contains_key(&to) {
            order_by_credit.insert(to, edge_order_value(edge));
        } else if let Some(from) = edge.from.filter(|id| credits_by_id.contains_key(id)) {
            if artists_by_id.contains_key(&to) {
                credit_artist_links.push((from, to));
            }
        }
    }

    let mut credited = Vec::new();
    for (credit_db_id, artist_db_id) in credit_artist_links {
        let Some(credit) = credits_by_id.get(&credit_db_id) else {
            continue;
        };
        let Some(artist) = artists_by_id.get(&artist_db_id) else {
            continue;
        };
        credited.push((
            CreditedArtist {
                artist: artist.clone(),
                credit: credit.clone(),
            },
            order_by_credit.get(&credit_db_id).copied().flatten(),
        ));
    }

    Ok(credited)
}

struct CreditedArtistSortEntry {
    credited: CreditedArtist,
    artist: ArtistSortEntry,
    order: Option<u64>,
}

fn sort_credited_artists(credited: &mut Vec<(CreditedArtist, Option<u64>)>) {
    let mut entries: Vec<CreditedArtistSortEntry> = credited
        .drain(..)
        .map(|(credited, order)| CreditedArtistSortEntry {
            artist: ArtistSortEntry::new(credited.artist.clone()),
            credited,
            order,
        })
        .collect();

    entries.sort_by(|left, right| match (left.order, right.order) {
        (Some(left_order), Some(right_order)) if left_order != right_order => {
            left_order.cmp(&right_order)
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        _ => {
            let artist_ord = compare_artists_stably(&left.artist, &right.artist);
            if artist_ord != Ordering::Equal {
                artist_ord
            } else {
                let credit_type_ord = left
                    .credited
                    .credit
                    .credit_type
                    .to_string()
                    .cmp(&right.credited.credit.credit_type.to_string());
                if credit_type_ord != Ordering::Equal {
                    credit_type_ord
                } else {
                    left.credited
                        .credit
                        .detail
                        .cmp(&right.credited.credit.detail)
                }
            }
        }
    });

    credited.extend(
        entries
            .into_iter()
            .map(|entry| (entry.credited, entry.order)),
    );
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
            date_created: artist.created_at,
            artist,
        }
    }
}

fn compare_artist_field(a: &ArtistSortEntry, b: &ArtistSortEntry, key: SortKey) -> Ordering {
    match key {
        SortKey::SortName => a
            .lower_sort_name
            .as_deref()
            .unwrap_or(a.lower_name.as_str())
            .cmp(
                b.lower_sort_name
                    .as_deref()
                    .unwrap_or(b.lower_name.as_str()),
            ),
        SortKey::Name => a.lower_name.cmp(&b.lower_name),
        SortKey::DateCreated => compare_option(&a.date_created, &b.date_created),
        SortKey::DbId => compare_option(&a.db_id, &b.db_id),
        SortKey::ReleaseDate | SortKey::TrackNumber | SortKey::DiscNumber | SortKey::Duration => {
            Ordering::Equal
        }
    }
}

fn compare_artist_entries(a: &ArtistSortEntry, b: &ArtistSortEntry, sort: &[SortSpec]) -> Ordering {
    for spec in sort {
        let ord = apply_direction(compare_artist_field(a, b, spec.key), spec.direction);
        if ord != Ordering::Equal {
            return ord;
        }
    }

    let name_ord = a.lower_name.cmp(&b.lower_name);
    if name_ord != Ordering::Equal {
        return name_ord;
    }

    compare_option(&a.db_id, &b.db_id)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn paginate_artists(mut artists: Vec<Artist>, options: &ListOptions) -> PagedResult<Artist> {
    let total_count = artists.len() as u64;
    let offset = options.offset.unwrap_or(0).min(total_count);
    let offset = u64_to_usize_saturating(offset).min(artists.len());
    let limit = options.limit.map(u64_to_usize_saturating);

    let entries = match limit {
        Some(limit) => artists.drain(offset..).take(limit).collect(),
        None => artists.drain(offset..).collect(),
    };

    PagedResult {
        entries,
        total_count,
        offset: offset as u64,
    }
}

fn sort_and_paginate_artists(
    mut entries: Vec<ArtistSortEntry>,
    options: &ListOptions,
) -> PagedResult<Artist> {
    let total_count = entries.len() as u64;
    let offset = options.offset.unwrap_or(0).min(total_count);
    let offset = u64_to_usize_saturating(offset).min(entries.len());
    let limit = options.limit.map(u64_to_usize_saturating);

    if entries.is_empty() {
        return PagedResult {
            entries: Vec::new(),
            total_count,
            offset: offset as u64,
        };
    }

    if options.sort.is_empty() {
        let entries = match limit {
            Some(limit) => entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|entry| entry.artist)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.artist)
                .collect(),
        };

        return PagedResult {
            entries,
            total_count,
            offset: offset as u64,
        };
    }

    let page_end = match limit {
        Some(limit) => offset.saturating_add(limit).min(entries.len()),
        None => entries.len(),
    };
    if page_end == 0 || offset >= page_end {
        return PagedResult {
            entries: Vec::new(),
            total_count,
            offset: offset as u64,
        };
    }

    if page_end < entries.len() {
        let pivot = page_end - 1;
        entries.select_nth_unstable_by(pivot, |a, b| compare_artist_entries(a, b, &options.sort));
        entries.truncate(page_end);
    }

    entries.sort_by(|a, b| compare_artist_entries(a, b, &options.sort));
    PagedResult {
        entries: match limit {
            Some(limit) => entries
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|entry| entry.artist)
                .collect(),
            None => entries
                .into_iter()
                .skip(offset)
                .map(|entry| entry.artist)
                .collect(),
        },
        total_count,
        offset: offset as u64,
    }
}

pub(crate) fn query(
    db: &DbAny,
    from: impl Into<QueryId>,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Artist>> {
    let artists = get(db, from)?;

    if options.search_term.is_none() && options.sort.is_empty() {
        return Ok(paginate_artists(artists, options));
    }

    let mut entries: Vec<ArtistSortEntry> = artists.into_iter().map(ArtistSortEntry::new).collect();

    // Text search filter
    if let Some(ref term) = options.search_term {
        let lower_term = term.to_lowercase();
        entries.retain(|entry| entry.lower_name.contains(&lower_term));
    }

    Ok(sort_and_paginate_artists(entries, options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        connect_artist as link,
        insert_artist,
        insert_release,
        new_test_db,
    };

    #[test]
    fn get_credited_filters_out_non_credit_neighbors() -> anyhow::Result<()> {
        use crate::db::test_db::{
            connect,
            insert_track,
        };

        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Album")?;
        let artist_id = insert_artist(&mut db, "Artist One")?;
        link(&mut db, release_id, artist_id)?;

        // Attach tracks directly to the release — these are non-credit distance-2
        // neighbors. beyond() prevents expansion past them, but they still appear
        // in the search skeleton; is_element_type filters them out in post-processing.
        let track1 = insert_track(&mut db, "Track 1")?;
        let track2 = insert_track(&mut db, "Track 2")?;
        connect(&mut db, release_id, track1)?;
        connect(&mut db, release_id, track2)?;

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
        assert!(!artist.verified);
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
    fn update_modifies_artist_fields() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Original")?;

        let mut artist = get_by_id(&db, artist_id)?.expect("artist should exist");
        artist.artist_name = "Updated".to_string();
        artist.verified = true;
        update(&mut db, &artist)?;

        let updated = get_by_id(&db, artist_id)?.expect("artist should exist");
        assert_eq!(updated.artist_name, "Updated");
        assert!(updated.verified);
        Ok(())
    }

    #[test]
    fn query_with_default_options_returns_all() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_artist(&mut db, "Alpha")?;
        insert_artist(&mut db, "Beta")?;

        let options = super::super::ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: None,
        };
        let result = query(&db, "artists", &options)?;
        assert_eq!(result.total_count, 2);
        assert_eq!(result.entries.len(), 2);
        Ok(())
    }

    #[test]
    fn query_with_search_term_filters() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_artist(&mut db, "Rock Band")?;
        insert_artist(&mut db, "Jazz Trio")?;
        insert_artist(&mut db, "Rock Duo")?;

        let options = super::super::ListOptions {
            sort: vec![],
            offset: None,
            limit: None,
            search_term: Some("rock".to_string()),
        };
        let result = query(&db, "artists", &options)?;
        assert_eq!(result.total_count, 2);
        assert_eq!(result.entries.len(), 2);
        Ok(())
    }

    #[test]
    fn query_sort_by_name_ascending() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_artist(&mut db, "Zephyr")?;
        insert_artist(&mut db, "Apex")?;
        insert_artist(&mut db, "Middle")?;

        let options = super::super::ListOptions {
            sort: vec![SortSpec {
                key: SortKey::Name,
                direction: super::super::SortDirection::Ascending,
            }],
            offset: None,
            limit: None,
            search_term: None,
        };
        let result = query(&db, "artists", &options)?;
        let names: Vec<&str> = result
            .entries
            .iter()
            .map(|a| a.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Apex", "Middle", "Zephyr"]);
        Ok(())
    }

    #[test]
    fn query_with_limit_and_offset() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_artist(&mut db, "A")?;
        insert_artist(&mut db, "B")?;
        insert_artist(&mut db, "C")?;

        let options = super::super::ListOptions {
            sort: vec![SortSpec {
                key: SortKey::Name,
                direction: super::super::SortDirection::Ascending,
            }],
            offset: Some(1),
            limit: Some(1),
            search_term: None,
        };
        let result = query(&db, "artists", &options)?;
        assert_eq!(result.total_count, 3);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.offset, 1);
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
        connect_artist as link,
        insert_artist,
        insert_release,
        insert_track,
        new_test_db,
    };

    #[bench]
    fn get_credited_realistic_release_with_tracks(b: &mut Bencher) {
        // Release with 3 credits + 15 attached tracks (non-credit neighbors at
        // distance 2). Exercises the inclusion filter's job of excluding
        // non-credit nodes from the search skeleton.
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
