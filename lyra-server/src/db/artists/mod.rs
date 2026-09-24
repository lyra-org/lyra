// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod relations;

use std::cmp::Ordering;
use std::collections::HashMap;

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbElement,
    DbError,
    DbId,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryId,
};
use anyhow::anyhow;
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;

#[harmony_macros::userdata(name = "ArtistType")]
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
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
    ListOptions,
    PagedResult,
    SortKey,
    SortSpec,
    apply_direction,
    compare_option,
};

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
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

impl Artist {
    pub(crate) fn set_artist_name(&mut self, artist_name: String) {
        self.artist_name = artist_name;
    }

    pub(crate) fn set_sort_name(&mut self, sort_name: String) {
        self.sort_name = Some(sort_name);
    }

    #[cfg(test)]
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

impl_luau_record_userdata!(
    Artist,
    "Artist",
    fields {
        db_id: Option<NodeId> as "db_id",
        id: String as "id",
        artist_name: String as "artist_name",
        scan_name: String as "scan_name",
        sort_name: Option<String> as "sort_name",
        artist_type: Option<ArtistType> as "artist_type",
        description: Option<String> as "description",
        verified: bool as "verified",
        locked: Option<bool> as "locked",
        created_at: Option<u64> as "created_at",
    },
    methods {
        set_artist_name(artist_name: String),
        set_sort_name(sort_name: String),
        set_artist_type(artist_type: ArtistType),
        set_description(description: String),
        set_verified(verified: bool),
    }
);

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

fn get_connected_artists(db: &impl super::DbAccess, from: &QueryId) -> anyhow::Result<Vec<Artist>> {
    Ok(db
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
        .try_into()?)
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

/// Each owner's credited artists, once each, in credit order.
pub(crate) fn get_many_by_owner(
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

pub(crate) fn get_credited(
    db: &impl super::DbAccess,
    owner_db_id: DbId,
) -> anyhow::Result<Vec<CreditedArtist>> {
    Ok(get_credited_many_by_owner(db, &[owner_db_id])?
        .remove(&owner_db_id)
        .unwrap_or_default())
}

pub(crate) fn get_credited_many_by_owner(
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

fn filter_artists_by_type(artists: Vec<Artist>, artist_type: Option<ArtistType>) -> Vec<Artist> {
    let Some(artist_type) = artist_type else {
        return artists;
    };

    artists
        .into_iter()
        .filter(|artist| artist.artist_type == Some(artist_type))
        .collect()
}

pub(crate) fn get_by_id(
    db: &impl super::DbAccess,
    artist_db_id: DbId,
) -> anyhow::Result<Option<Artist>> {
    super::graph::fetch_typed_by_id(db, artist_db_id, "Artist")
}

/// Atomically aligns the stored row to `artist`.
pub(crate) fn update(db: &mut DbAny, artist: &Artist) -> anyhow::Result<()> {
    db.transaction_mut(|t| update_in_transaction(t, artist))
}

pub(crate) fn update_in_transaction(
    db: &mut DbAnyTransactionMut<'_>,
    artist: &Artist,
) -> anyhow::Result<()> {
    let artist_db_id = artist
        .db_id
        .clone()
        .map(DbId::from)
        .ok_or_else(|| anyhow!("artist update missing db_id"))?;
    super::replace_element_in_transaction(
        db,
        artist_db_id,
        [
            ("sort_name", artist.sort_name.is_none()),
            ("artist_type", artist.artist_type.is_none()),
            ("description", artist.description.is_none()),
            ("locked", artist.locked.is_none()),
            ("created_at", artist.created_at.is_none()),
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
    date_created: Option<u64>,
    match_score: u32,
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
            date_created: artist.created_at,
            artist,
            match_score: 0,
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

    let score_ord = b.match_score.cmp(&a.match_score);
    if score_ord != Ordering::Equal {
        return score_ord;
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

pub(crate) fn query_items(artists: Vec<Artist>, options: &ListOptions) -> PagedResult<Artist> {
    if options.search_term.is_none() && options.sort.is_empty() {
        return paginate_artists(artists, options);
    }

    let mut entries: Vec<ArtistSortEntry> = artists.into_iter().map(ArtistSortEntry::new).collect();

    if let Some(ref term) = options.search_term {
        super::search::fuzzy_filter(
            &mut entries,
            term,
            |entry| entry.artist.artist_name.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    sort_and_paginate_artists(entries, options)
}

pub(crate) fn query(
    db: &DbAny,
    from: impl Into<QueryId>,
    options: &ListOptions,
    artist_type: Option<ArtistType>,
) -> anyhow::Result<PagedResult<Artist>> {
    let artists = filter_artists_by_type(get(db, from)?, artist_type);
    Ok(query_items(artists, options))
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
                sort_name: Some("Guard, Drift".to_string()),
                artist_type: Some(ArtistType::Group),
                description: Some("A band".to_string()),
                verified: true,
                locked: Some(true),
                created_at: Some(1),
            },
        )?;

        update(
            &mut db,
            &Artist {
                db_id,
                id: "drift-guard".to_string(),
                artist_name: "Drift Guard".to_string(),
                scan_name: "drift guard".to_string(),
                sort_name: None,
                artist_type: None,
                description: None,
                verified: true,
                locked: None,
                created_at: None,
            },
        )?;

        let keys = crate::db::test_db::stored_keys(&db, artist_db_id)?;
        assert_eq!(
            keys,
            [
                "db_element_id",
                "id",
                "artist_name",
                "scan_name",
                "verified"
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            "only non-Option keys may remain after an all-None update"
        );
        Ok(())
    }

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
    fn credited_artists_follow_credit_order() -> anyhow::Result<()> {
        use crate::db::{
            CreditType,
            test_db::connect_credit,
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
        assert!(!artist.verified);
        Ok(())
    }

    #[test]
    fn query_filters_by_artist_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let person_id = insert_artist(&mut db, "Person Artist")?;
        let group_id = insert_artist(&mut db, "Group Artist")?;

        let mut person = get_by_id(&db, person_id)?.expect("person artist should exist");
        person.set_artist_type(ArtistType::Person);
        update(&mut db, &person)?;

        let mut group = get_by_id(&db, group_id)?.expect("group artist should exist");
        group.set_artist_type(ArtistType::Group);
        update(&mut db, &group)?;

        let result = query(
            &db,
            "artists",
            &ListOptions {
                sort: vec![],
                offset: None,
                limit: None,
                search_term: None,
            },
            Some(ArtistType::Person),
        )?;

        assert_eq!(result.total_count, 1);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].artist_name, "Person Artist");
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
        let result = query(&db, "artists", &options, None)?;
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
        let result = query(&db, "artists", &options, None)?;
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
        let result = query(&db, "artists", &options, None)?;
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
        let result = query(&db, "artists", &options, None)?;
        assert_eq!(result.total_count, 3);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.offset, 1);
        Ok(())
    }
}

#[cfg(all(test, feature = "nightly"))]
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
