// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
};

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
    QueryId,
};
use anyhow::bail;
use lyra_db::nanoid;

use crate::services::{
    auth::{
        Principal,
        access,
    },
    catalog::Direction,
    search,
};
use lyra_db::{
    Playlist,
    favorites::FavoriteKind,
};

/// The most track or entry ids one playlist write takes.
pub(crate) const MAX_PLAYLIST_TRACK_IDS: usize = 10_000;

/// A playlist request the caller must change, applied to nothing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidPlaylistRequest {
    #[error("too many {what}: {count}, at most {MAX_PLAYLIST_TRACK_IDS}")]
    TooManyIds { what: &'static str, count: usize },
    #[error("playlist entry listed more than once: {0}")]
    RepeatedEntry(String),
    #[error("the relevance sort key needs a search term")]
    RelevanceWithoutSearch,
}

/// Raises [`InvalidPlaylistRequest::TooManyIds`] past [`MAX_PLAYLIST_TRACK_IDS`].
pub(crate) fn check_id_count(what: &'static str, count: usize) -> anyhow::Result<()> {
    if count > MAX_PLAYLIST_TRACK_IDS {
        return Err(InvalidPlaylistRequest::TooManyIds { what, count }.into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct PlaylistTrackLink {
    pub(crate) entry_db_id: DbId,
    pub(crate) entry_id: String,
    pub(crate) track_db_id: DbId,
    pub(crate) position: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CreatePlaylistRequest {
    pub(crate) user_db_id: DbId,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) is_public: Option<bool>,
    pub(crate) created_at_ms: Option<u64>,
    pub(crate) updated_at_ms: Option<u64>,
    pub(crate) track_db_ids: Vec<DbId>,
}

#[derive(Clone, Debug)]
pub(crate) struct UpdatePlaylistRequest {
    pub(crate) playlist_id: QueryId,
    pub(crate) name: Option<String>,
    /// Outer `None` leaves the description untouched; `Some(None)` clears it.
    pub(crate) description: Option<Option<String>>,
    pub(crate) is_public: Option<bool>,
    pub(crate) updated_at_ms: Option<u64>,
}

fn resolve_optional_id(db: &DbAny, query_id: QueryId) -> anyhow::Result<Option<DbId>> {
    match query_id {
        QueryId::Id(db_id) => Ok(Some(db_id)),
        QueryId::Alias(alias) => {
            if let Ok(parsed) = alias.trim().parse::<i64>()
                && parsed > 0
            {
                return Ok(Some(DbId(parsed)));
            }

            let result = db.exec(QueryBuilder::select().ids(alias.as_str()).query())?;
            let ids = result.ids();
            match ids.as_slice() {
                [node_id] => Ok(Some(*node_id)),
                [] => Ok(None),
                _ => bail!("entity alias resolves to multiple ids: {alias}"),
            }
        }
    }
}

pub(crate) fn resolve_id(db: &DbAny, query_id: QueryId) -> anyhow::Result<DbId> {
    let alias = match &query_id {
        QueryId::Alias(alias) => Some(alias.clone()),
        QueryId::Id(_) => None,
    };
    resolve_optional_id(db, query_id)?.ok_or_else(|| match alias {
        Some(alias) => anyhow::anyhow!("entity alias not found: {alias}"),
        None => anyhow::anyhow!("entity not found"),
    })
}

fn validate_name(raw_name: &str) -> anyhow::Result<String> {
    let trimmed = raw_name.trim().to_string();
    if trimmed.is_empty() {
        bail!("playlist name cannot be empty");
    }
    Ok(trimmed)
}

pub(crate) fn list(db: &DbAny) -> anyhow::Result<Vec<Playlist>> {
    lyra_db::playlists::get(db)
}

#[cfg(test)]
pub(crate) fn get(db: &DbAny, query_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, query_id)? else {
        return Ok(None);
    };
    lyra_db::playlists::get_by_id(db, playlist_db_id)
}

pub(crate) fn get_by_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<Playlist>> {
    lyra_db::playlists::get_by_user(db, user_db_id)
}

pub(crate) fn get_owner(db: &DbAny, playlist_id: QueryId) -> anyhow::Result<Option<DbId>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    lyra_db::playlists::get_owner(db, playlist_db_id)
}

/// The public ids of the owners of these playlists, keyed by playlist. Playlists without an
/// owner are absent.
pub(crate) fn owner_ids(
    db: &DbAny,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, String>> {
    let mut owners = HashMap::with_capacity(playlist_db_ids.len());
    for &playlist_db_id in playlist_db_ids {
        if let Some(owner_db_id) = lyra_db::playlists::get_owner(db, playlist_db_id)? {
            owners.insert(playlist_db_id, owner_db_id);
        }
    }
    let owner_db_ids = owners
        .values()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let public_ids = lyra_db::lookup::find_ids_by_db_ids(db, &owner_db_ids)?;
    Ok(owners
        .into_iter()
        .filter_map(|(playlist_db_id, owner_db_id)| {
            Some((playlist_db_id, public_ids.get(&owner_db_id)?.clone()))
        })
        .collect())
}

pub(crate) fn get_tracks(
    db: &DbAny,
    playlist_id: QueryId,
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;

    let playlist_tracks = lyra_db::playlists::get_tracks(db, playlist_db_id)?;
    let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|track| track.edge_id).collect();
    let track_ids = lyra_db::playlists::resolve_edge_targets(db, &edge_ids)?;

    let mut links = Vec::with_capacity(playlist_tracks.len());
    for (playlist_track, track_db_id) in playlist_tracks.into_iter().zip(track_ids) {
        links.push(PlaylistTrackLink {
            entry_db_id: playlist_track.edge_id,
            entry_id: playlist_track.entry_id,
            track_db_id,
            position: playlist_track.position,
        });
    }

    Ok(links)
}

pub(crate) fn get_tracks_many(
    db: &DbAny,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<PlaylistTrackLink>>> {
    let raw = lyra_db::playlists::get_tracks_many(db, playlist_db_ids)?;
    let mut result = HashMap::new();
    for (playlist_id, playlist_tracks) in raw {
        let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|t| t.edge_id).collect();
        let track_ids = lyra_db::playlists::resolve_edge_targets(db, &edge_ids)?;
        let mut links = Vec::with_capacity(playlist_tracks.len());
        for (pt, track_db_id) in playlist_tracks.into_iter().zip(track_ids) {
            links.push(PlaylistTrackLink {
                entry_db_id: pt.edge_id,
                entry_id: pt.entry_id,
                track_db_id,
                position: pt.position,
            });
        }
        result.insert(playlist_id, links);
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PlaylistSummary {
    pub(crate) track_count: u64,
    pub(crate) total_duration_ms: u64,
}

/// Track counts and total durations for many playlists without hydrating each
/// track into a response.
///
/// `track_count` is the raw entry count, matching the `unavailable` placeholders
/// that `inc=tracks` returns for entries the caller cannot see. Durations are
/// summed only over accessible tracks, because `TrackResponse::unavailable`
/// deliberately withholds `duration_ms` — an unfiltered total would disclose it.
pub(crate) fn summaries(
    db: &DbAny,
    principal: &Principal,
    playlist_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, PlaylistSummary>> {
    let tracks_by_playlist = get_tracks_many(db, playlist_db_ids)?;
    let track_db_ids = tracks_by_playlist
        .values()
        .flatten()
        .map(|link| link.track_db_id)
        .collect::<Vec<_>>();
    let tracks = lyra_db::tracks::get_by_ids(db, &track_db_ids)?;
    let accessible =
        crate::services::auth::access::accessible_entities(db, principal, &track_db_ids)?;

    Ok(tracks_by_playlist
        .into_iter()
        .map(|(playlist_db_id, links)| {
            let mut summary = PlaylistSummary::default();
            for link in links {
                summary.track_count = summary.track_count.saturating_add(1);
                if !accessible.contains(&link.track_db_id) {
                    continue;
                }
                let Some(duration_ms) = tracks
                    .get(&link.track_db_id)
                    .and_then(|track| track.duration_ms)
                else {
                    continue;
                };
                summary.total_duration_ms = summary.total_duration_ms.saturating_add(duration_ms);
            }
            (playlist_db_id, summary)
        })
        .collect())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SortKey {
    Name,
    CreatedAt,
    UpdatedAt,
    TrackCount,
    TotalDuration,
    /// When the caller first favorited the playlist; unfavorited playlists sort last.
    FavoritedAt,
    /// How well the name matches the search, best first; needs a search term.
    Relevance,
    Id,
}

impl SortKey {
    /// Every key and the token that names it.
    pub(crate) const KEYS: &'static [(&'static str, Self)] = &[
        ("name", Self::Name),
        ("created_at", Self::CreatedAt),
        ("updated_at", Self::UpdatedAt),
        ("track_count", Self::TrackCount),
        ("total_duration", Self::TotalDuration),
        ("favorited_at", Self::FavoritedAt),
        ("relevance", Self::Relevance),
        ("id", Self::Id),
    ];

    pub(crate) fn parse(token: &str) -> Option<Self> {
        Self::KEYS
            .iter()
            .find(|(name, _)| *name == token)
            .map(|(_, key)| *key)
    }

    fn needs_summary(self) -> bool {
        matches!(self, Self::TrackCount | Self::TotalDuration)
    }
}

/// Which of the playlists the caller can see a list keeps.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlaylistFilter {
    /// Playlists this user owns.
    pub(crate) owner: Option<DbId>,
    pub(crate) favorite: Option<bool>,
    /// Only these playlists.
    pub(crate) ids: Option<HashSet<DbId>>,
    pub(crate) exclude_ids: HashSet<DbId>,
    /// Leaves out playlists without an owner, which a deleted user leaves behind.
    pub(crate) owned: bool,
}

struct ListEntry {
    playlist: Playlist,
    sort_name: String,
    summary: PlaylistSummary,
    favorited_at: Option<i64>,
    match_score: u32,
}

fn compare_field(a: &ListEntry, b: &ListEntry, key: SortKey) -> Ordering {
    match key {
        SortKey::Name => a
            .sort_name
            .cmp(&b.sort_name)
            .then_with(|| a.playlist.name.cmp(&b.playlist.name)),
        SortKey::CreatedAt => {
            lyra_db::compare_option(&a.playlist.created_at_ms, &b.playlist.created_at_ms)
        }
        SortKey::UpdatedAt => {
            lyra_db::compare_option(&a.playlist.updated_at_ms, &b.playlist.updated_at_ms)
        }
        SortKey::TrackCount => a.summary.track_count.cmp(&b.summary.track_count),
        SortKey::TotalDuration => a
            .summary
            .total_duration_ms
            .cmp(&b.summary.total_duration_ms),
        SortKey::FavoritedAt => a.favorited_at.cmp(&b.favorited_at),
        SortKey::Relevance => b.match_score.cmp(&a.match_score),
        SortKey::Id => a.playlist.id.cmp(&b.playlist.id),
    }
}

fn compare_entries(a: &ListEntry, b: &ListEntry, sort: &[(SortKey, Direction)]) -> Ordering {
    for &(key, direction) in sort {
        if key == SortKey::FavoritedAt {
            match (a.favorited_at, b.favorited_at) {
                (Some(_), None) => return Ordering::Less,
                (None, Some(_)) => return Ordering::Greater,
                _ => {}
            }
        }
        let ord = direction.apply(compare_field(a, b, key));
        if ord != Ordering::Equal {
            return ord;
        }
    }

    b.match_score
        .cmp(&a.match_score)
        .then_with(|| a.sort_name.cmp(&b.sort_name))
        .then_with(|| a.playlist.name.cmp(&b.playlist.name))
        .then_with(|| a.playlist.id.cmp(&b.playlist.id))
}

/// The playlists the principal can see that pass `filter` and match `search`, ordered by
/// `sort` (by relevance when searching and by name otherwise when empty), with any summaries
/// the sort computed so callers need not repeat them.
pub(crate) fn list_visible(
    db: &DbAny,
    principal: &Principal,
    filter: PlaylistFilter,
    sort: &[(SortKey, Direction)],
    search: Option<&str>,
) -> anyhow::Result<(Vec<Playlist>, HashMap<DbId, PlaylistSummary>)> {
    let sort = match (sort.is_empty(), search.is_some()) {
        (true, true) => &[(SortKey::Relevance, Direction::Ascending)],
        (true, false) => &[(SortKey::Name, Direction::Ascending)],
        (false, _) => sort,
    };
    if search.is_none() && sort.iter().any(|(key, _)| *key == SortKey::Relevance) {
        return Err(InvalidPlaylistRequest::RelevanceWithoutSearch.into());
    }
    let user_db_id = principal.require(db)?;
    let candidates = match filter.owner {
        Some(owner) => get_by_user(db, owner)?,
        None => list(db)?,
    };
    let favorites = match filter.favorite {
        Some(_) => Some(lyra_db::favorites::target_ids(
            db,
            user_db_id,
            FavoriteKind::Playlist,
        )?),
        None => None,
    };
    let favorited_at = if sort.iter().any(|(key, _)| *key == SortKey::FavoritedAt) {
        lyra_db::favorites::first_favorited_at(db, user_db_id, FavoriteKind::Playlist)?
    } else {
        HashMap::new()
    };
    let mut entries = Vec::new();
    for playlist in candidates {
        let Some(playlist_db_id) = playlist.db_id.clone().map(DbId::from) else {
            continue;
        };
        if filter
            .ids
            .as_ref()
            .is_some_and(|ids| !ids.contains(&playlist_db_id))
            || filter.exclude_ids.contains(&playlist_db_id)
            || !(if filter.owned {
                access::playlist_visible_to_listener(db, user_db_id, playlist_db_id)?
            } else {
                access::playlist_visible_to_user(db, user_db_id, playlist_db_id)?
            })
        {
            continue;
        }
        if let (Some(wanted), Some(favorites)) = (filter.favorite, &favorites)
            && favorites.contains(&playlist_db_id) != wanted
        {
            continue;
        }
        entries.push(ListEntry {
            sort_name: playlist.name.to_lowercase(),
            favorited_at: favorited_at.get(&playlist_db_id).copied(),
            playlist,
            summary: PlaylistSummary::default(),
            match_score: 0,
        });
    }

    // Filter before summarising: discarded playlists should not be walked.
    if let Some(term) = search {
        search::filter(
            &mut entries,
            term,
            |entry| entry.playlist.name.as_str(),
            |entry, score| entry.match_score = score,
        );
    }

    let summaries = if sort.iter().any(|(key, _)| key.needs_summary()) {
        let playlist_db_ids = entries
            .iter()
            .filter_map(|entry| entry.playlist.db_id.clone().map(DbId::from))
            .collect::<Vec<_>>();
        let computed = summaries(db, principal, &playlist_db_ids)?;
        for entry in &mut entries {
            entry.summary = entry
                .playlist
                .db_id
                .clone()
                .map(DbId::from)
                .and_then(|playlist_db_id| computed.get(&playlist_db_id).copied())
                .unwrap_or_default();
        }
        computed
    } else {
        HashMap::new()
    };

    entries.sort_by(|a, b| compare_entries(a, b, sort));
    Ok((
        entries.into_iter().map(|entry| entry.playlist).collect(),
        summaries,
    ))
}

/// Creates the playlist and returns it with its database id set.
pub(crate) fn create(
    db: &mut DbAny,
    request: &CreatePlaylistRequest,
) -> anyhow::Result<(DbId, Playlist)> {
    check_id_count("track ids", request.track_db_ids.len())?;
    let mut playlist = Playlist {
        db_id: None,
        id: nanoid(),
        name: validate_name(&request.name)?,
        description: request.description.clone(),
        is_public: request.is_public,
        created_at_ms: request.created_at_ms,
        updated_at_ms: request.updated_at_ms,
    };
    require_tracks(db, &request.track_db_ids)?;

    let playlist_db_id = db.transaction_mut(|t| -> anyhow::Result<DbId> {
        let playlist_db_id =
            lyra_db::playlists::create_in_transaction(t, &playlist, request.user_db_id)?;
        if !request.track_db_ids.is_empty() {
            lyra_db::playlists::add_tracks(t, playlist_db_id, &request.track_db_ids)?;
            lyra_db::covers::display::sync_playlist_cover(t, playlist_db_id)?;
        }
        Ok(playlist_db_id)
    })?;
    playlist.db_id = Some(playlist_db_id.into());
    Ok((playlist_db_id, playlist))
}

fn require_tracks(db: &DbAny, track_db_ids: &[DbId]) -> anyhow::Result<()> {
    for &track_db_id in track_db_ids {
        if lyra_db::tracks::get_by_id(db, track_db_id)?.is_none() {
            bail!("track not found: {}", track_db_id.0);
        }
    }
    Ok(())
}

pub(crate) fn delete(db: &mut DbAny, playlist_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    let Some(playlist) = lyra_db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };

    let orphaned = lyra_db::playlists::delete(db, playlist_db_id)?;
    let covers_root = crate::services::covers::configured_covers_root();
    for cover in orphaned {
        crate::services::covers::discard_cover(&covers_root, &cover);
    }
    Ok(Some(playlist))
}

pub(crate) fn update(
    db: &mut DbAny,
    request: &UpdatePlaylistRequest,
) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, request.playlist_id.clone())? else {
        return Ok(None);
    };
    let Some(mut playlist) = lyra_db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };
    apply_update(&mut playlist, request)?;

    lyra_db::playlists::update(db, &playlist)?;
    Ok(Some(playlist))
}

fn apply_update(playlist: &mut Playlist, request: &UpdatePlaylistRequest) -> anyhow::Result<()> {
    if let Some(name) = &request.name {
        playlist.name = validate_name(name)?;
    }
    if let Some(description) = &request.description {
        playlist.description = description.clone();
    }
    if let Some(is_public) = request.is_public {
        playlist.is_public = Some(is_public);
    }
    if let Some(updated_at_ms) = request.updated_at_ms {
        playlist.updated_at_ms = Some(updated_at_ms);
    }
    Ok(())
}

/// Applies `request` and makes `track_db_ids` the playlist's entries, in order and with
/// repeats, as one transaction.
pub(crate) fn replace(
    db: &mut DbAny,
    request: &UpdatePlaylistRequest,
    track_db_ids: &[DbId],
) -> anyhow::Result<Option<Playlist>> {
    check_id_count("track ids", track_db_ids.len())?;
    let Some(playlist_db_id) = resolve_optional_id(db, request.playlist_id.clone())? else {
        return Ok(None);
    };
    let Some(mut playlist) = lyra_db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };
    apply_update(&mut playlist, request)?;
    require_tracks(db, track_db_ids)?;
    let entries = lyra_db::playlists::get_tracks(db, playlist_db_id)?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        lyra_db::playlists::update_in_transaction(t, &playlist)?;
        let edge_ids = entries
            .iter()
            .map(|entry| entry.edge_id)
            .collect::<Vec<_>>();
        lyra_db::playlists::remove_tracks(t, playlist_db_id, &edge_ids)?;
        lyra_db::playlists::add_tracks(t, playlist_db_id, track_db_ids)?;
        lyra_db::covers::display::sync_playlist_cover(t, playlist_db_id)
    })?;
    Ok(Some(playlist))
}

#[cfg(test)]
pub(crate) fn add_track(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_db_id: QueryId,
) -> anyhow::Result<lyra_db::playlists::PlaylistTrack> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let track_db_id = resolve_id(db, track_db_id)?;
    db.transaction_mut(|t| {
        let added = lyra_db::playlists::add_track(t, playlist_db_id, track_db_id)?;
        lyra_db::covers::display::offer_track_to_playlist_cover(t, playlist_db_id, track_db_id)?;
        Ok(added)
    })
}

/// Adds `track_ids` in order: at the 0-based `position` when given, clamped to the playlist's
/// length, and at the end otherwise.
pub(crate) fn add_tracks(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_ids: &[QueryId],
    position: Option<u64>,
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    check_id_count("track ids", track_ids.len())?;
    let playlist_db_id = resolve_id(db, playlist_id)?;
    if track_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut resolved_track_ids = Vec::with_capacity(track_ids.len());
    for track_id in track_ids {
        let track_db_id = resolve_id(db, track_id.clone())?;
        if lyra_db::tracks::get_by_id(db, track_db_id)?.is_none() {
            bail!("track not found: {}", track_db_id.0);
        }
        resolved_track_ids.push(track_db_id);
    }

    let results = db.transaction_mut(|t| -> anyhow::Result<_> {
        let results = match position {
            Some(position) => {
                lyra_db::playlists::insert_tracks(t, playlist_db_id, &resolved_track_ids, position)?
            }
            None => lyra_db::playlists::add_tracks(t, playlist_db_id, &resolved_track_ids)?,
        };
        lyra_db::covers::display::sync_playlist_cover(t, playlist_db_id)?;
        Ok(results)
    })?;
    Ok(results
        .into_iter()
        .zip(resolved_track_ids)
        .map(|(result, track_db_id)| PlaylistTrackLink {
            entry_db_id: result.edge_id,
            entry_id: result.entry_id,
            track_db_id,
            position: result.position,
        })
        .collect())
}

/// The playlist's entry with public id `entry_id`, looked up under the caller's guard.
pub(crate) fn find_entry(
    db: &DbAny,
    playlist_db_id: DbId,
    entry_id: &str,
) -> anyhow::Result<Option<PlaylistTrackLink>> {
    Ok(get_tracks(db, QueryId::Id(playlist_db_id))?
        .into_iter()
        .find(|track| track.entry_id == entry_id))
}

fn require_entry(
    db: &DbAny,
    playlist_db_id: DbId,
    entry_id: &str,
) -> anyhow::Result<PlaylistTrackLink> {
    find_entry(db, playlist_db_id, entry_id)?
        .ok_or_else(|| anyhow::anyhow!("playlist entry not found: {entry_id}"))
}

/// Removes the playlist's entries named by public id, failing before any removal when one is
/// not in the playlist or is named twice.
pub(crate) fn remove_tracks(
    db: &mut DbAny,
    playlist_db_id: DbId,
    entry_ids: &[String],
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    check_id_count("entry ids", entry_ids.len())?;
    let mut seen = HashSet::with_capacity(entry_ids.len());
    if let Some(repeated) = entry_ids
        .iter()
        .find(|entry_id| !seen.insert(entry_id.as_str()))
    {
        return Err(InvalidPlaylistRequest::RepeatedEntry(repeated.clone()).into());
    }
    let mut entries = get_tracks(db, QueryId::Id(playlist_db_id))?
        .into_iter()
        .map(|link| (link.entry_id.clone(), link))
        .collect::<HashMap<_, _>>();
    let removed = entry_ids
        .iter()
        .map(|entry_id| {
            entries
                .remove(entry_id)
                .ok_or_else(|| anyhow::anyhow!("playlist entry not found: {entry_id}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    db.transaction_mut(|t| -> anyhow::Result<()> {
        let edge_ids = removed
            .iter()
            .map(|track| track.entry_db_id)
            .collect::<Vec<_>>();
        lyra_db::playlists::remove_tracks(t, playlist_db_id, &edge_ids)?;
        lyra_db::covers::display::sync_playlist_cover(t, playlist_db_id)
    })?;

    Ok(removed)
}

pub(crate) fn move_track(
    db: &mut DbAny,
    playlist_db_id: DbId,
    entry_id: &str,
    new_position: u64,
) -> anyhow::Result<()> {
    let entry_db_id = require_entry(db, playlist_db_id, entry_id)?.entry_db_id;
    db.transaction_mut(|t| {
        lyra_db::playlists::move_track(t, playlist_db_id, entry_db_id, new_position)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::fixtures::{
        insert_track,
        test_user,
    };
    use lyra_db::test_db::new_test_db;

    fn insert_track_with_duration(
        db: &mut DbAny,
        title: &str,
        duration_ms: u64,
    ) -> anyhow::Result<DbId> {
        let track_db_id = insert_track(db, title)?;
        let mut track = lyra_db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing after insert"))?;
        track.set_duration_ms(duration_ms);
        db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        Ok(track_db_id)
    }

    fn principal(user_db_id: DbId, permissions: Vec<lyra_db::Permission>) -> Principal {
        Principal::from_parts(
            user_db_id,
            "summary-principal".to_string(),
            "summary-principal".to_string(),
            permissions,
            None,
            std::collections::HashSet::new(),
        )
    }

    fn create_playlist(db: &mut DbAny, user_db_id: DbId, name: &str) -> anyhow::Result<DbId> {
        create(
            db,
            &CreatePlaylistRequest {
                user_db_id,
                name: name.to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
                track_db_ids: Vec::new(),
            },
        )
        .map(|(playlist_db_id, _)| playlist_db_id)
    }

    fn owner(db: &mut DbAny) -> anyhow::Result<(DbId, Principal)> {
        let user_db_id = lyra_db::test_db::insert_user(db, "playlist-owner")?;
        let principal = Principal::for_user(
            db,
            user_db_id,
            vec![lyra_db::Permission::Admin],
            std::collections::HashSet::new(),
        );
        Ok((user_db_id, principal))
    }

    fn names(playlists: &[Playlist]) -> Vec<&str> {
        playlists
            .iter()
            .map(|playlist| playlist.name.as_str())
            .collect()
    }

    #[test]
    fn list_visible_rejects_principal_whose_db_id_was_recycled() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (alice, stale) = owner(&mut db)?;
        let replacement = lyra_db::test_db::recycle_user(&mut db, alice, "replacement")?;
        create_playlist(&mut db, replacement, "Replacement's Playlist")?;

        let error = list_visible(&db, &stale, PlaylistFilter::default(), &[], None)
            .expect_err("a deleted user's principal must not list its replacement's playlists");
        assert!(matches!(
            error.downcast_ref::<crate::services::auth::AuthError>(),
            Some(crate::services::auth::AuthError::InvalidBearerCredential)
        ));
        Ok(())
    }

    #[test]
    fn list_visible_sorts_by_track_count() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = owner(&mut db)?;
        create_playlist(&mut db, user_db_id, "Zebra")?;
        let filled = create_playlist(&mut db, user_db_id, "Alpha")?;
        let track = insert_track(&mut db, "Track")?;
        add_tracks(&mut db, QueryId::Id(filled), &[QueryId::Id(track)], None)?;

        let sort = [(SortKey::TrackCount, Direction::Descending)];
        let (ordered, summaries) =
            list_visible(&db, &principal, PlaylistFilter::default(), &sort, None)?;
        assert_eq!(names(&ordered), ["Alpha", "Zebra"]);
        assert_eq!(summaries.get(&filled).map(|s| s.track_count), Some(1));

        // An empty sort orders by name, which needs no summaries.
        let (ordered, summaries) =
            list_visible(&db, &principal, PlaylistFilter::default(), &[], None)?;
        assert_eq!(names(&ordered), ["Alpha", "Zebra"]);
        assert!(
            summaries.is_empty(),
            "name sort should not pay for summaries"
        );

        Ok(())
    }

    #[test]
    fn list_visible_filters_by_search_term() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = owner(&mut db)?;
        create_playlist(&mut db, user_db_id, "Late Night Jazz")?;
        create_playlist(&mut db, user_db_id, "Morning Rock")?;

        let (ordered, _) = list_visible(
            &db,
            &principal,
            PlaylistFilter::default(),
            &[],
            Some("jazz"),
        )?;
        assert_eq!(names(&ordered), ["Late Night Jazz"]);

        Ok(())
    }

    #[test]
    fn list_visible_ranks_a_search_by_relevance_without_a_sort() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = owner(&mut db)?;
        create_playlist(&mut db, user_db_id, "A Jazz Night")?;
        create_playlist(&mut db, user_db_id, "Jazz")?;

        let (ordered, _) = list_visible(
            &db,
            &principal,
            PlaylistFilter::default(),
            &[],
            Some("jazz"),
        )?;
        assert_eq!(names(&ordered), ["Jazz", "A Jazz Night"]);

        let sort = [(SortKey::Relevance, Direction::Ascending)];
        let error = list_visible(&db, &principal, PlaylistFilter::default(), &sort, None)
            .expect_err("relevance needs a search term");
        assert!(matches!(
            error.downcast_ref::<InvalidPlaylistRequest>(),
            Some(InvalidPlaylistRequest::RelevanceWithoutSearch)
        ));
        Ok(())
    }

    #[test]
    fn list_visible_keeps_only_the_named_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = owner(&mut db)?;
        let first = create_playlist(&mut db, user_db_id, "First")?;
        let second = create_playlist(&mut db, user_db_id, "Second")?;
        create_playlist(&mut db, user_db_id, "Third")?;

        let filter = PlaylistFilter {
            ids: Some(HashSet::from([first, second])),
            exclude_ids: HashSet::from([second]),
            ..PlaylistFilter::default()
        };
        let (ordered, _) = list_visible(&db, &principal, filter, &[], None)?;
        assert_eq!(names(&ordered), ["First"]);
        Ok(())
    }

    #[test]
    fn remove_tracks_refuses_a_repeated_entry_removing_nothing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("repeat-remover")?)?;
        let track = insert_track(&mut db, "Track")?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Repeats")?;
        let added = add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &[QueryId::Id(track), QueryId::Id(track)],
            None,
        )?;

        let entry_id = added[0].entry_id.clone();
        let error = remove_tracks(&mut db, playlist_db_id, &[entry_id.clone(), entry_id])
            .expect_err("a repeated entry is refused");
        assert!(matches!(
            error.downcast_ref::<InvalidPlaylistRequest>(),
            Some(InvalidPlaylistRequest::RepeatedEntry(_))
        ));
        assert_eq!(entry_tracks(&db, playlist_db_id)?, vec![track, track]);
        Ok(())
    }

    #[test]
    fn writes_past_the_id_cap_apply_nothing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("capped")?)?;
        let track = insert_track(&mut db, "Track")?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Capped")?;
        let too_many = vec![QueryId::Id(track); MAX_PLAYLIST_TRACK_IDS + 1];

        let error = add_tracks(&mut db, QueryId::Id(playlist_db_id), &too_many, None)
            .expect_err("past the cap");
        assert!(matches!(
            error.downcast_ref::<InvalidPlaylistRequest>(),
            Some(InvalidPlaylistRequest::TooManyIds { .. })
        ));
        assert!(entry_tracks(&db, playlist_db_id)?.is_empty());
        Ok(())
    }

    #[test]
    fn list_visible_covers_public_playlists_owners_and_favorites() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (user_db_id, principal) = owner(&mut db)?;
        let other = lyra_db::test_db::insert_user(&mut db, "playlist-other")?;
        create_playlist(&mut db, user_db_id, "Mine")?;
        let shared = create_playlist(&mut db, other, "Shared")?;
        create_playlist(&mut db, other, "Private")?;
        let mut public = lyra_db::playlists::get_by_id(&db, shared)?.expect("playlist");
        public.is_public = Some(true);
        lyra_db::playlists::update(&mut db, &public)?;
        lyra_db::favorites::add(&mut db, user_db_id, shared, FavoriteKind::Playlist, 1_000)?;

        let list =
            |db: &DbAny, filter, sort: &[(SortKey, Direction)]| -> anyhow::Result<Vec<String>> {
                let (ordered, _) = list_visible(db, &principal, filter, sort, None)?;
                Ok(ordered.into_iter().map(|playlist| playlist.name).collect())
            };
        assert_eq!(
            list(&db, PlaylistFilter::default(), &[])?,
            ["Mine", "Shared"],
            "another user's private playlist stays hidden, even from an admin",
        );
        let mine = PlaylistFilter {
            owner: Some(user_db_id),
            ..PlaylistFilter::default()
        };
        assert_eq!(list(&db, mine, &[])?, ["Mine"]);
        let theirs = PlaylistFilter {
            owner: Some(other),
            ..PlaylistFilter::default()
        };
        assert_eq!(list(&db, theirs, &[])?, ["Shared"]);
        let favorite = |favorite| PlaylistFilter {
            favorite: Some(favorite),
            ..PlaylistFilter::default()
        };
        assert_eq!(list(&db, favorite(true), &[])?, ["Shared"]);
        assert_eq!(list(&db, favorite(false), &[])?, ["Mine"]);
        for direction in [Direction::Ascending, Direction::Descending] {
            assert_eq!(
                list(
                    &db,
                    PlaylistFilter::default(),
                    &[(SortKey::FavoritedAt, direction)]
                )?,
                ["Shared", "Mine"],
                "unfavorited playlists sort last in both directions",
            );
        }

        public.is_public = Some(false);
        lyra_db::playlists::update(&mut db, &public)?;
        assert!(
            list(&db, favorite(true), &[])?.is_empty(),
            "a favorite that turned private drops out",
        );
        Ok(())
    }

    /// `Some(None)` is an explicit clear and must reach storage; `None` leaves
    /// the stored description alone.
    #[test]
    fn delete_cleans_up_entries_favorites_and_cover_profile() -> anyhow::Result<()> {
        use lyra_db::covers::display::{
            self,
            DisplayCoverScope,
            DisplayCoverTargetKind,
        };
        let mut db = new_test_db()?;
        let user = lyra_db::users::create(&mut db, &test_user("playlist-delete")?)?;
        let track = insert_track(&mut db, "Retained track")?;
        let playlist_id = create_playlist(&mut db, user, "Deleted")?;
        let playlist = get(&db, QueryId::Id(playlist_id))?.expect("playlist");
        let entry = add_track(&mut db, QueryId::Id(playlist_id), QueryId::Id(track))?;
        lyra_db::favorites::add(
            &mut db,
            user,
            playlist_id,
            lyra_db::favorites::FavoriteKind::Playlist,
            1,
        )?;
        display::sync_playlist_cover(&mut db, playlist_id)?;
        assert!(
            display::get_profile(
                &db,
                DisplayCoverScope::Instance,
                DisplayCoverTargetKind::Playlist,
                None,
                &playlist.id
            )?
            .is_some()
        );
        assert!(delete(&mut db, QueryId::Id(playlist_id))?.is_some());
        assert!(get(&db, QueryId::Id(playlist_id))?.is_none());
        assert!(
            db.exec(agdb::QueryBuilder::select().ids(entry.edge_id).query())
                .is_err()
        );
        assert!(!lyra_db::favorites::has(&db, user, playlist_id)?);
        assert!(
            display::get_profile(
                &db,
                DisplayCoverScope::Instance,
                DisplayCoverTargetKind::Playlist,
                None,
                &playlist.id
            )?
            .is_none()
        );
        assert!(lyra_db::tracks::get_by_id(&db, track)?.is_some());
        Ok(())
    }

    #[test]
    fn update_distinguishes_absent_description_from_explicit_clear() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("describer")?)?;
        let (playlist_db_id, created) = create(
            &mut db,
            &CreatePlaylistRequest {
                user_db_id,
                name: "Described".to_string(),
                description: Some("original".to_string()),
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
                track_db_ids: Vec::new(),
            },
        )?;
        assert_eq!(created.db_id.map(DbId::from), Some(playlist_db_id));
        assert_eq!(created.description.as_deref(), Some("original"));

        let untouched = update(
            &mut db,
            &UpdatePlaylistRequest {
                playlist_id: QueryId::Id(playlist_db_id),
                name: Some("Renamed".to_string()),
                description: None,
                is_public: None,
                updated_at_ms: None,
            },
        )?
        .ok_or_else(|| anyhow::anyhow!("playlist missing"))?;
        assert_eq!(untouched.description.as_deref(), Some("original"));

        update(
            &mut db,
            &UpdatePlaylistRequest {
                playlist_id: QueryId::Id(playlist_db_id),
                name: None,
                description: Some(None),
                is_public: None,
                updated_at_ms: None,
            },
        )?
        .ok_or_else(|| anyhow::anyhow!("playlist missing"))?;

        let stored = lyra_db::playlists::get_by_id(&db, playlist_db_id)?
            .ok_or_else(|| anyhow::anyhow!("playlist missing"))?;
        assert_eq!(stored.description, None);
        assert_eq!(stored.name, "Renamed");
        Ok(())
    }

    fn replacement(playlist_db_id: DbId, name: Option<&str>) -> UpdatePlaylistRequest {
        UpdatePlaylistRequest {
            playlist_id: QueryId::Id(playlist_db_id),
            name: name.map(str::to_string),
            description: None,
            is_public: None,
            updated_at_ms: None,
        }
    }

    fn entry_tracks(db: &DbAny, playlist_db_id: DbId) -> anyhow::Result<Vec<DbId>> {
        Ok(get_tracks(db, QueryId::Id(playlist_db_id))?
            .into_iter()
            .map(|link| link.track_db_id)
            .collect())
    }

    #[test]
    fn replace_updates_fields_and_entries_keeping_order_and_repeats() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("replacer")?)?;
        let first = insert_track(&mut db, "First")?;
        let second = insert_track(&mut db, "Second")?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Before")?;
        add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &[QueryId::Id(first), QueryId::Id(second)],
            None,
        )?;

        let replaced = replace(
            &mut db,
            &replacement(playlist_db_id, Some("After")),
            &[second, first, second],
        )?
        .ok_or_else(|| anyhow::anyhow!("playlist missing"))?;

        assert_eq!(replaced.name, "After");
        assert_eq!(
            entry_tracks(&db, playlist_db_id)?,
            vec![second, first, second]
        );
        let positions = get_tracks(&db, QueryId::Id(playlist_db_id))?
            .into_iter()
            .map(|link| link.position)
            .collect::<Vec<_>>();
        assert_eq!(positions, vec![0, 1, 2]);
        Ok(())
    }

    #[test]
    fn replace_changes_nothing_when_a_track_is_missing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("failed-replacer")?)?;
        let kept = insert_track(&mut db, "Kept")?;
        let deleted = insert_track(&mut db, "Deleted")?;
        db.exec_mut(QueryBuilder::remove().ids(deleted).query())?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Unchanged")?;
        add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &[QueryId::Id(kept)],
            None,
        )?;

        assert!(
            replace(
                &mut db,
                &replacement(playlist_db_id, Some("Renamed")),
                &[kept, deleted],
            )
            .is_err()
        );

        let stored = lyra_db::playlists::get_by_id(&db, playlist_db_id)?
            .ok_or_else(|| anyhow::anyhow!("playlist missing"))?;
        assert_eq!(stored.name, "Unchanged");
        assert_eq!(entry_tracks(&db, playlist_db_id)?, vec![kept]);
        Ok(())
    }

    #[test]
    fn summaries_count_entries_and_sum_known_durations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("summaries")?)?;
        let short = insert_track_with_duration(&mut db, "Short", 1_000)?;
        let long = insert_track_with_duration(&mut db, "Long", 2_500)?;
        let unknown = insert_track(&mut db, "Unknown Duration")?;

        let filled = create_playlist(&mut db, user_db_id, "Filled")?;
        let empty = create_playlist(&mut db, user_db_id, "Empty")?;
        add_tracks(
            &mut db,
            QueryId::Id(filled),
            &[
                QueryId::Id(short),
                QueryId::Id(long),
                QueryId::Id(unknown),
                // A duplicate entry counts twice.
                QueryId::Id(short),
            ],
            None,
        )?;

        let summaries = summaries(
            &db,
            &principal(user_db_id, vec![lyra_db::Permission::Admin]),
            &[filled, empty],
        )?;
        let filled_summary = summaries.get(&filled).copied().unwrap_or_default();
        assert_eq!(filled_summary.track_count, 4);
        assert_eq!(filled_summary.total_duration_ms, 1_000 + 2_500 + 1_000);

        let empty_summary = summaries.get(&empty).copied().unwrap_or_default();
        assert_eq!(empty_summary.track_count, 0);
        assert_eq!(empty_summary.total_duration_ms, 0);

        Ok(())
    }

    #[test]
    fn summaries_withhold_durations_of_inaccessible_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("summary-access")?)?;
        let track = insert_track_with_duration(&mut db, "Private", 9_000)?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Public Playlist")?;
        add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &[QueryId::Id(track)],
            None,
        )?;

        // The track belongs to no library this principal can reach. The entry
        // still counts — `inc=tracks` shows it as `unavailable` — but its
        // duration must not leak, matching `TrackResponse::unavailable`.
        let viewer = principal(user_db_id, Vec::new());
        let summary = summaries(&db, &viewer, &[playlist_db_id])?
            .get(&playlist_db_id)
            .copied()
            .unwrap_or_default();
        assert_eq!(summary.track_count, 1);
        assert_eq!(summary.total_duration_ms, 0);

        let admin = principal(user_db_id, vec![lyra_db::Permission::Admin]);
        let summary = summaries(&db, &admin, &[playlist_db_id])?
            .get(&playlist_db_id)
            .copied()
            .unwrap_or_default();
        assert_eq!(summary.track_count, 1);
        assert_eq!(summary.total_duration_ms, 9_000);

        Ok(())
    }

    #[test]
    fn summaries_follow_entry_removal() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = lyra_db::users::create(&mut db, &test_user("summary-removal")?)?;
        let track = insert_track_with_duration(&mut db, "Track", 4_000)?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Shrinking")?;
        let added = add_tracks(
            &mut db,
            QueryId::Id(playlist_db_id),
            &[QueryId::Id(track)],
            None,
        )?;

        remove_tracks(&mut db, playlist_db_id, &[added[0].entry_id.clone()])?;

        let summary = summaries(
            &db,
            &principal(user_db_id, vec![lyra_db::Permission::Admin]),
            &[playlist_db_id],
        )?
        .get(&playlist_db_id)
        .copied()
        .unwrap_or_default();
        assert_eq!(summary.track_count, 0);
        assert_eq!(summary.total_duration_ms, 0);

        Ok(())
    }

    /// Times writes at the id cap on an mmap database, as the server opens it. Run with
    /// `cargo test -p lyra-server --release -- --ignored playlist_writes_at_the_cap --nocapture`.
    #[test]
    #[ignore = "timing; run in release"]
    fn playlist_writes_at_the_cap() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let created = lyra_db::bootstrap::create(&lyra_db::bootstrap::DbConfig {
            kind: lyra_db::bootstrap::DbKind::Mmap,
            path: dir.path().join("timing.agdb"),
        })?;
        let mut db = futures::executor::block_on(created.db.write());
        let db = &mut *db;
        let user_db_id = lyra_db::users::create(db, &test_user("timing")?)?;
        let tracks = (0..MAX_PLAYLIST_TRACK_IDS)
            .map(|index| insert_track(db, &format!("Track {index}")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let track_ids = tracks.iter().copied().map(QueryId::Id).collect::<Vec<_>>();
        let time = |label: &str, started: std::time::Instant| {
            println!("{label}: {:?}", started.elapsed());
        };

        let public = tracks
            .iter()
            .map(|&id| lyra_db::lookup::find_id_by_db_id(db, id).map(Option::unwrap))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let viewer = principal(user_db_id, Vec::new());
        let started = std::time::Instant::now();
        let resolved = public
            .iter()
            .map(|id| lyra_db::lookup::find_node_id_by_id(db, id).map(Option::unwrap))
            .collect::<anyhow::Result<Vec<_>>>()?;
        access::accessible_entities(db, &viewer, &resolved)?;
        for &id in &resolved {
            lyra_db::tracks::get_by_id(db, id)?;
        }
        time(
            "resolve and check the cap's track ids, as the bindings do",
            started,
        );
        let started = std::time::Instant::now();
        let (full, _) = create(
            db,
            &CreatePlaylistRequest {
                user_db_id,
                name: "Full".to_string(),
                description: None,
                is_public: None,
                created_at_ms: None,
                updated_at_ms: None,
                track_db_ids: tracks.clone(),
            },
        )?;
        time("create with the cap", started);

        let started = std::time::Instant::now();
        replace(db, &replacement(full, None), &tracks)?;
        time("replace a full playlist with the cap", started);

        let empty = create_playlist(db, user_db_id, "Empty")?;
        let started = std::time::Instant::now();
        add_tracks(db, QueryId::Id(empty), &track_ids, None)?;
        time("add the cap to an empty playlist", started);

        let existing = create_playlist(db, user_db_id, "Existing")?;
        add_tracks(db, QueryId::Id(existing), &track_ids[..100], None)?;
        let started = std::time::Instant::now();
        add_tracks(db, QueryId::Id(existing), &track_ids, Some(50))?;
        time("insert the cap into a 100-entry playlist", started);

        let started = std::time::Instant::now();
        add_tracks(db, QueryId::Id(full), &track_ids[..1], Some(0))?;
        time("insert one at the start of a full playlist", started);
        let last = get_tracks(db, QueryId::Id(full))?
            .pop()
            .ok_or_else(|| anyhow::anyhow!("a full playlist"))?;
        let started = std::time::Instant::now();
        move_track(db, full, &last.entry_id, 0)?;
        time(
            "move the last entry of a full playlist to the start",
            started,
        );

        let first = get_tracks(db, QueryId::Id(full))?[0].entry_id.clone();
        let started = std::time::Instant::now();
        remove_tracks(db, full, &[first])?;
        time("remove the first entry of a full playlist", started);

        let entry_ids = get_tracks(db, QueryId::Id(full))?
            .into_iter()
            .map(|link| link.entry_id)
            .take(MAX_PLAYLIST_TRACK_IDS)
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        remove_tracks(db, full, &entry_ids)?;
        time("remove the cap's entries", started);
        Ok(())
    }
}
