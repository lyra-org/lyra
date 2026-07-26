// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
    QueryBuilder,
    QueryId,
};
use anyhow::bail;
use nanoid::nanoid;

use crate::{
    db::{
        self,
        Playlist,
    },
    services::auth::Principal,
};

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
    pub(crate) created_at: Option<u64>,
    pub(crate) updated_at: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct UpdatePlaylistRequest {
    pub(crate) playlist_id: QueryId,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) is_public: Option<bool>,
    pub(crate) updated_at: Option<u64>,
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
    db::playlists::get(db)
}

pub(crate) fn get(db: &DbAny, query_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, query_id)? else {
        return Ok(None);
    };
    db::playlists::get_by_id(db, playlist_db_id)
}

pub(crate) fn get_by_user(db: &DbAny, user_db_id: DbId) -> anyhow::Result<Vec<Playlist>> {
    db::playlists::get_by_user(db, user_db_id)
}

pub(crate) fn get_owner(db: &DbAny, playlist_id: QueryId) -> anyhow::Result<Option<DbId>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    db::playlists::get_owner(db, playlist_db_id)
}

pub(crate) fn get_tracks(
    db: &DbAny,
    playlist_id: QueryId,
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;

    let playlist_tracks = db::playlists::get_tracks(db, playlist_db_id)?;
    let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|track| track.edge_id).collect();
    let track_ids = db::playlists::resolve_edge_targets(db, &edge_ids)?;

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
    let raw = db::playlists::get_tracks_many(db, playlist_db_ids)?;
    let mut result = HashMap::new();
    for (playlist_id, playlist_tracks) in raw {
        let edge_ids: Vec<DbId> = playlist_tracks.iter().map(|t| t.edge_id).collect();
        let track_ids = db::playlists::resolve_edge_targets(db, &edge_ids)?;
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
    let tracks = db::tracks::get_by_ids(db, &track_db_ids)?;
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

pub(crate) fn create(db: &mut DbAny, request: &CreatePlaylistRequest) -> anyhow::Result<DbId> {
    let playlist = Playlist {
        db_id: None,
        id: nanoid!(),
        name: validate_name(&request.name)?,
        description: request.description.clone(),
        is_public: request.is_public,
        created_at: request.created_at,
        updated_at: request.updated_at,
    };

    db::playlists::create(db, &playlist, request.user_db_id)
}

pub(crate) fn delete(db: &mut DbAny, playlist_id: QueryId) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, playlist_id)? else {
        return Ok(None);
    };
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };

    db::playlists::delete(db, playlist_db_id)?;
    Ok(Some(playlist))
}

pub(crate) fn update(
    db: &mut DbAny,
    request: &UpdatePlaylistRequest,
) -> anyhow::Result<Option<Playlist>> {
    let Some(playlist_db_id) = resolve_optional_id(db, request.playlist_id.clone())? else {
        return Ok(None);
    };
    let Some(mut playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(None);
    };

    if let Some(name) = &request.name {
        playlist.name = validate_name(name)?;
    }
    if let Some(description) = &request.description {
        playlist.description = Some(description.clone());
    }
    if let Some(is_public) = request.is_public {
        playlist.is_public = Some(is_public);
    }
    if let Some(updated_at) = request.updated_at {
        playlist.updated_at = Some(updated_at);
    }

    db::playlists::update(db, &playlist)?;
    Ok(Some(playlist))
}

pub(crate) fn add_track(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_db_id: QueryId,
) -> anyhow::Result<db::playlists::PlaylistTrack> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let track_db_id = resolve_id(db, track_db_id)?;
    db.transaction_mut(|t| {
        let added = db::playlists::add_track(t, playlist_db_id, track_db_id)?;
        db::covers::display::offer_track_to_playlist_cover(t, playlist_db_id, track_db_id)?;
        Ok(added)
    })
}

pub(crate) fn add_tracks(
    db: &mut DbAny,
    playlist_id: QueryId,
    track_ids: &[QueryId],
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let mut resolved_track_ids = Vec::with_capacity(track_ids.len());
    for track_id in track_ids {
        let track_db_id = resolve_id(db, track_id.clone())?;
        if db::tracks::get_by_id(db, track_db_id)?.is_none() {
            bail!("track not found: {}", track_db_id.0);
        }
        resolved_track_ids.push(track_db_id);
    }

    let results = db.transaction_mut(|t| -> anyhow::Result<_> {
        let results = db::playlists::add_tracks(t, playlist_db_id, &resolved_track_ids)?;
        db::covers::display::sync_playlist_cover(t, playlist_db_id)?;
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

pub(crate) fn remove_track(db: &mut DbAny, entry_db_id: QueryId) -> anyhow::Result<()> {
    let entry_db_id = resolve_id(db, entry_db_id)?;
    let playlist_db_id = get_playlist_for_entry(db, entry_db_id)?;
    db.transaction_mut(|t| {
        db::playlists::remove_track(t, entry_db_id)?;
        if let Some(playlist_db_id) = playlist_db_id {
            db::covers::display::sync_playlist_cover(t, playlist_db_id)?;
        }
        Ok(())
    })
}

pub(crate) fn get_playlist_for_entry(
    db: &DbAny,
    entry_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    let result = db.exec(QueryBuilder::select().ids(entry_db_id).query())?;
    Ok(result
        .elements
        .first()
        .and_then(|element| (element.id.0 < 0 && element.from.0 != 0).then_some(element.from)))
}

pub(crate) fn remove_tracks(
    db: &mut DbAny,
    playlist_id: QueryId,
    entry_ids: &[QueryId],
) -> anyhow::Result<Vec<PlaylistTrackLink>> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let existing_tracks = get_tracks(db, QueryId::Id(playlist_db_id))?;
    let mut removed = Vec::with_capacity(entry_ids.len());

    for entry_id in entry_ids {
        let entry_db_id = resolve_id(db, entry_id.clone())?;
        let track = existing_tracks
            .iter()
            .find(|track| track.entry_db_id == entry_db_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("playlist entry not found: {}", entry_db_id.0))?;
        removed.push(track);
    }

    db.transaction_mut(|t| -> anyhow::Result<()> {
        for track in &removed {
            db::playlists::remove_track(t, track.entry_db_id)?;
        }
        db::covers::display::sync_playlist_cover(t, playlist_db_id)
    })?;

    Ok(removed)
}

pub(crate) fn move_track(
    db: &mut DbAny,
    playlist_id: QueryId,
    entry_id: QueryId,
    new_position: u64,
) -> anyhow::Result<()> {
    let playlist_db_id = resolve_id(db, playlist_id)?;
    let entry_db_id = resolve_id(db, entry_id)?;
    db.transaction_mut(|t| db::playlists::move_track(t, playlist_db_id, entry_db_id, new_position))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_track,
        new_test_db,
        test_user,
    };

    fn insert_track_with_duration(
        db: &mut DbAny,
        title: &str,
        duration_ms: u64,
    ) -> anyhow::Result<DbId> {
        let track_db_id = insert_track(db, title)?;
        let mut track = db::tracks::get_by_id(db, track_db_id)?
            .ok_or_else(|| anyhow::anyhow!("track missing after insert"))?;
        track.set_duration_ms(duration_ms);
        db.exec_mut(QueryBuilder::insert().element(&track).query())?;
        Ok(track_db_id)
    }

    fn principal(user_db_id: DbId, permissions: Vec<db::Permission>) -> Principal {
        Principal {
            user_db_id,
            user_public_id: "summary-principal".to_string(),
            username: "summary-principal".to_string(),
            permissions,
            role_name: None,
            accessible_library_ids: std::collections::HashSet::new(),
        }
    }

    fn create_playlist(db: &mut DbAny, user_db_id: DbId, name: &str) -> anyhow::Result<DbId> {
        create(
            db,
            &CreatePlaylistRequest {
                user_db_id,
                name: name.to_string(),
                description: None,
                is_public: None,
                created_at: None,
                updated_at: None,
            },
        )
    }

    #[test]
    fn summaries_count_entries_and_sum_known_durations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_db_id = db::users::create(&mut db, &test_user("summaries")?)?;
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
        )?;

        let summaries = summaries(
            &db,
            &principal(user_db_id, vec![db::Permission::Admin]),
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
        let user_db_id = db::users::create(&mut db, &test_user("summary-access")?)?;
        let track = insert_track_with_duration(&mut db, "Private", 9_000)?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Public Playlist")?;
        add_tracks(&mut db, QueryId::Id(playlist_db_id), &[QueryId::Id(track)])?;

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

        let admin = principal(user_db_id, vec![db::Permission::Admin]);
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
        let user_db_id = db::users::create(&mut db, &test_user("summary-removal")?)?;
        let track = insert_track_with_duration(&mut db, "Track", 4_000)?;
        let playlist_db_id = create_playlist(&mut db, user_db_id, "Shrinking")?;
        let added = add_tracks(&mut db, QueryId::Id(playlist_db_id), &[QueryId::Id(track)])?;

        remove_track(&mut db, QueryId::Id(added[0].entry_db_id))?;

        let summary = summaries(
            &db,
            &principal(user_db_id, vec![db::Permission::Admin]),
            &[playlist_db_id],
        )?
        .get(&playlist_db_id)
        .copied()
        .unwrap_or_default();
        assert_eq!(summary.track_count, 0);
        assert_eq!(summary.total_duration_ms, 0);

        Ok(())
    }
}
