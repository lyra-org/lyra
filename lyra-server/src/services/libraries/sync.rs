// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use std::collections::BTreeMap;

use std::collections::BTreeSet;

use super::scanning::prepare_entries;
use crate::{
    Library,
    db::{
        self,
        DbAsync,
        entries::sync_entries,
    },
    services::{
        covers::eager_sync_cover_metadata,
        metadata::{
            cleanup::cleanup_orphaned_metadata,
            ingestion::{
                ParsedMetadataGroup,
                apply_metadata,
                coalesce_disc_groups,
                group_entries,
                source_directory_for_group_entries,
            },
            log_skip_summary,
            lyrics::providers::{
                MAX_CONCURRENT_DISPATCHES,
                dispatch_for_track as dispatch_lyrics_for_track,
            },
            parse_metadata,
        },
        providers::{
            LibraryRefreshOptions,
            refresh_library_metadata,
        },
    },
};

pub(crate) async fn full_sync(db: &DbAsync, library: &Library) -> anyhow::Result<Vec<DbId>> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    let existing = {
        let db_read = db.read().await;
        db::entries::get(&db_read, library_db_id)?
    };
    let prepared = prepare_entries(library, existing)?;
    let mut db_write = db.write().await;
    sync_entries(&mut db_write, library, prepared)
}

pub(crate) async fn add_metadata(
    db: &DbAsync,
    library: &Library,
    entries: Vec<DbId>,
) -> anyhow::Result<()> {
    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    if entries.is_empty() {
        return Ok(());
    }

    // Cloned for the lyrics-dispatch hook below; `group_entries` consumes the original.
    let touched_entries = entries.clone();

    let groups = {
        let db_read = db.read().await;
        group_entries(&db_read, library_db_id, entries)?
    };

    let mut parsed_groups = Vec::new();
    for (coalesce_group_key, entries) in groups.into_iter().enumerate() {
        let source_dir =
            source_directory_for_group_entries(&entries).unwrap_or_else(|| library.path.clone());
        let entry_source_dirs: BTreeMap<DbId, std::path::PathBuf> = entries
            .iter()
            .filter_map(|entry| {
                if entry.kind != crate::db::entries::EntryKind::File {
                    return None;
                }
                let entry_db_id = entry.db_id?;
                let parent = entry
                    .full_path
                    .parent()
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| source_dir.clone());
                Some((entry_db_id, parent))
            })
            .collect();
        let parse_output = parse_metadata(entries).await?;
        if !parse_output.skipped.is_empty() {
            log_skip_summary(&parse_output.skipped);
        }
        let metadata = parse_output.metadata;
        if metadata.is_empty() {
            continue;
        }

        let mut metadata_by_source_dir = BTreeMap::new();
        for track in metadata {
            let track_source_dir = entry_source_dirs
                .get(&track.entry_db_id)
                .cloned()
                .unwrap_or_else(|| source_dir.clone());
            metadata_by_source_dir
                .entry(track_source_dir)
                .or_insert_with(Vec::new)
                .push(track);
        }

        for (source_dir, metadata) in metadata_by_source_dir {
            parsed_groups.push(ParsedMetadataGroup {
                coalesce_group_key,
                source_dir,
                metadata,
            });
        }
    }

    for metadata in coalesce_disc_groups(parsed_groups) {
        if metadata.is_empty() {
            continue;
        }
        let mut db_write = db.write().await;
        apply_metadata(&mut db_write, library_db_id, metadata)?;
    }

    let mut db_write = db.write().await;
    cleanup_orphaned_metadata(&mut db_write)?;
    drop(db_write);

    // Dedupe entries → tracks (cue sheets fan one entry to many; one track
    // is reachable from several entries).
    let track_ids: Vec<DbId> = {
        let db_read = db.read().await;
        let mut seen = BTreeSet::new();
        for entry_db_id in touched_entries {
            for track in db::tracks::get_by_entry(&db_read, entry_db_id)? {
                if let Some(track_db_id) = track.db_id.map(DbId::from) {
                    seen.insert(track_db_id);
                }
            }
        }
        seen.into_iter().collect()
    };
    if !track_ids.is_empty() {
        tokio::spawn(async move {
            use futures::stream::{
                self,
                StreamExt,
            };
            stream::iter(track_ids)
                .for_each_concurrent(MAX_CONCURRENT_DISPATCHES, |track_db_id| async move {
                    if let Err(err) = dispatch_lyrics_for_track(track_db_id, false).await {
                        tracing::warn!(
                            track_db_id = track_db_id.0,
                            error = %err,
                            "lyrics dispatch failed for newly-added track"
                        );
                    }
                })
                .await;
        });
    }

    Ok(())
}

pub(crate) async fn sync_library(db: &DbAsync, library: &Library) -> anyhow::Result<()> {
    let result = full_sync(db, library).await?;
    add_metadata(db, library, result).await?;

    let library_db_id = library
        .db_id
        .ok_or_else(|| anyhow::anyhow!("library missing db_id"))?;

    eager_sync_cover_metadata(db, library_db_id, &library.path).await;

    let options = LibraryRefreshOptions {
        replace_cover: false,
        force_refresh: false,
        apply_sync_filters: false,
        provider_id: None,
    };
    if let Err(err) = refresh_library_metadata(library_db_id, &options).await {
        tracing::warn!(
            library_db_id = library_db_id.0,
            error = %err,
            "provider refresh failed during library sync"
        );
    }
    Ok(())
}
