// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::DbId;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::DescribeInterface;
use serde::Serialize;

use crate::{
    plugins::{
        args::UniqueIds,
        catalog::{
            self,
            KeyedLookup,
        },
        db::ViewerDb,
        types::EntryInfo,
    },
    services::{
        catalog::Viewer,
        entries::{
            self,
            PlacedEntry,
        },
    },
};

pub(crate) struct EntriesModule;

#[harmony_macros::module(id = "lyra/entries", docs_feature = "docgen")]
impl EntriesModule {
    #[harmony(
        description = "The entries with these ids that the caller can see, keyed by id.",
        returns(HashMap<u64, EntryInfo>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(&db, &ids.0, entries::get, EntryInfo::shown_to).await
    }

    #[harmony(
        description = "The entries directly under each of these libraries and directory entries that the caller can see, by name, keyed by parent id.",
        returns(HashMap<u64, Vec<EntryInfo>>)
    )]
    async fn by_parent(
        parent_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(&db, &parent_ids.0, entries::by_parent, show_all).await
    }

    #[harmony(
        description = "The entries holding the audio of each of these tracks that the caller can see, keyed by track id.",
        returns(HashMap<u64, Vec<EntryInfo>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(&db, &track_ids.0, entries::by_track, show_all).await
    }
}

async fn keyed<T, R: Serialize>(
    db: &ViewerDb,
    ids: &[DbId],
    lookup: KeyedLookup<T>,
    show: fn(T, &Viewer) -> R,
) -> luau::runtime::Result<luau::OwnedTable> {
    let (db, viewer) = db.read().await?;
    let found = lookup(&db, &viewer, ids).map_err(catalog::error)?;
    let mut table = luau::OwnedTable::with_entry_capacity(0, 0, found.len());
    for (id, value) in found {
        table.set_key(
            id.0,
            harmony_luau::serializable_to_luau_owned(show(value, &viewer))?,
        );
    }
    Ok(table)
}

fn show_all(entries: Vec<PlacedEntry>, viewer: &Viewer) -> Vec<EntryInfo> {
    entries
        .into_iter()
        .map(|entry| EntryInfo::shown_to(entry, viewer))
        .collect()
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &EntriesModule::module_descriptor(),
        &[],
        &[EntryInfo::interface_descriptor()],
        &[],
    )
}
