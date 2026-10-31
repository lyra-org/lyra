// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::DescribeInterface;

use crate::{
    plugins::{
        args::UniqueIds,
        catalog::{
            self,
            KeyedLookup,
        },
        db::ViewerDb,
        ids::Resolved,
        types::EntryInfo,
    },
    services::entries::{
        self,
        PlacedEntry,
    },
};

pub(crate) struct EntriesModule;

#[harmony_macros::module(id = "lyra/entries", docs_feature = "docgen")]
impl EntriesModule {
    #[harmony(
        description = "The entries with these ids that the caller can see, keyed by id.",
        returns(HashMap<String, EntryInfo>)
    )]
    async fn get(
        ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(
            &db,
            ids.0,
            entries::get,
            |entry| vec![entry],
            |mut infos| harmony_luau::serializable_to_luau_owned(infos.pop()),
        )
        .await
    }

    #[harmony(
        description = "The entries directly under each of these libraries and directory entries that the caller can see, by name, keyed by parent id.",
        returns(HashMap<String, Vec<EntryInfo>>)
    )]
    async fn by_parent(
        parent_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(
            &db,
            parent_ids.0,
            entries::by_parent,
            |entries| entries,
            harmony_luau::serializable_to_luau_owned,
        )
        .await
    }

    #[harmony(
        description = "The entries holding the audio of each of these tracks that the caller can see, keyed by track id.",
        returns(HashMap<String, Vec<EntryInfo>>)
    )]
    async fn by_track(
        track_ids: UniqueIds,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        keyed(
            &db,
            track_ids.0,
            entries::by_track,
            |entries| entries,
            harmony_luau::serializable_to_luau_owned,
        )
        .await
    }
}

/// The entries `lookup` finds for each id as the viewer reads them, keyed by id.
async fn keyed<T>(
    db: &ViewerDb,
    ids: Vec<String>,
    lookup: KeyedLookup<T>,
    entries_of: fn(T) -> Vec<PlacedEntry>,
    value_of: fn(Vec<EntryInfo>) -> luau::runtime::Result<luau::Value>,
) -> luau::runtime::Result<luau::OwnedTable> {
    let (db, viewer) = db.read().await?;
    let ids = Resolved::of(&*db, ids)?;
    let found = lookup(&db, &viewer, ids.db_ids())
        .map_err(catalog::error)?
        .into_iter()
        .map(|(id, value)| (id, entries_of(value)))
        .collect::<Vec<_>>();
    let parents = EntryInfo::parents(&*db, found.iter().flat_map(|(_, entries)| entries))?;
    let full_path = EntryInfo::shows_full_path(&viewer);
    let mut values = Vec::with_capacity(found.len());
    for (id, entries) in found {
        let infos = entries
            .into_iter()
            .map(|entry| EntryInfo::new(entry, &parents, full_path))
            .collect::<luau::runtime::Result<Vec<_>>>()?;
        values.push((id, value_of(infos)?));
    }
    Ok(ids.keyed(values))
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
