// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use anyhow::Result;
use harmony_core::ModuleSpec;

use crate::plugins::{
    api,
    artists,
    auth,
    chromaprint,
    covers,
    datastore,
    entities,
    entries,
    favorites,
    genres,
    ids,
    images,
    labels,
    libraries,
    listens,
    lyrics,
    manifests,
    metadata,
    mix,
    playback_sessions,
    playback_sources,
    playlists,
    releases,
    server,
    tags,
    track_sources,
    tracks,
    users,
};

type RenderDocsFn = fn() -> Result<String>;

struct Surface {
    id: &'static str,
    render_docs: RenderDocsFn,
}

macro_rules! surface {
    ($id:literal, $module:path, $render:path) => {
        Surface {
            id: $id,
            render_docs: || $render().map_err(anyhow::Error::from),
        }
    };
}

pub(crate) fn lyra_doc_source_ids() -> impl Iterator<Item = &'static str> {
    surfaces().iter().map(|surface| surface.id)
}

pub(crate) fn render_lyra_doc_source(id: &str) -> Result<Option<String>> {
    surfaces()
        .iter()
        .find(|surface| surface.id == id)
        .map(|surface| (surface.render_docs)().map(Some))
        .transpose()
        .map(Option::flatten)
}

pub(crate) fn module_specs() -> Vec<ModuleSpec> {
    vec![
        api::module_spec(),
        artists::module_spec(),
        auth::module_spec(),
        chromaprint::module_spec(),
        covers::module_spec(),
        harmony_crypt::module_spec(),
        datastore::module_spec(),
        entities::module_spec(),
        entries::module_spec(),
        favorites::module_spec(),
        genres::module_spec(),
        harmony_http::module_spec(),
        ids::module_spec(),
        images::module_spec(),
        labels::module_spec(),
        libraries::module_spec(),
        listens::module_spec(),
        harmony_json::module_spec(),
        lyrics::module_spec(),
        metadata::module_spec(),
        mix::module_spec(),
        harmony_net::module_spec(),
        playback_sessions::module_spec(),
        playback_sources::module_spec(),
        playlists::module_spec(),
        releases::module_spec(),
        server::module_spec(),
        harmony_task::module_spec(),
        tags::module_spec(),
        track_sources::module_spec(),
        tracks::module_spec(),
        users::module_spec(),
        manifests::module_spec(),
    ]
}

pub(crate) fn module_scope_ids() -> HashSet<Arc<str>> {
    module_specs()
        .into_iter()
        .filter_map(|spec| spec.capability.map(|capability| capability.0))
        .collect()
}

fn surfaces() -> &'static [Surface] {
    &[
        surface!(
            "lyra/releases",
            releases::get_module,
            releases::render_luau_definition
        ),
        surface!("lyra/api", api::get_module, api::render_luau_definition),
        surface!(
            "lyra/artists",
            artists::get_module,
            artists::render_luau_definition
        ),
        surface!("lyra/auth", auth::get_module, auth::render_luau_definition),
        surface!(
            "lyra/chromaprint",
            chromaprint::get_module,
            chromaprint::render_luau_definition
        ),
        surface!(
            "lyra/covers",
            covers::get_module,
            covers::render_luau_definition
        ),
        surface!(
            "lyra/genres",
            genres::get_module,
            genres::render_luau_definition
        ),
        surface!("lyra/tags", tags::get_module, tags::render_luau_definition),
        surface!(
            "lyra/favorites",
            favorites::get_module,
            favorites::render_luau_definition
        ),
        surface!(
            "lyra/track_sources",
            track_sources::get_module,
            track_sources::render_luau_definition
        ),
        surface!(
            "lyra/tracks",
            tracks::get_module,
            tracks::render_luau_definition
        ),
        surface!(
            "lyra/entities",
            entities::get_module,
            entities::render_luau_definition
        ),
        surface!("lyra/ids", ids::get_module, ids::render_luau_definition),
        surface!(
            "lyra/images",
            images::get_module,
            images::render_luau_definition
        ),
        surface!(
            "lyra/labels",
            labels::module_spec,
            labels::render_luau_definition
        ),
        surface!(
            "lyra/entries",
            entries::get_module,
            entries::render_luau_definition
        ),
        surface!(
            "lyra/libraries",
            libraries::get_module,
            libraries::render_luau_definition
        ),
        surface!(
            "lyra/datastore",
            datastore::get_module,
            datastore::render_luau_definition
        ),
        surface!(
            "lyra/metadata",
            metadata::get_module,
            metadata::render_luau_definition
        ),
        surface!("lyra/mix", mix::get_module, mix::render_luau_definition),
        surface!(
            "lyra/listens",
            listens::get_module,
            listens::render_luau_definition
        ),
        surface!(
            "lyra/lyrics",
            lyrics::get_module,
            lyrics::render_luau_definition
        ),
        surface!(
            "lyra/playback_sources",
            playback_sources::get_module,
            playback_sources::render_luau_definition
        ),
        surface!(
            "lyra/playlists",
            playlists::get_module,
            playlists::render_luau_definition
        ),
        surface!(
            "lyra/playback_sessions",
            playback_sessions::get_module,
            playback_sessions::render_luau_definition
        ),
        surface!(
            "lyra/server",
            server::get_module,
            server::render_luau_definition
        ),
        surface!(
            "lyra/users",
            users::get_module,
            users::render_luau_definition
        ),
        surface!(
            "lyra/plugins",
            manifests::module_spec,
            manifests::render_luau_definition
        ),
    ]
}
