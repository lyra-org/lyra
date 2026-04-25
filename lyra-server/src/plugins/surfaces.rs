// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use anyhow::Result;
use harmony_core::Module;

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
    metadata,
    mix,
    playback_sessions,
    playback_sources,
    playlists,
    releases,
    runtime,
    server,
    tags,
    track_sources,
    tracks,
    users,
};

type RenderDocsFn = fn() -> Result<String>;

struct Surface {
    id: &'static str,
    module: fn() -> Module,
    render_docs: RenderDocsFn,
}

macro_rules! surface {
    ($id:literal, $module:path, $render:path) => {
        Surface {
            id: $id,
            module: $module,
            render_docs: || $render().map_err(anyhow::Error::from),
        }
    };
}

pub(crate) fn lyra_modules() -> Vec<Module> {
    surfaces()
        .iter()
        .map(|surface| (surface.module)())
        .collect()
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
        surface!(
            "lyra/labels",
            labels::get_module,
            labels::render_luau_definition
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
            runtime::get_module,
            runtime::render_luau_definition
        ),
    ]
}
