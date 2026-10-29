// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The scopes a plugin manifest may declare, and the module each one gates.

use harmony_core::{
    GatedModule,
    ModuleSpec,
    plugin::PluginHost,
};
use semver::Version;

use crate::plugins::{
    api,
    artists,
    auth,
    chromaprint,
    covers,
    datastore,
    editions,
    entities,
    entries,
    favorites,
    genres,
    ids,
    images,
    labels,
    libraries,
    listens,
    locale,
    lyrics,
    manifests,
    metadata,
    mix,
    playback_sessions,
    playback_sources,
    playlists,
    ratings,
    releases,
    server,
    tags,
    track_sources,
    tracks,
    users,
};

pub(crate) struct ScopeDef {
    pub(crate) id: &'static str,
    pub(crate) module: fn() -> ModuleSpec,
}

/// Declares every scope once: the enum, `Scope::ALL`, and each scope's
/// definition all come from the same row, so none can miss a scope.
macro_rules! scopes {
    ($($scope:ident => { id: $id:expr, module: $module:path $(,)? }),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum Scope {
            $($scope),+
        }

        impl Scope {
            pub(crate) const ALL: &[Scope] = &[$(Scope::$scope),+];

            pub(crate) fn def(self) -> ScopeDef {
                match self {
                    $(Scope::$scope => ScopeDef {
                        id: $id,
                        module: $module,
                    }),+
                }
            }
        }
    };
}

scopes! {
    HarmonyCrypt => {
        id: harmony_crypt::SCOPE,
        module: harmony_crypt::module_spec,
    },
    HarmonyHttp => {
        id: harmony_http::SCOPE,
        module: harmony_http::module_spec,
    },
    HarmonyNet => {
        id: harmony_net::SCOPE,
        module: harmony_net::module_spec,
    },
    HarmonySerde => {
        id: harmony_serde::SCOPE,
        module: harmony_serde::module_spec,
    },
    HarmonyTask => {
        id: harmony_task::SCOPE,
        module: harmony_task::module_spec,
    },
    Api => {
        id: "lyra.api",
        module: api::module_spec,
    },
    Artists => {
        id: "lyra.artists",
        module: artists::module_spec,
    },
    Auth => {
        id: "lyra.auth",
        module: auth::module_spec,
    },
    Chromaprint => {
        id: "lyra.chromaprint",
        module: chromaprint::module_spec,
    },
    Covers => {
        id: "lyra.covers",
        module: covers::module_spec,
    },
    Datastore => {
        id: "lyra.datastore",
        module: datastore::module_spec,
    },
    Editions => {
        id: "lyra.editions",
        module: editions::module_spec,
    },
    Entities => {
        id: "lyra.entities",
        module: entities::module_spec,
    },
    Entries => {
        id: "lyra.entries",
        module: entries::module_spec,
    },
    Favorites => {
        id: "lyra.favorites",
        module: favorites::module_spec,
    },
    Genres => {
        id: "lyra.genres",
        module: genres::module_spec,
    },
    Ids => {
        id: "lyra.ids",
        module: ids::module_spec,
    },
    Images => {
        id: "lyra.images",
        module: images::module_spec,
    },
    Labels => {
        id: "lyra.labels",
        module: labels::module_spec,
    },
    Libraries => {
        id: "lyra.libraries",
        module: libraries::module_spec,
    },
    Listens => {
        id: "lyra.listens",
        module: listens::module_spec,
    },
    Locale => {
        id: "lyra.locale",
        module: locale::module_spec,
    },
    Lyrics => {
        id: "lyra.lyrics",
        module: lyrics::module_spec,
    },
    Metadata => {
        id: "lyra.metadata",
        module: metadata::module_spec,
    },
    Mix => {
        id: "lyra.mix",
        module: mix::module_spec,
    },
    PlaybackSessions => {
        id: "lyra.playback_sessions",
        module: playback_sessions::module_spec,
    },
    PlaybackSources => {
        id: "lyra.playback_sources",
        module: playback_sources::module_spec,
    },
    Playlists => {
        id: "lyra.playlists",
        module: playlists::module_spec,
    },
    Plugins => {
        id: "lyra.plugins",
        module: manifests::module_spec,
    },
    Ratings => {
        id: "lyra.ratings",
        module: ratings::module_spec,
    },
    Releases => {
        id: "lyra.releases",
        module: releases::module_spec,
    },
    Server => {
        id: "lyra.server",
        module: server::module_spec,
    },
    Tags => {
        id: "lyra.tags",
        module: tags::module_spec,
    },
    TrackSources => {
        id: "lyra.track_sources",
        module: track_sources::module_spec,
    },
    Tracks => {
        id: "lyra.tracks",
        module: tracks::module_spec,
    },
    Users => {
        id: "lyra.users",
        module: users::module_spec,
    },
}

/// Every module plugins can `require`, behind its scope.
pub(crate) fn gated_modules() -> Vec<GatedModule> {
    Scope::ALL
        .iter()
        .map(|scope| {
            let def = scope.def();
            GatedModule::new(def.id, (def.module)())
        })
        .collect()
}

pub(crate) fn plugin_host() -> PluginHost {
    PluginHost::new(
        "lyra",
        Version::parse(env!("CARGO_PKG_VERSION")).expect("package version is semver"),
        Scope::ALL
            .iter()
            .map(|scope| scope.def().id.into())
            .collect(),
    )
    .expect("lyra is not a fixed manifest field")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Scope;

    #[test]
    fn scopes_have_unique_ids_and_modules() {
        let mut ids = HashSet::new();
        let mut modules = HashSet::new();
        for scope in Scope::ALL {
            let def = scope.def();
            assert!(ids.insert(def.id), "scope id {} is listed twice", def.id);
            let module_id = (def.module)().id.0;
            assert!(
                modules.insert(module_id.clone()),
                "module {module_id} is gated twice"
            );
        }
    }
}
