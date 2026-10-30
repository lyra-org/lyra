// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The scopes a plugin manifest may declare, the module each one gates, and
//! what each lets a plugin do.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use harmony_core::{
    GatedModule,
    ModuleSpec,
    plugin::PluginHost,
    scopes::{
        EffectiveScopes,
        ScopeGrowth,
        ScopedPlugin,
        scope_growth,
    },
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

/// What a scope lets a plugin do, as admins weigh it. Reading the shared
/// library is not listed: every plugin may.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Effect {
    /// Sends data off the server.
    Egress,
    /// Adds endpoints to the server.
    Exposes,
    /// Handles credentials and sign-in sessions.
    Authority,
    /// Reads the named data of the users it acts for.
    ReadsPersonal(&'static str),
    /// Changes the named data of the users it acts for.
    WritesPersonal(&'static str),
    /// Changes the named data of the shared library.
    WritesLibrary(&'static str),
}

pub(crate) struct ScopeDef {
    pub(crate) id: &'static str,
    pub(crate) module: fn() -> ModuleSpec,
    pub(crate) effects: &'static [Effect],
}

/// Declares every scope once: the enum, `Scope::ALL`, and each scope's
/// definition all come from the same row, so none can miss a scope.
macro_rules! scopes {
    ($(
        $scope:ident => {
            id: $id:expr,
            module: $module:path,
            effects: $effects:expr $(,)?
        }
    ),+ $(,)?) => {
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
                        effects: $effects,
                    }),+
                }
            }
        }
    };
}

scopes! {
    HarmonyCrypt => {
        id: harmony_crypt::SCOPE,
        module: harmony_crypt::CryptModule::module_spec,
        effects: &[],
    },
    HarmonyHttp => {
        id: harmony_http::SCOPE,
        module: harmony_http::HttpModule::module_spec,
        effects: &[Effect::Egress],
    },
    HarmonyNet => {
        id: harmony_net::SCOPE,
        module: harmony_net::NetModule::module_spec,
        effects: &[Effect::Egress],
    },
    HarmonySerde => {
        id: harmony_serde::SCOPE,
        module: harmony_serde::SerdeModule::module_spec,
        effects: &[],
    },
    HarmonyTask => {
        id: harmony_task::SCOPE,
        module: harmony_task::TaskModule::module_spec,
        effects: &[],
    },
    Api => {
        id: "lyra.api",
        module: api::module_spec,
        effects: &[Effect::Exposes],
    },
    Artists => {
        id: "lyra.artists",
        module: artists::ArtistsModule::module_spec,
        effects: &[],
    },
    Auth => {
        id: "lyra.auth",
        module: auth::AuthModule::module_spec,
        effects: &[Effect::Authority],
    },
    Chromaprint => {
        id: "lyra.chromaprint",
        module: chromaprint::ChromaprintModule::module_spec,
        effects: &[],
    },
    Covers => {
        id: "lyra.covers",
        module: covers::CoversModule::module_spec,
        effects: &[],
    },
    Datastore => {
        id: "lyra.datastore",
        module: datastore::DataStoreModule::module_spec,
        effects: &[],
    },
    Editions => {
        id: "lyra.editions",
        module: editions::EditionsModule::module_spec,
        effects: &[],
    },
    Entities => {
        id: "lyra.entities",
        module: entities::module_spec,
        effects: &[],
    },
    Entries => {
        id: "lyra.entries",
        module: entries::EntriesModule::module_spec,
        effects: &[],
    },
    Favorites => {
        id: "lyra.favorites",
        module: favorites::FavoritesModule::module_spec,
        effects: &[
            Effect::ReadsPersonal("favorites"),
            Effect::WritesPersonal("favorites"),
        ],
    },
    Genres => {
        id: "lyra.genres",
        module: genres::GenresModule::module_spec,
        effects: &[Effect::WritesLibrary("genres")],
    },
    Ids => {
        id: "lyra.ids",
        module: ids::IdsModule::module_spec,
        effects: &[],
    },
    Images => {
        id: "lyra.images",
        module: images::ImagesModule::module_spec,
        effects: &[],
    },
    Labels => {
        id: "lyra.labels",
        module: labels::LabelsModule::module_spec,
        effects: &[Effect::WritesLibrary("record labels")],
    },
    Libraries => {
        id: "lyra.libraries",
        module: libraries::LibrariesModule::module_spec,
        effects: &[],
    },
    Listens => {
        id: "lyra.listens",
        module: listens::ListensModule::module_spec,
        effects: &[Effect::ReadsPersonal("listening history")],
    },
    Locale => {
        id: "lyra.locale",
        module: locale::LocaleModule::module_spec,
        effects: &[],
    },
    Lyrics => {
        id: "lyra.lyrics",
        module: lyrics::LyricsModule::module_spec,
        effects: &[
            Effect::WritesLibrary("lyrics"),
            Effect::WritesPersonal("lyrics"),
        ],
    },
    Metadata => {
        id: "lyra.metadata",
        module: metadata::module_spec,
        effects: &[Effect::WritesLibrary("metadata")],
    },
    Mix => {
        id: "lyra.mix",
        module: mix::module_spec,
        effects: &[Effect::ReadsPersonal("recent listens")],
    },
    PlaybackSessions => {
        id: "lyra.playback_sessions",
        module: playback_sessions::module_spec,
        effects: &[
            Effect::ReadsPersonal("what you are playing"),
            Effect::WritesPersonal("playback on your devices"),
        ],
    },
    PlaybackSources => {
        id: "lyra.playback_sources",
        module: playback_sources::module_spec,
        effects: &[],
    },
    Playlists => {
        id: "lyra.playlists",
        module: playlists::PlaylistsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal("playlists"),
            Effect::WritesPersonal("playlists"),
        ],
    },
    Plugins => {
        id: "lyra.plugins",
        module: manifests::module_spec,
        effects: &[],
    },
    Ratings => {
        id: "lyra.ratings",
        module: ratings::RatingsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal("ratings"),
            Effect::WritesPersonal("ratings"),
        ],
    },
    Releases => {
        id: "lyra.releases",
        module: releases::ReleasesModule::module_spec,
        effects: &[Effect::WritesLibrary("track chains")],
    },
    Server => {
        id: "lyra.server",
        module: server::ServerModule::module_spec,
        effects: &[],
    },
    Tags => {
        id: "lyra.tags",
        module: tags::TagsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal("tags"),
            Effect::WritesPersonal("tags"),
        ],
    },
    TrackSources => {
        id: "lyra.track_sources",
        module: track_sources::TrackSourcesModule::module_spec,
        effects: &[],
    },
    Tracks => {
        id: "lyra.tracks",
        module: tracks::TracksModule::module_spec,
        effects: &[],
    },
    Users => {
        id: "lyra.users",
        module: users::UsersModule::module_spec,
        effects: &[Effect::ReadsPersonal("user accounts")],
    },
}

impl Scope {
    pub(crate) fn from_id(id: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|scope| scope.def().id == id)
    }
}

/// How much care letting a plugin run calls for, from what it can do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Risk {
    Standard,
    /// Can send data off the server and touch personal or library data.
    Elevated,
    /// Adds endpoints or handles credentials.
    High,
}

impl Risk {
    pub(crate) fn of<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Self {
        let mut egress = false;
        let mut touches_data = false;
        for effect in effects {
            match effect {
                Effect::Exposes | Effect::Authority => return Self::High,
                Effect::Egress => egress = true,
                Effect::ReadsPersonal(_) | Effect::WritesPersonal(_) | Effect::WritesLibrary(_) => {
                    touches_data = true;
                }
            }
        }
        if egress && touches_data {
            Self::Elevated
        } else {
            Self::Standard
        }
    }
}

/// One thing a plugin can do, and the plugins whose scopes let it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PluginEffect {
    pub(crate) effect: Effect,
    pub(crate) via: BTreeSet<String>,
}

/// The effects of `scopes`, each scope id paired with the plugins that
/// declare it, merged so each effect appears once.
pub(crate) fn effects_of<'a>(
    scopes: impl IntoIterator<Item = (&'a str, &'a BTreeSet<String>)>,
) -> Vec<PluginEffect> {
    let mut merged: BTreeMap<Effect, BTreeSet<String>> = BTreeMap::new();
    for (id, via) in scopes {
        let Some(scope) = Scope::from_id(id) else {
            continue;
        };
        for effect in scope.def().effects {
            merged
                .entry(*effect)
                .or_default()
                .extend(via.iter().cloned());
        }
    }
    merged
        .into_iter()
        .map(|(effect, via)| PluginEffect { effect, via })
        .collect()
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
    .with_reserved_ids(crate::routes::RESERVED_PLUGIN_IDS.iter().copied())
}

/// What one plugin newly reaches once a candidate is installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeGain {
    pub(crate) growth: ScopeGrowth,
    /// What the newly reached scopes let it do, through the plugins
    /// declaring them.
    pub(crate) effects: Vec<PluginEffect>,
    /// Its risk once the candidate is installed, from everything it
    /// reaches then.
    pub(crate) risk: Risk,
}

/// What installing `candidate` among `plugins`, replacing the one with its
/// id if any, lets each plugin newly reach, by [`scope_growth`].
pub(crate) fn scope_gains<P: ScopedPlugin>(
    plugins: &[P],
    candidate: &P,
) -> BTreeMap<String, ScopeGain> {
    let after = |id: &str| {
        if id == candidate.id() {
            Some(candidate)
        } else {
            plugins.iter().find(|plugin| plugin.id() == id)
        }
    };
    scope_growth(plugins, candidate)
        .into_iter()
        .filter_map(|(id, growth)| {
            let reach = EffectiveScopes::of(after(&id)?, after);
            let effects = effects_of(
                growth
                    .scopes
                    .iter()
                    .filter_map(|scope| Some((scope.as_str(), reach.scopes.get(scope)?))),
            );
            let all = effects_of(
                reach
                    .scopes
                    .iter()
                    .map(|(scope, via)| (scope.as_str(), via)),
            );
            let risk = Risk::of(all.iter().map(|effect| &effect.effect));
            Some((
                id,
                ScopeGain {
                    growth,
                    effects,
                    risk,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{
        BTreeSet,
        HashSet,
    };

    use super::{
        Effect,
        Risk,
        Scope,
        effects_of,
    };

    fn via(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn effects_merge_across_scopes_and_skip_utilities() {
        let (own, dependency) = (via(&["plugin"]), via(&["helper"]));
        let effects = effects_of([
            ("harmony.http", &own),
            ("harmony.net", &dependency),
            ("lyra.ids", &own),
        ]);

        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].effect, Effect::Egress);
        assert_eq!(effects[0].via, via(&["helper", "plugin"]));
    }

    #[test]
    fn risk_rises_with_what_a_plugin_can_do() {
        assert_eq!(Risk::of(&[Effect::Egress]), Risk::Standard);
        assert_eq!(
            Risk::of(&[Effect::ReadsPersonal("listening history")]),
            Risk::Standard
        );
        assert_eq!(
            Risk::of(&[Effect::Egress, Effect::ReadsPersonal("listening history")]),
            Risk::Elevated
        );
        assert_eq!(Risk::of(&[Effect::Exposes]), Risk::High);
        assert_eq!(Risk::of(&[Effect::Authority]), Risk::High);
    }

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
