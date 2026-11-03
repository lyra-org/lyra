// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The scopes a plugin manifest may declare, the module or functions each
//! one gates, and what each lets a plugin do.
//!
//! When adding or changing a function of a gated module, or a method of a
//! class it hands out, check:
//!
//! - whether it takes file paths, user ids, or filters by personal data, and
//!   what it does with those of users the plugin doesn't act for;
//! - whether its effects are among its scope's, and the scope's effects still
//!   all hold;
//! - whether it reaches every user's data rather than the acting users'. Such
//!   a function gets a narrow scope of its own, `within` its module's, named
//!   `<module scope>.all`, so plugins that don't call it don't carry its
//!   effects;
//! - whether it changes the shared library in a module whose scope otherwise
//!   only reads it. Such a function goes under the module's
//!   `<module scope>.manage` scope, so plugins that only read carry no
//!   effects;
//! - whether it is a privileged action with effects beyond its module's, such
//!   as starting a library sync. Such a function gets a narrow scope of its
//!   own as well, named for the action, as `lyra.libraries.manage` and
//!   `lyra.metadata.refresh` are.
//!
//! `gated_module_functions_are_reviewed` lists every function with its
//! arguments, and the methods of the classes each module declares, and fails
//! when the list changes, as a prompt for this review. A module must declare
//! every class it hands out for its methods to be listed.

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
    Egress,
    Exposes,
    Authority,
    ReadsPersonal(PersonalKind, Users),
    WritesPersonal(PersonalKind, Users),
    WritesLibrary(LibraryKind),
}

impl Effect {
    /// The effect as a list item for admins, in English.
    pub(crate) fn description(self) -> String {
        match self {
            Self::Egress => "Sends data off the server".into(),
            Self::Exposes => "Adds endpoints to the server or listens on the network".into(),
            Self::Authority => {
                "Signs users in to Lyra, checks their credentials and manages their sessions".into()
            }
            Self::ReadsPersonal(data, Users::Acting) => {
                format!("Reads the {} of the users it acts for", data.noun())
            }
            Self::ReadsPersonal(data, Users::All) => format!("Reads every user's {}", data.noun()),
            Self::WritesPersonal(data, Users::Acting) => {
                format!("Changes the {} of the users it acts for", data.noun())
            }
            Self::WritesPersonal(data, Users::All) => {
                format!("Changes every user's {}", data.noun())
            }
            Self::WritesLibrary(data) => format!("Changes the library's {}", data.noun()),
        }
    }
}

/// Whose personal data an effect reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Users {
    /// The users the plugin acts for.
    Acting,
    /// Every user's, whoever the plugin acts for.
    All,
}

/// Declares a kind of data an effect names, each variant with a stable id
/// and the noun phrase descriptions use.
macro_rules! data_kinds {
    ($(#[$meta:meta])* $kind:ident { $($variant:ident => ($id:literal, $noun:literal)),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub(crate) enum $kind {
            $($variant),+
        }

        impl $kind {
            #[cfg(test)]
            pub(crate) const ALL: &[$kind] = &[$($kind::$variant),+];

            pub(crate) fn id(self) -> &'static str {
                match self {
                    $(Self::$variant => $id),+
                }
            }

            pub(crate) fn noun(self) -> &'static str {
                match self {
                    $(Self::$variant => $noun),+
                }
            }
        }
    };
}

data_kinds! {
    /// Data each user keeps apart from the shared library.
    PersonalKind {
        Favorites => ("favorites", "favorites"),
        ListeningHistory => ("listening_history", "listening history"),
        RecentListens => ("recent_listens", "recent listens"),
        Playlists => ("playlists", "playlists"),
        Ratings => ("ratings", "ratings"),
        Tags => ("tags", "tags"),
        Lyrics => ("personal_lyrics", "personal lyrics"),
        PlaybackActivity => ("playback_activity", "playback activity"),
        PlaybackSessions => ("playback_sessions", "playback sessions"),
        AccountDetails => ("account_details", "account details"),
    }
}

data_kinds! {
    /// Data of the shared library.
    LibraryKind {
        Genres => ("genres", "genres"),
        RecordLabels => ("record_labels", "record labels"),
        Lyrics => ("lyrics", "lyrics"),
        Metadata => ("metadata", "metadata"),
        TrackChains => ("track_chains", "track chains"),
        Covers => ("covers", "covers"),
    }
}

pub(crate) struct ScopeDef {
    pub(crate) id: &'static str,
    pub(crate) gate: Gate,
    pub(crate) effects: &'static [Effect],
}

/// What a scope lets a plugin use.
pub(crate) enum Gate {
    /// A whole module, through `require`.
    Module(fn() -> ModuleSpec),
    /// Functions of the module another scope gates, which a plugin declaring
    /// only that scope gets as stubs raising an error. A manifest declaring
    /// this scope must declare the other too.
    Functions {
        within: Scope,
        functions: &'static [&'static str],
    },
}

macro_rules! gate {
    ($module:path ;) => {
        Gate::Module($module)
    };
    (; $within:ident, $functions:expr) => {
        Gate::Functions {
            within: Scope::$within,
            functions: $functions,
        }
    };
}

/// Declares every scope once: the enum, `Scope::ALL`, and each scope's
/// definition all come from the same row, so none can miss a scope. A row
/// gates either a `module` or, `within` another row's module, some of its
/// `functions`.
macro_rules! scopes {
    ($(
        $scope:ident => {
            id: $id:expr,
            $(module: $module:path,)?
            $(within: $within:ident, functions: $functions:expr,)?
            effects: $effects:expr $(,)?
        }
    ),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub(crate) enum Scope {
            $($scope),+
        }

        impl Scope {
            pub(crate) const ALL: &[Scope] = &[$(Scope::$scope),+];

            pub(crate) fn def(self) -> ScopeDef {
                match self {
                    $(Scope::$scope => ScopeDef {
                        id: $id,
                        gate: gate!($($module)? ; $($within, $functions)?),
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
        effects: &[Effect::Egress, Effect::Exposes],
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
        module: api::ApiModule::module_spec,
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
    CoversManage => {
        id: "lyra.covers.manage",
        within: Covers,
        functions: &["set_manual", "remove_manual"],
        effects: &[
            Effect::WritesLibrary(LibraryKind::Covers),
            Effect::WritesPersonal(PersonalKind::Playlists, Users::Acting),
        ],
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
        module: entities::EntitiesModule::module_spec,
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
            Effect::ReadsPersonal(PersonalKind::Favorites, Users::Acting),
            Effect::WritesPersonal(PersonalKind::Favorites, Users::Acting),
        ],
    },
    Genres => {
        id: "lyra.genres",
        module: genres::GenresModule::module_spec,
        effects: &[],
    },
    GenresManage => {
        id: "lyra.genres.manage",
        within: Genres,
        functions: &["resolve", "add_parent"],
        effects: &[Effect::WritesLibrary(LibraryKind::Genres)],
    },
    Labels => {
        id: "lyra.labels",
        module: labels::LabelsModule::module_spec,
        effects: &[],
    },
    LabelsManage => {
        id: "lyra.labels.manage",
        within: Labels,
        functions: &["resolve"],
        effects: &[Effect::WritesLibrary(LibraryKind::RecordLabels)],
    },
    Libraries => {
        id: "lyra.libraries",
        module: libraries::LibrariesModule::module_spec,
        effects: &[],
    },
    LibrariesManage => {
        id: "lyra.libraries.manage",
        within: Libraries,
        functions: &["sync", "refresh", "cancel_sync"],
        effects: &[
            Effect::WritesLibrary(LibraryKind::Genres),
            Effect::WritesLibrary(LibraryKind::RecordLabels),
            Effect::WritesLibrary(LibraryKind::Lyrics),
            Effect::WritesLibrary(LibraryKind::Metadata),
            Effect::WritesLibrary(LibraryKind::Covers),
        ],
    },
    Listens => {
        id: "lyra.listens",
        module: listens::ListensModule::module_spec,
        effects: &[
            Effect::ReadsPersonal(PersonalKind::ListeningHistory, Users::Acting),
            Effect::WritesPersonal(PersonalKind::ListeningHistory, Users::Acting),
        ],
    },
    ListensAll => {
        id: "lyra.listens.all",
        within: Listens,
        functions: &["on_update"],
        effects: &[Effect::ReadsPersonal(PersonalKind::ListeningHistory, Users::All)],
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
            Effect::ReadsPersonal(PersonalKind::Lyrics, Users::Acting),
            Effect::WritesPersonal(PersonalKind::Lyrics, Users::Acting),
        ],
    },
    LyricsAll => {
        id: "lyra.lyrics.all",
        within: Lyrics,
        functions: &["remove_all"],
        effects: &[
            Effect::WritesPersonal(PersonalKind::Lyrics, Users::All),
            Effect::WritesLibrary(LibraryKind::Lyrics),
        ],
    },
    Metadata => {
        id: "lyra.metadata",
        module: metadata::MetadataModule::module_spec,
        effects: &[Effect::WritesLibrary(LibraryKind::Metadata)],
    },
    MetadataRefresh => {
        id: "lyra.metadata.refresh",
        within: Metadata,
        functions: &["refresh"],
        effects: &[
            Effect::WritesLibrary(LibraryKind::Metadata),
            Effect::WritesLibrary(LibraryKind::Covers),
        ],
    },
    Mix => {
        id: "lyra.mix",
        module: mix::MixModule::module_spec,
        effects: &[Effect::ReadsPersonal(PersonalKind::RecentListens, Users::Acting)],
    },
    PlaybackSessions => {
        id: "lyra.playback_sessions",
        module: playback_sessions::PlaybackSessionsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal(PersonalKind::PlaybackActivity, Users::Acting),
            Effect::WritesPersonal(PersonalKind::PlaybackSessions, Users::Acting),
            Effect::WritesPersonal(PersonalKind::ListeningHistory, Users::Acting),
        ],
    },
    PlaybackSessionsAll => {
        id: "lyra.playback_sessions.all",
        within: PlaybackSessions,
        functions: &["list_connections", "list_playbacks", "on_update"],
        effects: &[Effect::ReadsPersonal(PersonalKind::PlaybackActivity, Users::All)],
    },
    PlaybackSources => {
        id: "lyra.playback_sources",
        module: playback_sources::PlaybackSourcesModule::module_spec,
        effects: &[],
    },
    Playlists => {
        id: "lyra.playlists",
        module: playlists::PlaylistsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal(PersonalKind::Playlists, Users::Acting),
            Effect::WritesPersonal(PersonalKind::Playlists, Users::Acting),
        ],
    },
    Plugins => {
        id: "lyra.plugins",
        module: manifests::PluginsModule::module_spec,
        effects: &[],
    },
    Ratings => {
        id: "lyra.ratings",
        module: ratings::RatingsModule::module_spec,
        effects: &[
            Effect::ReadsPersonal(PersonalKind::Ratings, Users::Acting),
            Effect::WritesPersonal(PersonalKind::Ratings, Users::Acting),
        ],
    },
    Releases => {
        id: "lyra.releases",
        module: releases::ReleasesModule::module_spec,
        effects: &[],
    },
    ReleasesManage => {
        id: "lyra.releases.manage",
        within: Releases,
        functions: &["replace_chains"],
        effects: &[Effect::WritesLibrary(LibraryKind::TrackChains)],
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
            Effect::ReadsPersonal(PersonalKind::Tags, Users::Acting),
            Effect::WritesPersonal(PersonalKind::Tags, Users::Acting),
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
        effects: &[Effect::ReadsPersonal(PersonalKind::AccountDetails, Users::All)],
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
    Elevated,
    High,
}

/// What raises a plugin's risk above standard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RiskReason {
    Exposes,
    Authority,
    /// It can send data off the server and read this personal data.
    SendsPersonal(PersonalKind, Users),
    /// It can send data off the server and change this personal data.
    ChangesPersonal(PersonalKind, Users),
    /// It can send data off the server and change this library data.
    ChangesLibrary(LibraryKind),
}

impl RiskReason {
    fn risk(self) -> Risk {
        match self {
            Self::Exposes
            | Self::Authority
            | Self::SendsPersonal(_, Users::All)
            | Self::ChangesPersonal(_, Users::All) => Risk::High,
            Self::SendsPersonal(_, Users::Acting)
            | Self::ChangesPersonal(_, Users::Acting)
            | Self::ChangesLibrary(_) => Risk::Elevated,
        }
    }

    /// The reason as a list item for admins, in English.
    pub(crate) fn description(self) -> String {
        match self {
            Self::Exposes => "Can add endpoints to the server or listen on the network".into(),
            Self::Authority => {
                "Can sign users in to Lyra, check their credentials and manage their sessions"
                    .into()
            }
            Self::SendsPersonal(data, Users::Acting) => format!(
                "Can send the {} of the users it acts for off the server",
                data.noun()
            ),
            Self::SendsPersonal(data, Users::All) => {
                format!("Can send every user's {} off the server", data.noun())
            }
            Self::ChangesPersonal(data, Users::Acting) => format!(
                "Can change the {} of the users it acts for and send data off the server",
                data.noun()
            ),
            Self::ChangesPersonal(data, Users::All) => format!(
                "Can change every user's {} and send data off the server",
                data.noun()
            ),
            Self::ChangesLibrary(data) => format!(
                "Can change the library's {} and send data off the server",
                data.noun()
            ),
        }
    }
}

/// A plugin's risk, with the reasons that set it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RiskAssessment {
    pub(crate) risk: Risk,
    /// Every reason at `risk`, none below it; empty at standard.
    pub(crate) reasons: Vec<RiskReason>,
}

impl RiskAssessment {
    /// High for adding endpoints, handling sign-in, or sending data off the
    /// server with every user's personal data; elevated for sending data off
    /// the server with personal or library data; else standard.
    pub(crate) fn of<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Self {
        let effects: Vec<Effect> = effects.into_iter().copied().collect();
        let egress = effects.contains(&Effect::Egress);
        let mut reasons: Vec<RiskReason> = effects
            .iter()
            .filter_map(|effect| match *effect {
                Effect::Egress => None,
                Effect::Exposes => Some(RiskReason::Exposes),
                Effect::Authority => Some(RiskReason::Authority),
                Effect::ReadsPersonal(data, users) => {
                    egress.then_some(RiskReason::SendsPersonal(data, users))
                }
                Effect::WritesPersonal(data, users) => {
                    egress.then_some(RiskReason::ChangesPersonal(data, users))
                }
                Effect::WritesLibrary(data) => egress.then_some(RiskReason::ChangesLibrary(data)),
            })
            .collect();
        let risk = reasons
            .iter()
            .map(|reason| reason.risk())
            .max()
            .unwrap_or(Risk::Standard);
        reasons.retain(|reason| reason.risk() == risk);
        reasons.sort();
        reasons.dedup();
        Self { risk, reasons }
    }
}

/// A plugin whose scope gives an effect.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EffectSource {
    pub(crate) plugin: String,
    pub(crate) scope: Scope,
}

/// One thing a plugin can do, and the plugins and scopes that let it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PluginEffect {
    pub(crate) effect: Effect,
    pub(crate) via: BTreeSet<EffectSource>,
}

impl PluginEffect {
    /// The plugins whose scopes give the effect.
    #[cfg(test)]
    pub(crate) fn plugins(&self) -> BTreeSet<&str> {
        self.via
            .iter()
            .map(|source| source.plugin.as_str())
            .collect()
    }
}

/// The effects of `scopes`, each scope id paired with the plugins that
/// declare it, merged so each effect appears once.
pub(crate) fn effects_of<'a>(
    scopes: impl IntoIterator<Item = (&'a str, &'a BTreeSet<String>)>,
) -> Vec<PluginEffect> {
    let mut merged: BTreeMap<Effect, BTreeSet<EffectSource>> = BTreeMap::new();
    for (id, via) in scopes {
        let Some(scope) = Scope::from_id(id) else {
            continue;
        };
        for effect in scope.def().effects {
            merged
                .entry(*effect)
                .or_default()
                .extend(via.iter().map(|plugin| EffectSource {
                    plugin: plugin.clone(),
                    scope,
                }));
        }
    }
    merged
        .into_iter()
        .map(|(effect, via)| PluginEffect { effect, via })
        .collect()
}

/// Every module plugins can `require`, behind its scope, with the functions
/// narrower scopes gate.
pub(crate) fn gated_modules() -> Vec<GatedModule> {
    Scope::ALL
        .iter()
        .filter_map(|scope| {
            let def = scope.def();
            let Gate::Module(module) = def.gate else {
                return None;
            };
            Some(narrowed_functions(*scope).fold(
                GatedModule::new(def.id, module()),
                |gated, (function, narrow)| gated.function_scope(function, narrow),
            ))
        })
        .collect()
}

/// The functions of `parent`'s module that narrower scopes gate, each with
/// the scope's id.
fn narrowed_functions(parent: Scope) -> impl Iterator<Item = (&'static str, &'static str)> {
    Scope::ALL.iter().flat_map(move |scope| {
        let def = scope.def();
        let functions = match def.gate {
            Gate::Functions { within, functions } if within == parent => functions,
            _ => &[],
        };
        functions.iter().map(move |function| (*function, def.id))
    })
}

/// Each scope that narrows another, with the id of the one it narrows.
pub(crate) fn scope_parents() -> impl Iterator<Item = (&'static str, &'static str)> {
    Scope::ALL.iter().filter_map(|scope| {
        let def = scope.def();
        match def.gate {
            Gate::Functions { within, .. } => Some((def.id, within.def().id)),
            Gate::Module(_) => None,
        }
    })
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
    .with_scope_parents(scope_parents())
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
    pub(crate) risk: RiskAssessment,
    /// Its risk as installed, from everything it reaches now; standard for
    /// a candidate not installed.
    pub(crate) previous_risk: Risk,
}

/// What installing `candidate` among `plugins`, replacing the one with its
/// id if any, lets each plugin newly reach, by [`scope_growth`]. `plugins`
/// may already hold other updates; each plugin's previous risk is judged
/// from the `installed` ones.
pub(crate) fn scope_gains<P: ScopedPlugin>(
    installed: &[P],
    plugins: &[P],
    candidate: &P,
) -> BTreeMap<String, ScopeGain> {
    let installed_as = |id: &str| installed.iter().find(|plugin| plugin.id() == id);
    let after = |id: &str| {
        if id == candidate.id() {
            Some(candidate)
        } else {
            plugins.iter().find(|plugin| plugin.id() == id)
        }
    };
    let risk_of = |reach: &EffectiveScopes| {
        let effects = effects_of(
            reach
                .scopes
                .iter()
                .map(|(scope, via)| (scope.as_str(), via)),
        );
        RiskAssessment::of(effects.iter().map(|effect| &effect.effect))
    };
    scope_growth(plugins, candidate)
        .into_iter()
        .filter_map(|(id, growth)| {
            let reach = EffectiveScopes::of(after(&id)?, after);
            let previous_risk = installed_as(&id)
                .map(|plugin| risk_of(&EffectiveScopes::of(plugin, installed_as)).risk)
                .unwrap_or(Risk::Standard);
            let effects = effects_of(
                growth
                    .scopes
                    .iter()
                    .filter_map(|scope| Some((scope.as_str(), reach.scopes.get(scope)?))),
            );
            Some((
                id,
                ScopeGain {
                    growth,
                    effects,
                    risk: risk_of(&reach),
                    previous_risk,
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
        EffectSource,
        Gate,
        LibraryKind,
        PersonalKind,
        Risk,
        RiskAssessment,
        RiskReason,
        Scope,
        Users,
        effects_of,
    };

    fn via(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(ToString::to_string).collect()
    }

    fn risk(effects: &[Effect]) -> Risk {
        RiskAssessment::of(effects).risk
    }

    fn reasons(effects: &[Effect]) -> Vec<String> {
        RiskAssessment::of(effects)
            .reasons
            .into_iter()
            .map(RiskReason::description)
            .collect()
    }

    #[test]
    fn effects_merge_across_scopes_and_skip_utilities() {
        let (own, dependency) = (via(&["plugin"]), via(&["helper"]));
        let effects = effects_of([
            ("harmony.http", &own),
            ("harmony.net", &dependency),
            ("lyra.libraries", &own),
        ]);

        assert_eq!(effects.len(), 2);
        assert_eq!(effects[0].effect, Effect::Egress);
        assert_eq!(
            effects[0].via,
            BTreeSet::from([
                EffectSource {
                    plugin: "helper".into(),
                    scope: Scope::HarmonyNet,
                },
                EffectSource {
                    plugin: "plugin".into(),
                    scope: Scope::HarmonyHttp,
                },
            ])
        );
        assert_eq!(effects[0].plugins(), BTreeSet::from(["helper", "plugin"]));
        assert_eq!(effects[1].effect, Effect::Exposes);
        assert_eq!(effects[1].plugins(), BTreeSet::from(["helper"]));
    }

    #[test]
    fn risk_rises_with_what_a_plugin_can_do() {
        let history = PersonalKind::ListeningHistory;
        assert_eq!(risk(&[]), Risk::Standard);
        assert_eq!(risk(&[Effect::Egress]), Risk::Standard);
        assert_eq!(
            risk(&[Effect::ReadsPersonal(history, Users::All)]),
            Risk::Standard
        );
        assert_eq!(
            risk(&[
                Effect::Egress,
                Effect::ReadsPersonal(history, Users::Acting)
            ]),
            Risk::Elevated
        );
        assert_eq!(
            risk(&[Effect::Egress, Effect::WritesLibrary(LibraryKind::Genres)]),
            Risk::Elevated
        );
        assert_eq!(
            risk(&[Effect::Egress, Effect::ReadsPersonal(history, Users::All)]),
            Risk::High
        );
        assert_eq!(
            risk(&[Effect::Egress, Effect::WritesPersonal(history, Users::All)]),
            Risk::High
        );
        assert_eq!(risk(&[Effect::Exposes]), Risk::High);
        assert_eq!(risk(&[Effect::Authority]), Risk::High);
    }

    #[test]
    fn risk_reasons_are_the_ones_at_its_level() {
        let history = PersonalKind::ListeningHistory;
        assert!(reasons(&[]).is_empty());
        assert!(reasons(&[Effect::Egress]).is_empty());
        assert!(reasons(&[Effect::ReadsPersonal(history, Users::All)]).is_empty());
        assert_eq!(
            reasons(&[Effect::Exposes]),
            ["Can add endpoints to the server or listen on the network"]
        );
        assert_eq!(
            reasons(&[Effect::Authority]),
            ["Can sign users in to Lyra, check their credentials and manage their sessions"]
        );
        assert_eq!(
            reasons(&[
                Effect::Egress,
                Effect::ReadsPersonal(history, Users::Acting),
                Effect::WritesPersonal(history, Users::Acting),
                Effect::WritesLibrary(LibraryKind::TrackChains),
            ]),
            [
                "Can send the listening history of the users it acts for off the server",
                "Can change the listening history of the users it acts for and send data off \
                 the server",
                "Can change the library's track chains and send data off the server",
            ]
        );
        assert_eq!(
            reasons(&[
                Effect::Egress,
                Effect::Exposes,
                Effect::ReadsPersonal(history, Users::All),
                Effect::WritesPersonal(PersonalKind::Lyrics, Users::All),
                Effect::ReadsPersonal(PersonalKind::Favorites, Users::Acting),
            ]),
            [
                "Can add endpoints to the server or listen on the network",
                "Can send every user's listening history off the server",
                "Can change every user's personal lyrics and send data off the server",
            ],
            "only the reasons at the plugin's level explain it"
        );
    }

    #[test]
    fn every_scope_describes_its_effects() {
        let described: Vec<(&str, Vec<String>)> = Scope::ALL
            .iter()
            .map(|scope| scope.def())
            .filter(|def| !def.effects.is_empty())
            .map(|def| {
                (
                    def.id,
                    def.effects
                        .iter()
                        .map(|effect| effect.description())
                        .collect(),
                )
            })
            .collect();
        let expected: &[(&str, &[&str])] = &[
            ("harmony.http", &["Sends data off the server"]),
            (
                "harmony.net",
                &[
                    "Sends data off the server",
                    "Adds endpoints to the server or listens on the network",
                ],
            ),
            (
                "lyra.api",
                &["Adds endpoints to the server or listens on the network"],
            ),
            (
                "lyra.auth",
                &["Signs users in to Lyra, checks their credentials and manages their sessions"],
            ),
            (
                "lyra.covers.manage",
                &[
                    "Changes the library's covers",
                    "Changes the playlists of the users it acts for",
                ],
            ),
            (
                "lyra.favorites",
                &[
                    "Reads the favorites of the users it acts for",
                    "Changes the favorites of the users it acts for",
                ],
            ),
            ("lyra.genres.manage", &["Changes the library's genres"]),
            (
                "lyra.labels.manage",
                &["Changes the library's record labels"],
            ),
            (
                "lyra.libraries.manage",
                &[
                    "Changes the library's genres",
                    "Changes the library's record labels",
                    "Changes the library's lyrics",
                    "Changes the library's metadata",
                    "Changes the library's covers",
                ],
            ),
            (
                "lyra.listens",
                &[
                    "Reads the listening history of the users it acts for",
                    "Changes the listening history of the users it acts for",
                ],
            ),
            (
                "lyra.listens.all",
                &["Reads every user's listening history"],
            ),
            (
                "lyra.lyrics",
                &[
                    "Reads the personal lyrics of the users it acts for",
                    "Changes the personal lyrics of the users it acts for",
                ],
            ),
            (
                "lyra.lyrics.all",
                &[
                    "Changes every user's personal lyrics",
                    "Changes the library's lyrics",
                ],
            ),
            ("lyra.metadata", &["Changes the library's metadata"]),
            (
                "lyra.metadata.refresh",
                &[
                    "Changes the library's metadata",
                    "Changes the library's covers",
                ],
            ),
            (
                "lyra.mix",
                &["Reads the recent listens of the users it acts for"],
            ),
            (
                "lyra.playback_sessions",
                &[
                    "Reads the playback activity of the users it acts for",
                    "Changes the playback sessions of the users it acts for",
                    "Changes the listening history of the users it acts for",
                ],
            ),
            (
                "lyra.playback_sessions.all",
                &["Reads every user's playback activity"],
            ),
            (
                "lyra.playlists",
                &[
                    "Reads the playlists of the users it acts for",
                    "Changes the playlists of the users it acts for",
                ],
            ),
            (
                "lyra.ratings",
                &[
                    "Reads the ratings of the users it acts for",
                    "Changes the ratings of the users it acts for",
                ],
            ),
            (
                "lyra.releases.manage",
                &["Changes the library's track chains"],
            ),
            (
                "lyra.tags",
                &[
                    "Reads the tags of the users it acts for",
                    "Changes the tags of the users it acts for",
                ],
            ),
            ("lyra.users", &["Reads every user's account details"]),
        ];
        assert_eq!(
            described,
            expected
                .iter()
                .map(|(id, effects)| (*id, effects.iter().map(ToString::to_string).collect()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn data_kinds_have_stable_ids() {
        let ids: Vec<&str> = PersonalKind::ALL
            .iter()
            .map(|kind| kind.id())
            .chain(LibraryKind::ALL.iter().map(|kind| kind.id()))
            .collect();
        assert_eq!(
            ids,
            [
                "favorites",
                "listening_history",
                "recent_listens",
                "playlists",
                "ratings",
                "tags",
                "personal_lyrics",
                "playback_activity",
                "playback_sessions",
                "account_details",
                "genres",
                "record_labels",
                "lyrics",
                "metadata",
                "track_chains",
                "covers",
            ]
        );
    }

    #[test]
    fn scopes_have_unique_ids_and_modules() {
        let mut ids = HashSet::new();
        let mut modules = HashSet::new();
        let mut functions = HashSet::new();
        for scope in Scope::ALL {
            let def = scope.def();
            assert!(ids.insert(def.id), "scope id {} is listed twice", def.id);
            match def.gate {
                Gate::Module(module) => {
                    let module_id = module().id.0;
                    assert!(
                        modules.insert(module_id.clone()),
                        "module {module_id} is gated twice"
                    );
                }
                Gate::Functions {
                    within,
                    functions: gated,
                } => {
                    assert!(
                        matches!(within.def().gate, Gate::Module(_)),
                        "{} narrows {}, which gates no module",
                        def.id,
                        within.def().id
                    );
                    for function in gated {
                        assert!(
                            functions.insert((within.def().id, *function)),
                            "{function} of {} is gated twice",
                            within.def().id
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn narrow_scopes_need_their_parent_in_a_manifest() {
        let load = |scopes: &str| {
            let json = format!(
                r#"{{"schema_version":1,"id":"demo","name":"Demo","version":"1.0.0","lyra":"{}","description":"","scopes":{scopes}}}"#,
                crate::plugins::testing::HOST_REQUIREMENT
            );
            harmony_core::plugin::PluginManager::load_manifest(
                std::path::Path::new("plugin.json"),
                json.as_bytes(),
                &super::plugin_host(),
            )
        };
        assert!(load(r#"["lyra.listens","lyra.listens.all"]"#).is_ok());
        for (narrow, parent) in [
            ("lyra.covers.manage", "lyra.covers"),
            ("lyra.genres.manage", "lyra.genres"),
            ("lyra.labels.manage", "lyra.labels"),
            ("lyra.libraries.manage", "lyra.libraries"),
            ("lyra.listens.all", "lyra.listens"),
            ("lyra.lyrics.all", "lyra.lyrics"),
            ("lyra.metadata.refresh", "lyra.metadata"),
            ("lyra.playback_sessions.all", "lyra.playback_sessions"),
            ("lyra.releases.manage", "lyra.releases"),
        ] {
            let error = load(&format!(r#"["{narrow}"]"#)).unwrap_err();
            assert!(
                matches!(
                    &error,
                    harmony_core::plugin::PluginLoadError::ScopeWithoutParent {
                        scope,
                        parent: found,
                        ..
                    } if scope == narrow && found == parent
                ),
                "{error}"
            );
        }
    }

    struct Plugin(Vec<String>);

    impl Plugin {
        fn new(scopes: &[&str]) -> Self {
            Self(scopes.iter().map(ToString::to_string).collect())
        }
    }

    impl harmony_core::scopes::ScopedPlugin for Plugin {
        fn id(&self) -> &str {
            "demo"
        }

        fn scopes(&self) -> &[String] {
            &self.0
        }

        fn dependency_ids(&self) -> impl Iterator<Item = &str> {
            std::iter::empty()
        }
    }

    #[test]
    fn reaching_a_narrow_scope_is_growth_with_its_effects() {
        let installed = [Plugin::new(&["harmony.http", "lyra.listens"])];
        let gains = super::scope_gains(
            &installed,
            &installed,
            &Plugin::new(&["harmony.http", "lyra.listens", "lyra.listens.all"]),
        );
        let gain = &gains["demo"];
        assert_eq!(
            gain.growth.scopes,
            BTreeSet::from(["lyra.listens.all".to_string()])
        );
        assert_eq!(
            gain.effects
                .iter()
                .map(|effect| effect.effect)
                .collect::<Vec<_>>(),
            [Effect::ReadsPersonal(
                PersonalKind::ListeningHistory,
                Users::All
            )]
        );
        assert_eq!(
            (gain.previous_risk, gain.risk.risk),
            (Risk::Elevated, Risk::High),
            "without the narrow scope, listens alone is no longer high"
        );
    }

    #[test]
    fn gated_modules_register() {
        let mut registry = harmony_core::ModuleRegistry::new();
        for module in super::gated_modules() {
            let id = module.spec.id.0.clone();
            registry
                .register(module)
                .unwrap_or_else(|error| panic!("{id}: {error}"));
        }
    }

    /// Each gated module's functions and the methods of the classes it
    /// declares, with their arguments and the narrower scope gating any of
    /// them. A change here is a prompt to review the new or changed ones
    /// against the checklist in this module's docs.
    #[test]
    fn gated_module_functions_are_reviewed() {
        fn signature(name: &str, function: &harmony_core::FunctionSpec) -> String {
            let arguments: Vec<&str> = function.arg_names.iter().map(|arg| &**arg).collect();
            format!("{name}({})", arguments.join(", "))
        }

        let exported: Vec<(String, Vec<String>)> = super::gated_modules()
            .into_iter()
            .map(|module| {
                let functions = module.spec.functions.iter().map(|function| {
                    let signature = signature(&function.name, function);
                    match module.function_scopes.get(&function.name) {
                        Some(scope) => format!("{signature} [{}]", scope.0),
                        None => signature,
                    }
                });
                let methods = module.spec.userdata.iter().flat_map(|class| {
                    class
                        .methods
                        .iter()
                        .map(|method| signature(&format!("{}:{}", class.name, method.name), method))
                });
                (
                    module.spec.id.0.to_string(),
                    functions.chain(methods).collect(),
                )
            })
            .collect();
        let reviewed: Vec<(String, Vec<String>)> = REVIEWED_FUNCTIONS
            .iter()
            .map(|(module, functions)| {
                (
                    module.to_string(),
                    functions.iter().map(ToString::to_string).collect(),
                )
            })
            .collect();
        assert_eq!(
            exported, reviewed,
            "the functions of the gated modules changed: review each new or changed function \
             or method against the checklist in the docs of lyra-server/src/plugins/scopes.rs, then \
             update REVIEWED_FUNCTIONS"
        );
    }

    const REVIEWED_FUNCTIONS: &[(&str, &[&str])] = &[
        (
            "harmony/crypt",
            &[
                "hash(algorithm, data)",
                "base64.encode(data)",
                "base64.decode(data)",
                "random(size)",
            ],
        ),
        (
            "harmony/http",
            &[
                "request(options)",
                "set_rate_limit(options)",
                "set_max_in_flight(options)",
                "encode_uri_component(input)",
            ],
        ),
        (
            "harmony/net",
            &[
                "udp_bind(options)",
                "tcp_connect(options)",
                "tcp_bind(options)",
                "UdpSocket:set_timeout(seconds)",
                "UdpSocket:address()",
                "UdpSocket:connect(address)",
                "UdpSocket:send(data, timeout)",
                "UdpSocket:recv(timeout)",
                "UdpSocket:recv_from(timeout)",
                "UdpSocket:send_to(data, address, timeout)",
                "UdpSocket:close()",
                "TcpStream:set_timeout(seconds)",
                "TcpStream:address()",
                "TcpStream:read(max_bytes, timeout)",
                "TcpStream:write(data, timeout)",
                "TcpStream:close()",
                "TcpListener:set_timeout(seconds)",
                "TcpListener:address()",
                "TcpListener:accept(timeout)",
                "TcpListener:close()",
            ],
        ),
        (
            "harmony/serde",
            &[
                "json.encode(input)",
                "json.encoder()",
                "json.decode(input)",
                "yaml.encode(input)",
                "yaml.decode(input)",
                "xml.encode(root, input)",
                "xml.encoder()",
                "xml.decode(input)",
                "empty_object()",
                "JsonEncoder:begin_object(key)",
                "JsonEncoder:end_object()",
                "JsonEncoder:begin_array(key)",
                "JsonEncoder:end_array()",
                "JsonEncoder:field(key, value)",
                "JsonEncoder:items(values)",
                "JsonEncoder:flush()",
                "JsonEncoder:finish()",
                "XmlEncoder:begin_element(name, attributes)",
                "XmlEncoder:end_element()",
                "XmlEncoder:elements(name, values)",
                "XmlEncoder:flush()",
                "XmlEncoder:finish()",
            ],
        ),
        (
            "harmony/task",
            &[
                "defer(task)",
                "delay(time, task)",
                "wait(time)",
                "cancel(thread)",
                "spawn(task)",
            ],
        ),
        (
            "lyra/api",
            &[
                "route(method, path, handler, options)",
                "get(path, handler, options)",
                "post(path, handler, options)",
                "put(path, handler, options)",
                "patch(path, handler, options)",
                "delete(path, handler, options)",
                "head(path, handler, options)",
                "options(path, handler, options)",
                "websocket(path, handler, options)",
                "response.json(status, body, headers)",
                "response.empty(status, headers)",
                "response.text(status, body, headers)",
                "response.bytes(status, body, headers)",
                "response.filler(status, size_bytes, headers)",
                "response.stream(status, produce, headers)",
                "response.redirect(status, location, headers)",
                "response.file(status, path, headers, transform)",
                "response.stream_track(track_id, options)",
                "response.download_track(track_id, options)",
                "response.hls_playlist(track_id, options)",
                "delivery_format(name)",
                "query.bool(query, key, default)",
                "query.int(query, key, default, min, max)",
                "query.csv(query, key)",
                "ApiWebSocketReader:recv()",
                "ApiWebSocketReader:close()",
                "ApiWebSocketSender:send(text)",
                "ApiWebSocketSender:is_closed()",
                "ApiWebSocketSender:close()",
                "ApiStreamWriter:write(chunk)",
            ],
        ),
        (
            "lyra/artists",
            &[
                "get(ids)",
                "query(query)",
                "by_release(release_ids)",
                "by_track(track_ids)",
                "artwork_id_by_name(name)",
                "relations(ids)",
            ],
        ),
        (
            "lyra/auth",
            &[
                "get_session_data(token)",
                "set_session_data(token, data)",
                "resolve_auth(bearer)",
                "logout_session(token)",
                "list_sessions(query)",
                "revoke_session(session_id)",
                "login(username, password, user_agent, client_name)",
                "capabilities()",
            ],
        ),
        ("lyra/chromaprint", &["compute(entry_id)"]),
        (
            "lyra/covers",
            &[
                "get(ids, options)",
                "artwork(ids, options)",
                "set_manual(id, image) [lyra.covers.manage]",
                "remove_manual(id) [lyra.covers.manage]",
            ],
        ),
        (
            "lyra/datastore",
            &[
                "get_or_create(name)",
                "DataStore:get(key)",
                "DataStore:set(key, value)",
                "DataStore:remove(key)",
                "DataStore:get_many(keys)",
                "DataStore:set_many(entries)",
                "DataStore:clear()",
            ],
        ),
        (
            "lyra/editions",
            &[
                "media_formats(input)",
                "media_formats_match(a, b)",
                "barcode(input)",
                "barcodes_match(a, b)",
            ],
        ),
        (
            "lyra/entities",
            &[
                "query(request)",
                "query_release(request)",
                "query_track(request)",
                "query_artist(request)",
                "get_type(id)",
                "query_many(request)",
            ],
        ),
        (
            "lyra/entries",
            &["get(ids)", "by_parent(parent_ids)", "by_track(track_ids)"],
        ),
        (
            "lyra/favorites",
            &[
                "add(target_id)",
                "remove(target_id)",
                "get(target_ids)",
                "list_ids(entity)",
            ],
        ),
        (
            "lyra/genres",
            &[
                "query(query)",
                "resolve(request) [lyra.genres.manage]",
                "add_parent(child_id, parent_id) [lyra.genres.manage]",
                "get(ids)",
                "by_release(release_ids)",
                "find_by_name(name)",
            ],
        ),
        (
            "lyra/labels",
            &[
                "resolve(request) [lyra.labels.manage]",
                "get_by_id(label_id)",
                "get_for_release(release_id)",
                "get_for_releases_many(release_ids)",
                "get_releases(label_id)",
                "get_releases_many(label_ids)",
            ],
        ),
        (
            "lyra/libraries",
            &[
                "list()",
                "get(library_ids)",
                "by_entity(entity_ids)",
                "sync(library_id, options) [lyra.libraries.manage]",
                "refresh(library_id, options) [lyra.libraries.manage]",
                "sync_status(library_ids, options)",
                "cancel_sync(library_id) [lyra.libraries.manage]",
            ],
        ),
        (
            "lyra/listens",
            &[
                "get(target_ids, options)",
                "submit(plays)",
                "on_update(handler) [lyra.listens.all]",
            ],
        ),
        (
            "lyra/locale",
            &[
                "language(input)",
                "country(input)",
                "parse(tag)",
                "tag(language, country)",
                "languages_match(a, b)",
            ],
        ),
        (
            "lyra/lyrics",
            &[
                "by_track(track_ids, options)",
                "get(ids)",
                "parse_lrc(text, options)",
                "set_personal(track_id, upload)",
                "remove_personal(track_id)",
                "remove_all(track_id) [lyra.lyrics.all]",
            ],
        ),
        (
            "lyra/metadata",
            &[
                "Provider.new(id, options)",
                "refresh(entity_id, options) [lyra.metadata.refresh]",
                "Provider:id(spec, generator)",
                "Provider:search(entity, handler)",
                "Provider:cover(entity, config, handler)",
                "Provider:lyrics(config, handler)",
                "Provider:similar_releases(config, handler)",
                "Provider:refresh(entity, handler, filter)",
                "Provider:declare_option(config)",
                "Provider:ensure_artist(request)",
                "Provider:mark_unmatched(node_id, id_types)",
                "Provider:layer(node_id)",
                "Layer:set_id(id_type, id_value)",
                "Layer:set_field(name, value)",
                "Layer:set_credits(role, credits)",
                "Layer:set_relations(relation_type, relations)",
                "Layer:save()",
                "Layer:set_custom_field(version, name, value)",
                "Layer:set_custom_fields(version, fields)",
                "Layer:clear_custom_fields(version)",
            ],
        ),
        (
            "lyra/mix",
            &[
                "Mixer.new(id, options)",
                "blend(request)",
                "path(request)",
                "Mixer:blend(spec, handler)",
                "Mixer:path(spec, handler)",
                "Mixer:declare_option(config)",
            ],
        ),
        (
            "lyra/playback_sessions",
            &[
                "on_update(handler) [lyra.playback_sessions.all]",
                "report_session(request)",
                "hold_session(sender, session_key)",
                "list_connections(query) [lyra.playback_sessions.all]",
                "list_playbacks(query) [lyra.playback_sessions.all]",
                "save_queue(request)",
                "remove_queue(client_name)",
                "latest_queue()",
                "send_command(request)",
            ],
        ),
        (
            "lyra/playback_sources",
            &[
                "get(id, include_entry)",
                "streamable(track_id, include_entry)",
                "decide(track_id, options)",
                "get_many(track_ids, include_entry)",
            ],
        ),
        (
            "lyra/playlists",
            &[
                "get(ids)",
                "query(query)",
                "entries(playlist_ids)",
                "create(request)",
                "update(playlist_id, fields)",
                "replace(playlist_id, request)",
                "add_tracks(playlist_id, track_ids, options)",
                "remove_entries(playlist_id, entry_ids)",
                "move_entry(playlist_id, entry_id, position)",
                "delete(playlist_id)",
            ],
        ),
        (
            "lyra/plugins",
            &[
                "declare_settings(callback)",
                "declare_user_settings(callback)",
                "id()",
                "manifest()",
                "list()",
                "get(id)",
                "SettingsBuilder:group(id, label)",
                "SettingsBuilder:string(key, props)",
                "SettingsBuilder:number(key, props)",
                "SettingsBuilder:bool(key, props)",
                "SettingsBuilder:choice(key, props)",
                "UserSettingsAccessor:get(user_id)",
            ],
        ),
        (
            "lyra/ratings",
            &[
                "set(target_id, rating)",
                "remove(target_id)",
                "get(target_ids)",
                "average(target_ids)",
            ],
        ),
        (
            "lyra/releases",
            &[
                "get(ids)",
                "query(query)",
                "by_track(track_ids)",
                "by_genre(genre_ids)",
                "chains(release_ids)",
                "replace_chains(release_id, chains) [lyra.releases.manage]",
                "similar(release_id, opts)",
            ],
        ),
        ("lyra/server", &["info()"]),
        (
            "lyra/tags",
            &[
                "add(target_id, tag, color)",
                "remove(target_id, tag)",
                "has(target_id, tag)",
                "has_many(target_ids, tag)",
                "get_for_target(target_id)",
                "get_for_targets_many(target_ids)",
                "get_tagged(tag)",
            ],
        ),
        ("lyra/track_sources", &["get_primary_source_key(track_id)"]),
        (
            "lyra/tracks",
            &[
                "get(ids)",
                "streamable(ids)",
                "query(query)",
                "by_release(release_ids)",
                "by_entry(entry_ids)",
                "chains(track_ids)",
            ],
        ),
        ("lyra/users", &["list()"]),
    ];
}
