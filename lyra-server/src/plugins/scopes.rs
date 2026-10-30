// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The scopes a plugin manifest may declare, the module or functions each
//! one gates, and what each lets a plugin do.

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
        effects: &[Effect::WritesLibrary(LibraryKind::Genres)],
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
        effects: &[Effect::WritesLibrary(LibraryKind::RecordLabels)],
    },
    Libraries => {
        id: "lyra.libraries",
        module: libraries::LibrariesModule::module_spec,
        effects: &[],
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
        functions: &["delete_for_track"],
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
        functions: &["on_update"],
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

/// Why a plugin has the risk it has.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RiskReason {
    /// It lists no effects at all.
    NoEffects,
    /// None of the effects it lists raises its risk.
    NoRaisingEffects,
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
            Self::NoEffects | Self::NoRaisingEffects => Risk::Standard,
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
            Self::NoEffects => "No listed effects beyond reading the shared library".into(),
            Self::NoRaisingEffects => "No listed effects that raise its risk".into(),
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
    /// Every reason at `risk`, none below it.
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
        if reasons.is_empty() {
            reasons.push(if effects.is_empty() {
                RiskReason::NoEffects
            } else {
                RiskReason::NoRaisingEffects
            });
        }
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
    .with_scope_parents(Scope::ALL.iter().filter_map(|scope| {
        let def = scope.def();
        match def.gate {
            Gate::Functions { within, .. } => Some((def.id, within.def().id)),
            Gate::Module(_) => None,
        }
    }))
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
            let risk = RiskAssessment::of(all.iter().map(|effect| &effect.effect));
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
            ("lyra.ids", &own),
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
        assert_eq!(
            reasons(&[]),
            ["No listed effects beyond reading the shared library"]
        );
        assert_eq!(
            reasons(&[Effect::Egress]),
            ["No listed effects that raise its risk"]
        );
        assert_eq!(
            reasons(&[Effect::ReadsPersonal(history, Users::All)]),
            ["No listed effects that raise its risk"]
        );
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
                "lyra.favorites",
                &[
                    "Reads the favorites of the users it acts for",
                    "Changes the favorites of the users it acts for",
                ],
            ),
            ("lyra.genres", &["Changes the library's genres"]),
            ("lyra.labels", &["Changes the library's record labels"]),
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
            ("lyra.releases", &["Changes the library's track chains"]),
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
            ("lyra.listens.all", "lyra.listens"),
            ("lyra.lyrics.all", "lyra.lyrics"),
            ("lyra.playback_sessions.all", "lyra.playback_sessions"),
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
        assert_eq!(gain.risk.risk, Risk::High);
        assert_eq!(
            RiskAssessment::of(&[
                Effect::Egress,
                Effect::ReadsPersonal(PersonalKind::ListeningHistory, Users::Acting),
                Effect::WritesPersonal(PersonalKind::ListeningHistory, Users::Acting),
            ])
            .risk,
            Risk::Elevated,
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
}
