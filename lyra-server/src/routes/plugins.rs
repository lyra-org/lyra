// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeMap,
    HashMap,
};

use agdb::{
    DbAny,
    DbId,
};
#[cfg(feature = "docgen")]
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{
        Path,
        Query,
    },
    http::{
        HeaderMap,
        StatusCode,
    },
};
use axum::{
    Router,
    routing::{
        delete,
        get,
        patch,
        post,
    },
};
use harmony_repository::SourceRecord;
use serde::{
    Deserialize,
    Serialize,
};

use crate::routes::request::bearer_credential;
use crate::{
    plugins::{
        lifecycle::PluginRestartError,
        repositories as repositories_service,
        scopes::{
            Effect,
            PluginEffect,
            Risk,
            RiskAssessment,
            RiskReason,
            ScopeGain,
            Users,
        },
        settings::{
            self as plugin_settings_registry,
            FieldDefinition,
            FieldGroupDefinition,
            FieldProps,
            Registry,
            Schema,
            SettingsScope,
        },
    },
    routes::AppError,
    services::STATE,
    services::plugin_scope::PluginId,
    services::{
        auth::{
            Principal,
            require_authenticated,
            require_manage_plugins,
        },
        settings::plugins as settings_service,
    },
};

fn map_repository_error(error: repositories_service::PluginRepoError) -> AppError {
    use repositories_service::PluginRepoError;
    match error {
        PluginRepoError::BadRequest(message) => AppError::bad_request(message),
        PluginRepoError::NotFound(message) => AppError::not_found(message),
        PluginRepoError::Conflict(message) => AppError::conflict(message),
        PluginRepoError::BadGateway(message) => AppError::bad_gateway(message),
        PluginRepoError::Unavailable {
            message,
            retry_after: Some(retry_after),
        } => AppError::service_unavailable_for(message, retry_after),
        PluginRepoError::Unavailable { message, .. } => AppError::service_unavailable(message),
        PluginRepoError::Internal(error) => error.into(),
    }
}

fn map_settings_state_error(error: anyhow::Error) -> AppError {
    if error
        .downcast_ref::<settings_service::InvalidStoredSettings>()
        .is_some()
    {
        AppError::conflict(format!("{error:#}"))
    } else {
        error.into()
    }
}

fn map_plugin_restart_error(error: PluginRestartError) -> AppError {
    match &error {
        PluginRestartError::NotFound(plugin_id) => {
            AppError::not_found(format!("plugin not found: {plugin_id}"))
        }
        PluginRestartError::RuntimeNotReady => AppError::service_unavailable(error.to_string()),
        PluginRestartError::Failed { .. } => AppError::conflict(error.to_string()),
    }
}

/// A plugin in the plugins directory. `name`, `version` and `description`
/// are its manifest's: the one it loaded with, or the one on disk when it
/// did not load. When that manifest cannot be read, `name` is the `id`,
/// `description` is empty, and `version` is the installed release of a
/// repository plugin and absent otherwise.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginResponse {
    /// The name of the plugin's directory, which a loaded plugin's manifest
    /// id matches.
    id: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    description: String,
    load: LoadStateResponse,
    source: PluginSourceResponse,
}

/// Whether the plugin runtime runs the plugin. `not_loaded` plugins were
/// left out when the plugins were last loaded, for the `reason`: a manifest
/// that cannot be read or that requires another server version, a
/// dependency that is missing or did not load, or an install since then.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum LoadStateResponse {
    Loaded,
    NotLoaded { reason: String },
}

/// Where an installed plugin came from. `local` plugins were placed in the
/// plugins directory by hand and are not managed by repository tooling.
/// `invalid` plugins carry a source record that could not be read, for
/// example one written by an older schema; uninstall or reinstall them.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PluginSourceResponse {
    Local,
    Invalid {
        error: String,
    },
    /// Installed from a repository. `status` compares the installed version
    /// with the release the subscribed repository of the same origin and ref
    /// offered at its last refresh or update check: `up_to_date`, always for
    /// pinned plugins; `update_available`; `incompatible`, with the
    /// `requires` that excludes this server, when no release runs on it; or
    /// `unknown`, with the `reason`, when no such subscription lists the
    /// plugin, or its repository has no offer for it for another reason,
    /// such as a missing `versions.json`. `offered` is the
    /// release on offer, and `hold` why an available update to it waits for
    /// an admin.
    Repository(Box<RepositorySourceResponse>),
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RepositorySourceResponse {
    origin: String,
    /// The ref whose `versions.json` the release was chosen from.
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    /// The installed release.
    version: String,
    /// The commit the installed release came from.
    commit: String,
    /// True when `ref` is a tag or commit, so updates never move the
    /// plugin. Branch installs track new releases.
    pinned: bool,
    /// True when the plugin's own code comes from the official Lyra
    /// repository's default branch and is held in that repository itself.
    /// It does not cover the plugin's dependencies, and is never derived
    /// from `repository.json`, `plugin.json`, a plugin id or a name.
    official: bool,
    #[serde(flatten)]
    status: UpdateStatusResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    offered: Option<OfferedReleaseResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hold: Option<HoldResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    installed_at: Option<String>,
    /// True when an automatic update, rather than a person, installed it
    /// at `installed_at`.
    installed_automatically: bool,
}

/// A release a repository offers a plugin on this server.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct OfferedReleaseResponse {
    /// The plugin's version at the release.
    version: String,
    /// The release commit an update request names to approve it.
    commit: String,
    /// The scopes the plugin declares at the release.
    scopes: Vec<String>,
}

/// Why an update waits for an admin. `adds_scopes`: the release lets the
/// plugin, or installed plugins depending on it, reach scopes they could
/// not before, through their own scopes or their dependencies', listed in
/// `gains`; an update request naming the release grants them. `breaks_dependents`: `dependents` require versions
/// of this plugin outside the release's. `unmet_dependencies`: the release
/// requires `dependencies` that would be missing or at other versions.
/// `dependency_cycle`: the release would leave `plugins` depending on each
/// other in a cycle, which none of them loads in. These judge the plugins
/// as they would be after the update, and no update applies until they
/// change.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
enum HoldResponse {
    AddsScopes {
        gains: Vec<ScopeGainResponse>,
    },
    BreaksDependents {
        dependents: Vec<RequirementResponse>,
    },
    UnmetDependencies {
        dependencies: Vec<RequirementResponse>,
    },
    DependencyCycle {
        plugins: Vec<String>,
    },
}

/// What one plugin would newly reach, and how its risk changes.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ScopeGainResponse {
    plugin: String,
    /// The plugin's risk once it reaches them, from everything it reaches
    /// then.
    risk: RiskResponse,
    /// The plugin's risk level now, from everything it reaches now;
    /// `standard` for a plugin not installed.
    previous_risk: RiskLevelResponse,
    /// What the newly reached scopes let the plugin do.
    effects: Vec<EffectResponse>,
    /// Scopes the plugin would newly reach.
    scopes: Vec<String>,
    /// Dependencies the plugin would newly reach whose scopes are unknown.
    unresolved_dependencies: Vec<String>,
}

/// One plugin's requirement on another: the dependent's on the held
/// plugin, or the held release's on a dependency.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RequirementResponse {
    id: String,
    requires: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ChoiceOptionResponse {
    value: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FieldPropsResponse {
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    required: bool,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "type")]
enum FieldResponse {
    #[serde(rename = "string")]
    String {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        value: Option<String>,
    },
    #[serde(rename = "number")]
    Number {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        value: Option<f64>,
    },
    #[serde(rename = "bool")]
    Bool {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        value: Option<bool>,
    },
    #[serde(rename = "choice")]
    Choice {
        key: String,
        #[serde(flatten)]
        props: FieldPropsResponse,
        options: Vec<ChoiceOptionResponse>,
        value: Option<String>,
    },
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct GroupResponse {
    id: String,
    label: String,
    fields: Vec<FieldResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginSettingsResponse {
    groups: Vec<GroupResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PluginSettingsStatus {
    Ready { groups: Vec<GroupResponse> },
    Initializing,
    NotDeclared,
    Invalid { message: String },
}

/// One plugin in a settings listing. Listings only include plugins that
/// declare the requested scope, so the entry carries enough of the manifest
/// for a client to present the plugin without the manifest list.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginSettingsEntry {
    plugin_id: String,
    name: String,
    version: String,
    #[serde(flatten)]
    status: PluginSettingsStatus,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdateSettingsRequest {
    values: HashMap<String, serde_json::Value>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RepositoryUrlRequest {
    url: String,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
}

/// A plugin to install, bound to the release its preview showed.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PreviewedPluginRequest {
    id: String,
    /// The `commit` the preview showed for the plugin. When the repository
    /// offers another release now, the plugin fails to install.
    commit: String,
}

impl PreviewedPluginRequest {
    fn into_previewed(self) -> repositories_service::PreviewedPlugin {
        repositories_service::PreviewedPlugin {
            id: self.id,
            commit: self.commit,
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct InstallPluginsRequest {
    url: String,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
    /// Plugins to install. An empty list is rejected.
    plugins: Vec<PreviewedPluginRequest>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct RepositoryInstallRequest {
    /// Plugins to install. An empty list is rejected.
    plugins: Vec<PreviewedPluginRequest>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct UpdatePluginsRequest {
    /// Plugins to update, each to the release `commit` its status offered,
    /// granting the scopes that release lets plugins reach. Omitted updates
    /// every repository-managed plugin whose update is not held. An empty
    /// list is rejected.
    #[serde(default)]
    plugins: Option<Vec<PreviewedPluginRequest>>,
}

/// Where an installed plugin stands against its subscribed repository.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum UpdateStatusResponse {
    UpToDate,
    UpdateAvailable,
    Incompatible { requires: String },
    Unknown { reason: String },
}

/// Where a catalogue plugin stands relative to the plugins directory.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CatalogStatusResponse {
    Available,
    UpToDate,
    UpdateAvailable,
    Incompatible { requires: String },
    Unknown { reason: String },
    Local,
}

/// Set when a multi-plugin index points at a plugin living in another
/// repository.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginPreviewSourceResponse {
    origin: String,
}

/// A plugin a repository offers, as an install would take it. `status`
/// says where it stands against the plugins directory: `available`;
/// `up_to_date` or `update_available`, comparing the installed version with
/// the offered release's; `incompatible`, with the `requires` that excludes
/// this server, when no release the repository offers runs on it; `unknown`,
/// with the `reason`, when its source record cannot be read or the
/// repository has no offer for it for another reason, which `error` then
/// gives too; or `local` when it was installed without a source record and
/// repository tooling leaves it alone.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginPreviewResponse {
    id: String,
    name: String,
    /// The plugin's version at the release an install would choose: the
    /// first in its repository's `versions.json` that accepts this server.
    version: String,
    description: String,
    /// Capability scopes the chosen release declares, which installing it
    /// grants.
    scopes: Vec<String>,
    /// What the chosen release could do installed; absent with `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    access: Option<PluginAccessResponse>,
    /// The commit of the release an install would choose.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    #[serde(flatten)]
    status: CatalogStatusResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<PluginPreviewSourceResponse>,
    /// True when the plugin's own code comes from the official Lyra
    /// repository's default branch and is held in that repository itself.
    /// It does not cover the plugin's dependencies, and is never derived
    /// from `repository.json`, `plugin.json`, a plugin id or a name.
    official: bool,
    /// Why the plugin has no installable release, such as all of them
    /// requiring another Lyra version; `name`, `version`, `description` and
    /// `scopes` are then empty. Installing the plugin fails, but not the
    /// repository's others.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// What a plugin can do once installed: through its own scopes, and
/// through every plugin it depends on, since it can use what they
/// `require`.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginAccessResponse {
    risk: RiskResponse,
    effects: Vec<EffectResponse>,
    /// Each scope reached, with the plugins declaring it.
    reached_scopes: Vec<ReachedScopeResponse>,
    /// Dependencies, direct or not, neither installed nor in the
    /// repository, so what they can do is unknown.
    unresolved_dependencies: Vec<String>,
    /// Installed plugins that depend on this one and would reach more once
    /// it is installed.
    widens: Vec<ScopeGainResponse>,
}

/// How much care letting a plugin run calls for, and what sets it.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RiskResponse {
    level: RiskLevelResponse,
    /// What the plugin can do that sets `level`; empty at `standard`.
    reasons: Vec<RiskReasonResponse>,
}

impl From<RiskAssessment> for RiskResponse {
    fn from(assessment: RiskAssessment) -> Self {
        Self {
            level: assessment.risk.into(),
            reasons: assessment.reasons.into_iter().map(Into::into).collect(),
        }
    }
}

/// `high` when the plugin adds endpoints, handles credentials, or can send
/// data off the server and touch every user's personal data, `elevated` when
/// it can send data off the server and touch personal or library data, else
/// `standard`.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RiskLevelResponse {
    Standard,
    Elevated,
    High,
}

impl From<Risk> for RiskLevelResponse {
    fn from(risk: Risk) -> Self {
        match risk {
            Risk::Standard => Self::Standard,
            Risk::Elevated => Self::Elevated,
            Risk::High => Self::High,
        }
    }
}

/// `egress`: sends data off the server. `exposes`: adds endpoints to the
/// server or listens on the network. `authority`: signs users in, checks
/// their credentials and manages their sessions. `reads_personal` and
/// `writes_personal`: reads or changes the personal `data` of the `users` it
/// names. `writes_library`: changes the shared library's `data`.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum EffectKind {
    Egress,
    Exposes,
    Authority,
    ReadsPersonal,
    WritesPersonal,
    WritesLibrary,
}

/// One thing a plugin can do. `kind`, `data` and `users` are the
/// machine-readable truth; `description` says the same in English for
/// display and is not to be parsed.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct EffectResponse {
    kind: EffectKind,
    /// The stable id of the data the effect concerns, for the kinds that
    /// name one, such as `listening_history` or `record_labels`.
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'static str>,
    /// Whose personal data it reaches, for the personal kinds.
    #[serde(skip_serializing_if = "Option::is_none")]
    users: Option<UsersResponse>,
    /// The scopes giving the effect, each with the plugin declaring it: the
    /// plugin itself or one of its dependencies.
    via: Vec<EffectSourceResponse>,
    /// The effect as a sentence-case list item, such as "Reads every user's
    /// listening history".
    description: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct EffectSourceResponse {
    plugin: String,
    scope: &'static str,
    /// Why the plugin says it needs the scope: its author's own unverified
    /// plain text, not checked by the server and not translated. Never render
    /// it as HTML. Omitted when there is none. A scope with several effects
    /// repeats it under each, so clients may group by plugin and scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    author_reason: Option<String>,
}

/// `exposes` and `authority`: as the effects of those kinds.
/// `sends_personal`: can send the personal `data` of the `users` it names
/// off the server. `changes_personal`: can change that data and send data
/// off the server. `changes_library`: can change the library's `data` and
/// send data off the server.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RiskReasonKind {
    Exposes,
    Authority,
    SendsPersonal,
    ChangesPersonal,
    ChangesLibrary,
}

/// What raises a plugin's risk. `kind`, `data` and `users` are the
/// machine-readable truth, as on effects; `description` is for display.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RiskReasonResponse {
    kind: RiskReasonKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    users: Option<UsersResponse>,
    /// The reason as a sentence-case list item, such as "Can send every
    /// user's listening history off the server".
    description: String,
}

impl From<RiskReason> for RiskReasonResponse {
    fn from(reason: RiskReason) -> Self {
        let (kind, data, users) = match reason {
            RiskReason::Exposes => (RiskReasonKind::Exposes, None, None),
            RiskReason::Authority => (RiskReasonKind::Authority, None, None),
            RiskReason::SendsPersonal(data, users) => {
                (RiskReasonKind::SendsPersonal, Some(data.id()), Some(users))
            }
            RiskReason::ChangesPersonal(data, users) => (
                RiskReasonKind::ChangesPersonal,
                Some(data.id()),
                Some(users),
            ),
            RiskReason::ChangesLibrary(data) => {
                (RiskReasonKind::ChangesLibrary, Some(data.id()), None)
            }
        };
        Self {
            kind,
            data,
            users: users.map(Into::into),
            description: reason.description(),
        }
    }
}

/// `acting`: the users the plugin acts for. `all`: every user's, whoever
/// it acts for.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum UsersResponse {
    Acting,
    All,
}

impl From<Users> for UsersResponse {
    fn from(users: Users) -> Self {
        match users {
            Users::Acting => Self::Acting,
            Users::All => Self::All,
        }
    }
}

impl From<PluginEffect> for EffectResponse {
    fn from(effect: PluginEffect) -> Self {
        let (kind, data, users) = match effect.effect {
            Effect::Egress => (EffectKind::Egress, None, None),
            Effect::Exposes => (EffectKind::Exposes, None, None),
            Effect::Authority => (EffectKind::Authority, None, None),
            Effect::ReadsPersonal(data, users) => {
                (EffectKind::ReadsPersonal, Some(data.id()), Some(users))
            }
            Effect::WritesPersonal(data, users) => {
                (EffectKind::WritesPersonal, Some(data.id()), Some(users))
            }
            Effect::WritesLibrary(data) => (EffectKind::WritesLibrary, Some(data.id()), None),
        };
        Self {
            kind,
            data,
            users: users.map(Into::into),
            description: effect.effect.description(),
            via: effect
                .via
                .into_iter()
                .map(|source| EffectSourceResponse {
                    plugin: source.plugin,
                    scope: source.scope.def().id,
                    author_reason: source.author_reason,
                })
                .collect(),
        }
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct ReachedScopeResponse {
    scope: String,
    via: Vec<String>,
}

fn plugin_access_response(access: repositories_service::PluginAccess) -> PluginAccessResponse {
    PluginAccessResponse {
        risk: access.risk.into(),
        effects: access.effects.into_iter().map(Into::into).collect(),
        reached_scopes: access
            .scopes
            .into_iter()
            .map(|(scope, via)| ReachedScopeResponse {
                scope,
                via: via.into_iter().collect(),
            })
            .collect(),
        unresolved_dependencies: access.unresolved.into_iter().collect(),
        widens: scope_gain_responses(access.widens),
    }
}

fn scope_gain_responses(gains: BTreeMap<String, ScopeGain>) -> Vec<ScopeGainResponse> {
    gains
        .into_iter()
        .map(|(plugin, gain)| ScopeGainResponse {
            plugin,
            risk: gain.risk.into(),
            previous_risk: gain.previous_risk.into(),
            effects: gain.effects.into_iter().map(Into::into).collect(),
            scopes: gain.growth.scopes.into_iter().collect(),
            unresolved_dependencies: gain.growth.unresolved.into_iter().collect(),
        })
        .collect()
}

/// A repository URL as resolved now, with the plugins it provides, before
/// subscribing to it.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RepositoryPreviewResponse {
    origin: String,
    name: String,
    description: String,
    /// The subscribed or requested ref, when one was given.
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    /// The branch, tag, or commit the repository resolved to.
    resolved_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    plugins: Vec<PluginPreviewResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct InstalledPluginResponse {
    id: String,
    version: String,
    commit: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FailedInstallResponse {
    id: String,
    error: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct InstallPluginsResponse {
    installed: Vec<InstalledPluginResponse>,
    /// Installed plugins whose reinstall would leave a plugin unable to
    /// load; nothing was downloaded for them.
    held: Vec<HeldPluginResponse>,
    failed: Vec<FailedInstallResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct UpdatedPluginResponse {
    id: String,
    version: String,
    commit: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct UpdatePluginsResponse {
    updated: Vec<UpdatedPluginResponse>,
    up_to_date: Vec<String>,
    /// Plugins no release of their repository runs on this server; nothing
    /// was downloaded for them.
    incompatible: Vec<IncompatiblePluginResponse>,
    /// Plugins whose newer release waits for an admin; nothing was
    /// downloaded for them.
    held: Vec<HeldPluginResponse>,
    failed: Vec<FailedInstallResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct HeldPluginResponse {
    id: String,
    /// The held release.
    release: OfferedReleaseResponse,
    hold: HoldResponse,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct IncompatiblePluginResponse {
    id: String,
    /// The server requirement that excludes this server.
    requires: String,
}

/// A subscribed repository as the server stored it at its last refresh,
/// update check, or daily check.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct PluginRepositoryResponse {
    id: String,
    origin: String,
    /// From the `repository.json` index when present, else the repository
    /// name.
    name: String,
    description: String,
    /// The subscribed ref, when one was given.
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    /// The branch, tag, or commit the repository resolved to at the last
    /// refresh; absent until then.
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_ref: Option<String>,
    /// Commit seen at the last refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    /// Absent until the first refresh.
    #[serde(skip_serializing_if = "Option::is_none")]
    refreshed_at: Option<String>,
    /// The plugins offered at the last refresh, in the order the repository
    /// lists them, each with where it stands against the plugins directory
    /// now; present when requested with
    /// `inc=plugins`, and always on adding or refreshing.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugins: Option<Vec<PluginPreviewResponse>>,
}

/// A subscribed repository that could not be refreshed.
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct FailedRefreshResponse {
    /// The repository's `id`.
    id: String,
    error: String,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Serialize)]
struct RefreshPluginRepositoriesResponse {
    /// Every subscribed repository with its `plugins`, as stored after the
    /// refresh; one that failed keeps what it stored before.
    repositories: Vec<PluginRepositoryResponse>,
    failed: Vec<FailedRefreshResponse>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
struct PluginRepositoryQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Comma-separated or repeated values: plugins.")
    )]
    #[serde(default, deserialize_with = "crate::routes::deserialize_inc")]
    inc: Option<Vec<String>>,
}

fn includes_plugins(inc: Option<Vec<String>>) -> Result<bool, AppError> {
    let values = crate::routes::parse_inc_values(inc, &["plugins"])?;
    Ok(values.iter().any(|value| value == "plugins"))
}

fn update_status_response(status: repositories_service::UpdateStatus) -> UpdateStatusResponse {
    use repositories_service::UpdateStatus;
    match status {
        UpdateStatus::UpToDate => UpdateStatusResponse::UpToDate,
        UpdateStatus::UpdateAvailable => UpdateStatusResponse::UpdateAvailable,
        UpdateStatus::Incompatible { requires } => UpdateStatusResponse::Incompatible { requires },
        UpdateStatus::Unknown { reason } => UpdateStatusResponse::Unknown { reason },
    }
}

fn catalog_status_response(status: repositories_service::CatalogStatus) -> CatalogStatusResponse {
    use repositories_service::{
        CatalogStatus,
        UpdateStatus,
    };
    match status {
        CatalogStatus::Available => CatalogStatusResponse::Available,
        CatalogStatus::Installed(UpdateStatus::UpToDate) => CatalogStatusResponse::UpToDate,
        CatalogStatus::Installed(UpdateStatus::UpdateAvailable) => {
            CatalogStatusResponse::UpdateAvailable
        }
        CatalogStatus::Installed(UpdateStatus::Incompatible { requires }) => {
            CatalogStatusResponse::Incompatible { requires }
        }
        CatalogStatus::Installed(UpdateStatus::Unknown { reason }) => {
            CatalogStatusResponse::Unknown { reason }
        }
        CatalogStatus::Local => CatalogStatusResponse::Local,
    }
}

fn plugin_preview_responses(
    plugins: Vec<repositories_service::PluginPreview>,
) -> Vec<PluginPreviewResponse> {
    plugins
        .into_iter()
        .map(|plugin| PluginPreviewResponse {
            id: plugin.id,
            name: plugin.name,
            version: plugin.version,
            description: plugin.description,
            scopes: plugin.scopes,
            access: plugin.access.map(plugin_access_response),
            commit: plugin.commit,
            status: catalog_status_response(plugin.status),
            source: plugin
                .source_origin
                .map(|origin| PluginPreviewSourceResponse { origin }),
            official: plugin.official,
            error: plugin.error,
        })
        .collect()
}

fn repository_preview_response(
    preview: repositories_service::RepositoryPreview,
) -> RepositoryPreviewResponse {
    RepositoryPreviewResponse {
        origin: preview.origin,
        name: preview.name,
        description: preview.description,
        git_ref: preview.git_ref,
        resolved_ref: preview.resolved_ref,
        commit: preview.commit,
        plugins: plugin_preview_responses(preview.plugins),
    }
}

fn repository_response(view: repositories_service::RepositoryView) -> PluginRepositoryResponse {
    let record = view.repository;
    PluginRepositoryResponse {
        id: record.id,
        origin: record.origin,
        name: record.name,
        description: record.description,
        git_ref: record.git_ref,
        resolved_ref: record.resolved_ref,
        commit: record.commit,
        refreshed_at: record
            .refreshed_at_ms
            .map(crate::services::timestamps::unix_ms_to_rfc3339_u64),
        plugins: view.plugins.map(plugin_preview_responses),
    }
}

fn offered_release_response(
    release: repositories_service::OfferedRelease,
) -> OfferedReleaseResponse {
    OfferedReleaseResponse {
        version: release.version.to_string(),
        commit: release.commit,
        scopes: release.scopes,
    }
}

fn hold_response(hold: repositories_service::Hold) -> HoldResponse {
    use repositories_service::Hold;
    let requirements = |requirements: Vec<repositories_service::Requirement>| {
        requirements
            .into_iter()
            .map(|requirement| RequirementResponse {
                id: requirement.id,
                requires: requirement.requires,
            })
            .collect()
    };
    match hold {
        Hold::AddsScopes { gains } => HoldResponse::AddsScopes {
            gains: scope_gain_responses(gains),
        },
        Hold::BreaksDependents { dependents } => HoldResponse::BreaksDependents {
            dependents: requirements(dependents),
        },
        Hold::UnmetDependencies { dependencies } => HoldResponse::UnmetDependencies {
            dependencies: requirements(dependencies),
        },
        Hold::DependencyCycle { plugins } => HoldResponse::DependencyCycle { plugins },
    }
}

fn source_response(
    record: SourceRecord,
    status: repositories_service::InstalledStatus,
) -> PluginSourceResponse {
    PluginSourceResponse::Repository(Box::new(RepositorySourceResponse {
        official: repositories_service::is_official_install(&record),
        status: update_status_response(status.status),
        offered: status.offered.map(offered_release_response),
        hold: status.hold.map(hold_response),
        origin: record.origin,
        git_ref: record.git_ref,
        version: record.version.to_string(),
        commit: record.commit,
        pinned: record.pinned,
        installed_at: record.installed_at,
        installed_automatically: record.installed_automatically,
    }))
}

fn plugin_response(installation: repositories_service::Installation) -> PluginResponse {
    use repositories_service::{
        InstallSource,
        LoadState,
    };
    PluginResponse {
        id: installation.id,
        name: installation.name,
        version: installation.version,
        description: installation.description,
        load: match installation.load {
            LoadState::Loaded => LoadStateResponse::Loaded,
            LoadState::NotLoaded { reason } => LoadStateResponse::NotLoaded { reason },
        },
        source: match installation.source {
            InstallSource::Local => PluginSourceResponse::Local,
            InstallSource::Invalid(error) => PluginSourceResponse::Invalid {
                error: error.to_string(),
            },
            InstallSource::Repository(install) => source_response(install.record, install.status),
        },
    }
}

fn props_to_response(props: &FieldProps) -> FieldPropsResponse {
    FieldPropsResponse {
        label: props.label.clone(),
        description: props.description.clone(),
        required: props.required,
    }
}

fn resolve_string(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<String> {
    stored
        .get(key)
        .and_then(|v| v.as_str().map(String::from))
        .or_else(|| default.as_ref().and_then(|v| v.as_str().map(String::from)))
}

fn resolve_number(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<f64> {
    stored
        .get(key)
        .and_then(|v| v.as_f64())
        .or_else(|| default.as_ref().and_then(|v| v.as_f64()))
}

fn resolve_bool(
    stored: &HashMap<String, serde_json::Value>,
    key: &str,
    default: &Option<serde_json::Value>,
) -> Option<bool> {
    stored
        .get(key)
        .and_then(|v| v.as_bool())
        .or_else(|| default.as_ref().and_then(|v| v.as_bool()))
}

fn field_to_response(
    field: &FieldDefinition,
    stored: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<FieldResponse> {
    let key = field.key();
    if let Some(value) = stored.get(key) {
        field.validate_value(value)?;
    }

    match field {
        FieldDefinition::String { key, props } => Ok(FieldResponse::String {
            key: key.clone(),
            value: resolve_string(stored, key, &props.default_value),
            props: props_to_response(props),
        }),
        FieldDefinition::Number {
            key,
            props,
            min,
            max,
        } => Ok(FieldResponse::Number {
            key: key.clone(),
            value: resolve_number(stored, key, &props.default_value),
            props: props_to_response(props),
            min: *min,
            max: *max,
        }),
        FieldDefinition::Bool { key, props } => Ok(FieldResponse::Bool {
            key: key.clone(),
            value: resolve_bool(stored, key, &props.default_value),
            props: props_to_response(props),
        }),
        FieldDefinition::Choice {
            key,
            props,
            options,
        } => Ok(FieldResponse::Choice {
            key: key.clone(),
            value: resolve_string(stored, key, &props.default_value),
            props: props_to_response(props),
            options: options
                .iter()
                .map(|o| ChoiceOptionResponse {
                    value: o.value.clone(),
                    label: o.label.clone(),
                    description: o.description.clone(),
                })
                .collect(),
        }),
    }
}

fn group_to_response(
    group: &FieldGroupDefinition,
    stored: &HashMap<String, serde_json::Value>,
) -> anyhow::Result<GroupResponse> {
    let fields = group
        .fields
        .iter()
        .map(|field| field_to_response(field, stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(GroupResponse {
        id: group.id.clone(),
        label: group.label.clone(),
        fields,
    })
}

async fn load_registered_schema(plugin_id: &str, scope: SettingsScope) -> Result<Schema, AppError> {
    let registry = plugin_settings_registry::settings_registry()
        .read_owned()
        .await;
    crate::services::plugin_scope::PluginId::new(plugin_id.to_string())
        .map_err(|_| AppError::not_found(format!("plugin not found: {plugin_id}")))?;
    if !registry.is_frozen() && registry.get_schema(plugin_id, scope).is_none() {
        return Err(AppError::service_unavailable(
            "plugin settings are still initializing",
        ));
    }
    registry
        .get_schema(plugin_id, scope)
        .cloned()
        .ok_or_else(|| AppError::not_found(format!("plugin not found: {plugin_id}")))
}

async fn load_settings_response(
    plugin_id: String,
    schema: Schema,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let stored = settings_service::load_validated_stored_values(
        &*STATE.db.read().await,
        &plugin_id,
        &schema,
    )
    .map_err(map_settings_state_error)?;
    let groups = schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(Json(PluginSettingsResponse { groups }))
}

/// Whose stored settings a status reads: the server's, or one verified user's.
#[derive(Clone, Copy)]
enum SettingsTarget {
    Server,
    User(DbId),
}

impl SettingsTarget {
    fn scope(self) -> SettingsScope {
        match self {
            Self::Server => SettingsScope::Global,
            Self::User(_) => SettingsScope::User,
        }
    }
}

fn build_status(
    registry: &Registry,
    db: &DbAny,
    plugin_id: &str,
    target: SettingsTarget,
) -> PluginSettingsStatus {
    let scope = target.scope();
    if PluginId::new(plugin_id.to_string()).is_err() {
        return PluginSettingsStatus::NotDeclared;
    }

    let schema = match registry.get_schema(plugin_id, scope) {
        Some(schema) => schema,
        None if !registry.is_frozen() => {
            return PluginSettingsStatus::Initializing;
        }
        None => return PluginSettingsStatus::NotDeclared,
    };

    let stored = match target {
        SettingsTarget::User(user_db_id) => {
            settings_service::load_validated_user_stored_values(db, user_db_id, plugin_id, schema)
        }
        SettingsTarget::Server => {
            settings_service::load_validated_stored_values(db, plugin_id, schema)
        }
    };
    let stored = match stored {
        Ok(values) => values,
        Err(error) => {
            return PluginSettingsStatus::Invalid {
                message: format!("{error:#}"),
            };
        }
    };

    match schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(groups) => PluginSettingsStatus::Ready { groups },
        Err(error) => PluginSettingsStatus::Invalid {
            message: format!("{error:#}"),
        },
    }
}

async fn list_plugins(headers: HeaderMap) -> Result<Json<Vec<PluginResponse>>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    // One scope for both, so a reload between the reads cannot pair one runtime's plugins with
    // another's errors.
    let (manifests, errors) = crate::plugins::STATE.discovered();
    let plugins_dir = STATE.boot.get().plugins_dir.clone();
    let subscriptions = repositories_service::list_subscriptions()
        .await
        .map_err(map_repository_error)?;
    let installations = tokio::task::block_in_place(|| {
        repositories_service::installations(&manifests, &errors, &plugins_dir, &subscriptions)
    })
    .map_err(map_repository_error)?;
    Ok(Json(
        installations.into_iter().map(plugin_response).collect(),
    ))
}

/// Whose settings a listing reports, resolved under the guard that reads them.
enum SettingsOwner<'a> {
    Server,
    User(&'a Principal),
}

impl SettingsOwner<'_> {
    fn resolve(&self, db: &DbAny) -> Result<SettingsTarget, AppError> {
        match self {
            Self::Server => Ok(SettingsTarget::Server),
            Self::User(principal) => Ok(SettingsTarget::User(principal.require(db)?)),
        }
    }
}

async fn collect_settings_entries(
    owner: SettingsOwner<'_>,
) -> Result<Vec<PluginSettingsEntry>, AppError> {
    let manifests = crate::plugins::STATE.manifests();
    let registry = plugin_settings_registry::settings_registry()
        .read_owned()
        .await;
    let db = STATE.db.read().await;
    let target = owner.resolve(&db)?;

    Ok(manifests
        .as_ref()
        .iter()
        .filter_map(|manifest| {
            let status = build_status(&registry, &db, &manifest.id, target);
            (!matches!(status, PluginSettingsStatus::NotDeclared)).then(|| PluginSettingsEntry {
                plugin_id: manifest.id.clone(),
                name: manifest.name.clone(),
                version: manifest.version.to_string(),
                status,
            })
        })
        .collect())
}

async fn list_all_settings(headers: HeaderMap) -> Result<Json<Vec<PluginSettingsEntry>>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    Ok(Json(collect_settings_entries(SettingsOwner::Server).await?))
}

async fn list_all_user_settings(
    headers: HeaderMap,
) -> Result<Json<Vec<PluginSettingsEntry>>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    Ok(Json(
        collect_settings_entries(SettingsOwner::User(&principal)).await?,
    ))
}

async fn restart_plugin(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let plugin_id = PluginId::new(plugin_id)
        .map_err(|err| AppError::bad_request(format!("invalid plugin id: {err}")))?;

    crate::plugins::STATE
        .generation()
        .registries
        .restart_plugin(&plugin_id)
        .await
        .map_err(map_plugin_restart_error)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn resolve_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<RepositoryPreviewResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let preview = repositories_service::resolve_preview(&request.url, request.git_ref.as_deref())
        .await
        .map_err(map_repository_error)?;
    Ok(Json(repository_preview_response(preview)))
}

async fn install_plugins(
    headers: HeaderMap,
    Json(request): Json<InstallPluginsRequest>,
) -> Result<Json<InstallPluginsResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let plugins: Vec<_> = request
        .plugins
        .into_iter()
        .map(PreviewedPluginRequest::into_previewed)
        .collect();
    let report = repositories_service::install(&request.url, request.git_ref.as_deref(), &plugins)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(install_report_response(report)))
}

async fn install_repository_plugins(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
    Json(request): Json<RepositoryInstallRequest>,
) -> Result<Json<InstallPluginsResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let plugins: Vec<_> = request
        .plugins
        .into_iter()
        .map(PreviewedPluginRequest::into_previewed)
        .collect();
    let report = repositories_service::install_from_repository(&repository_id, &plugins)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(install_report_response(report)))
}

fn install_report_response(report: repositories_service::InstallReport) -> InstallPluginsResponse {
    InstallPluginsResponse {
        installed: report
            .installed
            .into_iter()
            .map(|plugin| InstalledPluginResponse {
                id: plugin.id,
                version: plugin.version,
                commit: plugin.commit,
            })
            .collect(),
        held: report.held.into_iter().map(held_plugin_response).collect(),
        failed: report
            .failed
            .into_iter()
            .map(|failure| FailedInstallResponse {
                id: failure.id,
                error: failure.error,
            })
            .collect(),
    }
}

fn held_plugin_response(plugin: repositories_service::HeldPlugin) -> HeldPluginResponse {
    HeldPluginResponse {
        id: plugin.id,
        release: offered_release_response(plugin.release),
        hold: hold_response(plugin.hold),
    }
}

async fn update_installed_plugins(
    headers: HeaderMap,
    Json(request): Json<UpdatePluginsRequest>,
) -> Result<Json<UpdatePluginsResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let approved: Option<Vec<_>> = request.plugins.map(|plugins| {
        plugins
            .into_iter()
            .map(PreviewedPluginRequest::into_previewed)
            .collect()
    });
    let selection = match &approved {
        Some(plugins) => repositories_service::UpdateSelection::Approved(plugins),
        None => repositories_service::UpdateSelection::All,
    };
    let report = repositories_service::update_plugins(selection)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(update_report_response(report)))
}

fn update_report_response(report: repositories_service::UpdateReport) -> UpdatePluginsResponse {
    UpdatePluginsResponse {
        updated: report
            .updated
            .into_iter()
            .map(|plugin| UpdatedPluginResponse {
                id: plugin.id,
                version: plugin.version,
                commit: plugin.commit,
            })
            .collect(),
        up_to_date: report.up_to_date,
        incompatible: report
            .incompatible
            .into_iter()
            .map(|plugin| IncompatiblePluginResponse {
                id: plugin.id,
                requires: plugin.requires,
            })
            .collect(),
        held: report.held.into_iter().map(held_plugin_response).collect(),
        failed: report
            .failed
            .into_iter()
            .map(|failure| FailedInstallResponse {
                id: failure.id,
                error: failure.error,
            })
            .collect(),
    }
}

async fn reload_plugins(headers: HeaderMap) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    repositories_service::reload_plugins()
        .await
        .map_err(map_repository_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn uninstall_installed_plugin(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    repositories_service::uninstall(&plugin_id)
        .await
        .map_err(map_repository_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_plugin_repositories(
    headers: HeaderMap,
    Query(query): Query<PluginRepositoryQuery>,
) -> Result<Json<Vec<PluginRepositoryResponse>>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let with_plugins = includes_plugins(query.inc)?;
    let repositories = repositories_service::list_repositories(with_plugins)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(
        repositories.into_iter().map(repository_response).collect(),
    ))
}

async fn get_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
    Query(query): Query<PluginRepositoryQuery>,
) -> Result<Json<PluginRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let with_plugins = includes_plugins(query.inc)?;
    let repository = repositories_service::get_repository(&repository_id, with_plugins)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(repository_response(repository)))
}

async fn add_plugin_repository(
    headers: HeaderMap,
    Json(request): Json<RepositoryUrlRequest>,
) -> Result<Json<PluginRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let repository = repositories_service::add_repository(&request.url, request.git_ref.as_deref())
        .await
        .map_err(map_repository_error)?;
    Ok(Json(repository_response(repository)))
}

async fn refresh_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<Json<PluginRepositoryResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let repository = repositories_service::refresh_repository(&repository_id)
        .await
        .map_err(map_repository_error)?;
    Ok(Json(repository_response(repository)))
}

async fn refresh_plugin_repositories(
    headers: HeaderMap,
) -> Result<Json<RefreshPluginRepositoriesResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let report = repositories_service::refresh_repositories()
        .await
        .map_err(map_repository_error)?;
    Ok(Json(RefreshPluginRepositoriesResponse {
        repositories: report
            .repositories
            .into_iter()
            .map(repository_response)
            .collect(),
        failed: report
            .failed
            .into_iter()
            .map(|failure| FailedRefreshResponse {
                id: failure.id,
                error: failure.error,
            })
            .collect(),
    }))
}

async fn delete_plugin_repository(
    headers: HeaderMap,
    Path(repository_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    repositories_service::remove_repository(&repository_id)
        .await
        .map_err(map_repository_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::Global).await?;
    load_settings_response(plugin_id, schema).await
}

async fn update_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::Global).await?;
    let changes = settings_service::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    settings_service::apply_updates(&mut *STATE.db.write().await, &plugin_id, &schema, &changes)
        .map_err(map_settings_state_error)?;

    load_settings_response(plugin_id, schema).await
}

async fn delete_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<(), AppError> {
    let _principal = require_manage_plugins(bearer_credential(&headers)).await?;
    settings_service::clear_stored_values(&mut *STATE.db.write().await, &plugin_id)?;
    Ok(())
}

async fn load_user_settings_response(
    plugin_id: String,
    principal: &Principal,
    schema: Schema,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let db = STATE.db.read().await;
    let stored = settings_service::load_validated_user_stored_values(
        &db,
        principal.require(&db)?,
        &plugin_id,
        &schema,
    )
    .map_err(map_settings_state_error)?;
    drop(db);
    let groups = schema
        .groups
        .iter()
        .map(|group| group_to_response(group, &stored))
        .collect::<anyhow::Result<_>>()?;

    Ok(Json(PluginSettingsResponse { groups }))
}

async fn get_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::User).await?;
    load_user_settings_response(plugin_id, &principal, schema).await
}

async fn update_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<Json<PluginSettingsResponse>, AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let schema = load_registered_schema(&plugin_id, SettingsScope::User).await?;
    let changes = settings_service::validate_updates(&schema, &request.values)
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    {
        let mut db = STATE.db.write().await;
        let user_db_id = principal.require(&db)?;
        settings_service::apply_user_updates(&mut db, user_db_id, &plugin_id, &schema, &changes)
            .map_err(map_settings_state_error)?;
    }

    load_user_settings_response(plugin_id, &principal, schema).await
}

async fn delete_user_settings(
    headers: HeaderMap,
    Path(plugin_id): Path<String>,
) -> Result<(), AppError> {
    let principal = require_authenticated(bearer_credential(&headers)).await?;
    let mut db = STATE.db.write().await;
    let user_db_id = principal.require(&db)?;
    settings_service::clear_user_stored_values(&mut db, user_db_id, &plugin_id)?;
    Ok(())
}

#[cfg(feature = "docgen")]
fn get_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get plugin settings")
        .description("Returns the settings groups, with schema and current values, for a plugin.")
}

#[cfg(feature = "docgen")]
fn list_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugins").description(
        "Returns every installed plugin, sorted by id: the plugins the runtime loaded, the plugin directories it left out when it last loaded them, and plugins added since. `load` tells them apart: a `state` of `loaded`, or `not_loaded` with the `reason`, such as a manifest that requires another server version. A plugin that did not load is described by its manifest on disk when that can be read, and by its id and installed release otherwise, and can still be updated or uninstalled. Each plugin carries where it was installed from: a `source` of kind `repository` (origin, ref, version, commit, pinned, official, status, offered, hold, installed_at, installed_automatically), `local`, or `invalid` (error) when the source record could not be read and the plugin should be uninstalled or reinstalled. `pinned` is true for tag and commit installs, which updates never move. `status` compares the installed version with the release that the subscribed repository of the same origin and ref last offered this server (`up_to_date`, `update_available`, `incompatible` with the `requires` that excludes this server, or `unknown` with the `reason` when it offered nothing for another reason, such as a missing `versions.json`, or no such subscription lists the plugin), without contacting the forge; refreshing the repository or updating plugins stores the offered releases. `offered` is the release on offer (version, commit, scopes) and `hold` why an available update to it waits for an admin: `adds_scopes` with the `gains` of each plugin it would let reach new scopes, through its own or its dependencies', with what they let it do and its risk before and after, `breaks_dependents` with each dependent's id and requirement, `unmet_dependencies` with each dependency the release requires that would be missing or at another version, or `dependency_cycle` with the `plugins` it would leave depending on each other in a cycle. Dependencies are judged as if every available update were installed together. `installed_automatically` is true when the daily update check, not a request, installed the release at `installed_at`.",
    )
}

#[cfg(feature = "docgen")]
fn list_all_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all plugin settings").description(
        "Returns a list with the global settings schema and current values for every loaded plugin that declares global settings. Each entry carries the plugin's name and version plus a `status` field: `ready`, `initializing` (registry not yet frozen), or `invalid` (stored state is stale or malformed).",
    )
}

#[cfg(feature = "docgen")]
fn list_all_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List all user plugin settings").description(
        "Returns a list with the user-scoped settings schema and the authenticated user's current values for every loaded plugin that declares user settings. Entries mirror the admin list endpoint.",
    )
}

#[cfg(feature = "docgen")]
fn resolve_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Resolve plugin repository").description(
        "Fetches a Git repository URL and previews it before subscribing: name, description, resolved ref, commit, and the plugins it provides, as a subscribed repository lists them, with the release an install would choose, the capability scopes each requests, and a `status` against the plugins directory (`available`, `up_to_date`, `update_available`, `incompatible` with `requires`, `unknown` with `reason`, `local`). Nothing is installed.",
    )
}

#[cfg(feature = "docgen")]
fn install_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Install plugins from URL").description(
        "Resolves a Git repository URL, installs the selected plugins at the release its `versions.json` offers this server, and reloads the plugin runtime. Each plugin carries the `commit` its preview showed; one whose offered release has changed since, or that is no longer offered, fails on its own; the request is rejected when the repository offers none of them. A plugin that is installed already is `held`, with the same `breaks_dependents`, `unmet_dependencies` and `dependency_cycle` reasons as updates, when its reinstall would leave a plugin unable to load, judged again after downloading against the plugins that downloaded. The plugins are written to disk together: when one cannot be, none is. An empty `plugins` list is rejected. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn install_repository_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Install plugins from subscribed repository").description(
        "Resolves a subscribed repository's origin and ref, installs the selected plugins at the release its `versions.json` offers this server, and reloads the plugin runtime. Each plugin carries the `commit` its preview showed; one whose offered release has changed since, or that is no longer offered, fails on its own; the request is rejected when the repository offers none of them. A plugin that is installed already is `held`, with the same `breaks_dependents`, `unmet_dependencies` and `dependency_cycle` reasons as updates, when its reinstall would leave a plugin unable to load, judged again after downloading against the plugins that downloaded. The plugins are written to disk together: when one cannot be, none is. An empty `plugins` list is rejected. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn update_installed_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update installed plugins").description(
        "Re-lists the recorded origin and ref of the selected plugins (or every repository-managed plugin when `plugins` is omitted) and reinstalls those it offers a newer release of that runs on this server. Each selected plugin names the release `commit` its status offered; one offered at another commit now fails on its own. A newer release that would let the plugin, or an installed plugin depending on it, reach scopes or unknown dependencies it could not before is `held` (`adds_scopes`, with those `gains`) unless the request names it, which grants them; one outside a dependent's requirement is always `held` (`breaks_dependents`), as is one requiring a plugin that would be missing or at another version (`unmet_dependencies`) or one that would leave plugins depending on each other in a cycle (`dependency_cycle`). Holds are judged against the plugins as they would be after the update, and again after downloading, against the releases that downloaded: one needing a release that failed is `held` then. The updates are written to disk together: when one cannot be, none is. Pinned plugins are reported `up_to_date` without a forge call. Plugins no release of their repository runs on this server are reported `incompatible`, with the `requires` that excludes it, and nothing is downloaded for them. Each origin is listed once, only newer releases are downloaded, and the plugin runtime reloads once. Returns per-plugin results.",
    )
}

#[cfg(feature = "docgen")]
fn reload_plugins_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Reload plugins").description(
        "Reloads the plugin runtime from the plugins directory. Use it after a CLI install or when a previous reload failed.",
    )
}

#[cfg(feature = "docgen")]
fn uninstall_installed_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Uninstall plugin")
        .description(
            "Removes a repository-managed plugin from disk and reloads the plugin runtime. Local plugins without a source record are refused.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn list_plugin_repositories_docs(op: TransformOperation) -> TransformOperation {
    op.summary("List plugin repositories").description(
        "Returns the subscribed plugin repositories as the server stored them at their last refresh, update check, or daily check, without contacting the forge. `resolved_ref`, `commit` and `refreshed_at` reflect the last refresh and are absent until one has happened. With `inc=plugins`, each carries the `plugins` it offered then, in the shape a preview lists them, with each `status` judged against the plugins installed now.",
    )
}

#[cfg(feature = "docgen")]
fn get_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get plugin repository").description(
        "Returns one subscribed plugin repository as the server stored it, without contacting the forge, in the shape the list returns. With `inc=plugins`, it carries the `plugins` it offered at its last refresh, with each `status` judged against the plugins installed now.",
    )
}

#[cfg(feature = "docgen")]
fn add_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Add plugin repository").description(
        "Resolves a Git repository URL, remembers it as a subscription with what it offers, and returns it as the repository list does, with its `plugins`. A repository already subscribed, at any ref and with owner and name compared case-insensitively, is rejected with `409`.",
    )
}

#[cfg(feature = "docgen")]
fn refresh_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh plugin repository").description(
        "Re-resolves a subscribed repository, stores what it offers, and returns it as the repository list does, with its `plugins`; each plugin's `status` reports whether it is installed and whether an update is available.",
    )
}

#[cfg(feature = "docgen")]
fn refresh_plugin_repositories_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Refresh plugin repositories").description(
        "Re-resolves every subscribed repository, one at a time as the daily check does, and stores what each offers; nothing is installed. Once a repository's host rate-limits a request, its other repositories are reported `failed` with the same error without being contacted. Returns every subscribed repository as the repository list does, with its `plugins`, and the `id` and `error` of each that failed, which keeps what it stored before.",
    )
}

#[cfg(feature = "docgen")]
fn delete_plugin_repository_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Remove plugin repository")
        .description(
            "Forgets a subscribed repository. Plugins installed from it stay on disk and remain individually updatable.",
        )
        .response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn restart_plugin_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Restart plugin").description(
        "Tears down the plugin's current runtime registrations, re-runs its entrypoint from the files on disk, and activates its routes. Manifest changes require a full reload. Other plugins that already required this plugin's modules keep the old exports until they restart.",
    ).response::<204, ()>()
}

#[cfg(feature = "docgen")]
fn update_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update plugin settings")
        .description("Updates setting values for a plugin and returns the updated settings groups.")
}

#[cfg(feature = "docgen")]
fn delete_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete plugin settings")
        .description("Deletes all stored settings for a plugin. Use this to clear stale plugin settings after a schema change.")
}

#[cfg(feature = "docgen")]
fn get_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Get user plugin settings").description(
        "Returns the user-scoped settings schema and the authenticated user's current values.",
    )
}

#[cfg(feature = "docgen")]
fn update_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Update user plugin settings")
        .description("Updates user-scoped setting values for the authenticated user.")
}

#[cfg(feature = "docgen")]
fn delete_user_settings_docs(op: TransformOperation) -> TransformOperation {
    op.summary("Delete user plugin settings")
        .description("Deletes all user-scoped settings for the authenticated user.")
}

/// The fixed segments `plugin_routes` serves beside `/{plugin_id}`. A plugin
/// with one of these ids would share a path with a fixed route, which takes
/// precedence, and could not be uninstalled, so Lyra refuses to load it.
pub(crate) const RESERVED_PLUGIN_IDS: &[&str] = &[
    "settings",
    "resolve",
    "install",
    "update",
    "reload",
    "repositories",
];

pub fn plugin_routes() -> Router {
    Router::new()
        .route("/", get(list_plugins))
        .route("/settings", get(list_all_settings))
        .route("/resolve", post(resolve_repository))
        .route("/install", post(install_plugins))
        .route("/update", post(update_installed_plugins))
        .route("/reload", post(reload_plugins))
        .route("/repositories", get(list_plugin_repositories))
        .route("/repositories", post(add_plugin_repository))
        .route("/repositories/refresh", post(refresh_plugin_repositories))
        .route(
            "/repositories/{repository_id}/refresh",
            post(refresh_plugin_repository),
        )
        .route(
            "/repositories/{repository_id}/install",
            post(install_repository_plugins),
        )
        .route("/repositories/{repository_id}", get(get_plugin_repository))
        .route(
            "/repositories/{repository_id}",
            delete(delete_plugin_repository),
        )
        .route("/{plugin_id}", delete(uninstall_installed_plugin))
        .route("/{plugin_id}/restart", post(restart_plugin))
        .route("/{plugin_id}/settings", get(get_settings))
        .route("/{plugin_id}/settings", patch(update_settings))
        .route("/{plugin_id}/settings", delete(delete_settings))
}

#[cfg(feature = "docgen")]
pub(crate) fn plugin_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
        post_with,
    };

    aide::axum::ApiRouter::new()
        .api_route("/", get_with(list_plugins, list_plugins_docs))
        .api_route(
            "/settings",
            get_with(list_all_settings, list_all_settings_docs),
        )
        .api_route(
            "/resolve",
            post_with(resolve_repository, resolve_repository_docs),
        )
        .api_route("/install", post_with(install_plugins, install_plugins_docs))
        .api_route(
            "/update",
            post_with(update_installed_plugins, update_installed_plugins_docs),
        )
        .api_route("/reload", post_with(reload_plugins, reload_plugins_docs))
        .api_route(
            "/repositories",
            get_with(list_plugin_repositories, list_plugin_repositories_docs),
        )
        .api_route(
            "/repositories",
            post_with(add_plugin_repository, add_plugin_repository_docs),
        )
        .api_route(
            "/repositories/refresh",
            post_with(
                refresh_plugin_repositories,
                refresh_plugin_repositories_docs,
            ),
        )
        .api_route(
            "/repositories/{repository_id}/refresh",
            post_with(refresh_plugin_repository, refresh_plugin_repository_docs),
        )
        .api_route(
            "/repositories/{repository_id}/install",
            post_with(install_repository_plugins, install_repository_plugins_docs),
        )
        .api_route(
            "/repositories/{repository_id}",
            get_with(get_plugin_repository, get_plugin_repository_docs),
        )
        .api_route(
            "/repositories/{repository_id}",
            delete_with(delete_plugin_repository, delete_plugin_repository_docs),
        )
        .api_route(
            "/{plugin_id}",
            delete_with(uninstall_installed_plugin, uninstall_installed_plugin_docs),
        )
        .api_route(
            "/{plugin_id}/restart",
            post_with(restart_plugin, restart_plugin_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            get_with(get_settings, get_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            patch_with(update_settings, update_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            delete_with(delete_settings, delete_settings_docs),
        )
}

pub(super) fn me_plugin_settings_routes() -> Router {
    Router::new()
        .route("/settings", get(list_all_user_settings))
        .route("/{plugin_id}/settings", get(get_user_settings))
        .route("/{plugin_id}/settings", patch(update_user_settings))
        .route("/{plugin_id}/settings", delete(delete_user_settings))
}

#[cfg(feature = "docgen")]
pub(super) fn me_plugin_settings_openapi_routes() -> aide::axum::ApiRouter {
    use aide::axum::routing::{
        delete_with,
        get_with,
        patch_with,
    };

    aide::axum::ApiRouter::new()
        .api_route(
            "/settings",
            get_with(list_all_user_settings, list_all_user_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            get_with(get_user_settings, get_user_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            patch_with(update_user_settings, update_user_settings_docs),
        )
        .api_route(
            "/{plugin_id}/settings",
            delete_with(delete_user_settings, delete_user_settings_docs),
        )
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::LazyLock,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    use super::*;
    use crate::{
        plugins::testing::init_library_test_state,
        services::testing::runtime_test_lock,
    };
    use axum::{
        http::{
            HeaderMap,
            StatusCode,
        },
        response::IntoResponse,
    };
    use lyra_db::nanoid;
    use lyra_db::{
        Permission,
        roles::Role,
        users::User,
    };
    use tokio::sync::Mutex;

    static REGISTRY_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn unavailable_repositories_answer_503_with_the_forge_wait() {
        let unavailable = |retry_after| {
            map_repository_error(repositories_service::PluginRepoError::Unavailable {
                message: "rate limited".to_string(),
                retry_after,
            })
            .into_response()
        };

        let response = unavailable(Some(std::time::Duration::from_secs(42)));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "42");

        let response = unavailable(None);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get("retry-after").is_none());
    }

    fn props(required: bool) -> FieldProps {
        FieldProps {
            label: "Label".to_string(),
            description: None,
            required,
            default_value: None,
        }
    }

    fn group(id: &str, fields: Vec<FieldDefinition>) -> FieldGroupDefinition {
        FieldGroupDefinition {
            id: id.to_string(),
            label: format!("{id} label"),
            fields,
        }
    }

    fn manifest(id: &str) -> harmony_core::plugin::PluginManifest {
        harmony_core::plugin::PluginManifest {
            schema_version: 1,
            id: id.to_string(),
            name: id.to_string(),
            version: semver::Version::new(1, 0, 0),
            host_requirement: semver::VersionReq::STAR,
            description: String::new(),
            entrypoint: None,
            scopes: Vec::new(),
            scope_reasons: BTreeMap::new(),
            scope_grants: BTreeMap::new(),
            dependencies: Default::default(),
        }
    }

    /// The responses for the loaded `manifests`, in their order.
    fn plugin_responses(
        manifests: &[harmony_core::plugin::PluginManifest],
        plugins_dir: &std::path::Path,
        subscriptions: &[repositories_service::Subscription],
    ) -> Vec<PluginResponse> {
        let mut responses: Vec<_> =
            repositories_service::installations(manifests, &[], plugins_dir, subscriptions)
                .expect("installations")
                .into_iter()
                .map(plugin_response)
                .collect();
        responses.sort_by_key(|response| {
            manifests
                .iter()
                .position(|manifest| manifest.id == response.id)
        });
        responses
    }

    #[test]
    fn plugin_responses_report_plugin_sources() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        std::fs::create_dir(plugins_dir.path().join("managed"))?;
        std::fs::create_dir(plugins_dir.path().join("broken"))?;
        std::fs::write(
            plugins_dir
                .path()
                .join("broken")
                .join(harmony_repository::manifest::SOURCE_RECORD_FILENAME),
            r#"{"schema_version": 1}"#,
        )?;
        SourceRecord {
            schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
            origin: "https://github.com/o/r".into(),
            forge: harmony_repository::Forge::GitHub,
            git_ref: Some("v2".into()),
            version: semver::Version::new(1, 0, 0),
            commit: "a".repeat(40),
            pinned: true,
            subpath: None,
            via_repository: None,
            installed_at: None,
            installed_automatically: false,
        }
        .store(&plugins_dir.path().join("managed"))?;

        let responses = plugin_responses(
            &[manifest("local"), manifest("managed"), manifest("broken")],
            plugins_dir.path(),
            &[],
        );
        let json = serde_json::to_value(&responses)?;

        assert_eq!(json[0]["source"], serde_json::json!({ "kind": "local" }));
        assert_eq!(json[1]["source"]["kind"], "repository");
        assert_eq!(json[1]["source"]["origin"], "https://github.com/o/r");
        assert_eq!(json[1]["source"]["ref"], "v2");
        assert_eq!(json[1]["source"]["version"], "1.0.0");
        assert_eq!(json[1]["source"]["commit"], "a".repeat(40));
        assert_eq!(json[1]["source"]["pinned"], true);
        assert_eq!(json[1]["source"]["status"], "up_to_date");
        assert!(json[1]["source"].get("requires").is_none());
        assert!(json[1]["source"].get("forge").is_none());
        assert!(json[1].get("schema_version").is_none());
        assert_eq!(json[2]["source"]["kind"], "invalid");
        let error = json[2]["source"]["error"].as_str().unwrap_or_default();
        assert!(error.contains("schema_version 1"), "{error}");
        Ok(())
    }

    #[test]
    fn plugins_that_did_not_load_are_listed_with_the_reason_and_their_update() -> anyhow::Result<()>
    {
        use repositories_service::installed_tests::{
            ORIGIN,
            discover,
            offering,
            write_manifest,
        };
        let plugins_dir = tempfile::TempDir::new()?;
        for id in ["loaded", "refused", "unreadable"] {
            let dir = plugins_dir.path().join(id);
            crate::plugins::testing::write_plugin(&dir, id, &[], "")?;
            SourceRecord {
                schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
                origin: ORIGIN.into(),
                forge: harmony_repository::Forge::GitHub,
                git_ref: None,
                version: semver::Version::new(1, 0, 0),
                commit: "a".repeat(40),
                pinned: false,
                subpath: Some(format!("plugins/{id}")),
                via_repository: None,
                installed_at: None,
                installed_automatically: false,
            }
            .store(&dir)?;
        }
        let refused = std::fs::read_to_string(plugins_dir.path().join("refused/plugin.json"))?
            .replace(crate::plugins::testing::HOST_REQUIREMENT, "=0.0.0");
        write_manifest(plugins_dir.path(), "refused", &refused);
        write_manifest(plugins_dir.path(), "unreadable", "{");
        std::fs::create_dir(plugins_dir.path().join("handmade"))?;
        let (manifests, errors) = discover(plugins_dir.path())?;

        let responses: Vec<_> = repositories_service::installations(
            &manifests,
            &errors,
            plugins_dir.path(),
            &[offering(&["loaded", "refused", "unreadable"])],
        )?
        .into_iter()
        .map(plugin_response)
        .collect();
        let json = serde_json::to_value(&responses)?;

        let [handmade, loaded, refused, unreadable] = &json.as_array().expect("an array")[..]
        else {
            panic!("{json}");
        };
        assert_eq!(loaded["load"], serde_json::json!({ "state": "loaded" }));
        assert_eq!(
            refused["load"],
            serde_json::json!({
                "state": "not_loaded",
                "reason": format!(
                    "plugin 'refused' requires lyra =0.0.0, but this is lyra {}",
                    env!("CARGO_PKG_VERSION")
                ),
            })
        );
        for plugin in [loaded, refused, unreadable] {
            assert_eq!(plugin["version"], "1.0.0", "{plugin}");
            assert_eq!(plugin["source"]["status"], "update_available", "{plugin}");
            assert_eq!(plugin["source"]["offered"]["version"], "2.0.0", "{plugin}");
        }
        assert_eq!(unreadable["name"], "unreadable");
        assert_eq!(unreadable["load"]["state"], "not_loaded");
        assert_eq!(handmade["id"], "handmade");
        assert_eq!(handmade["load"]["state"], "not_loaded");
        assert_eq!(handmade["source"], serde_json::json!({ "kind": "local" }));
        assert!(handmade.get("version").is_none(), "{handmade}");
        Ok(())
    }

    #[test]
    fn plugin_responses_mark_official_installs_from_their_records_alone() -> anyhow::Result<()> {
        const OFFICIAL: &str = "https://git.lyra.pub/lyra/lyra?forge=gitlab";
        const INDEX: &str = "https://github.com/o/index";
        let plugins_dir = tempfile::TempDir::new()?;
        let installs = [
            ("official", OFFICIAL, None, false, None),
            ("branch", OFFICIAL, Some("main"), false, None),
            ("pinned", OFFICIAL, Some("v1"), true, None),
            ("via", OFFICIAL, None, false, Some(INDEX)),
            (
                "remote",
                "https://github.com/o/r",
                None,
                false,
                Some(OFFICIAL),
            ),
            ("third_party", "https://github.com/o/r", None, false, None),
        ];
        for (id, origin, git_ref, pinned, via_repository) in installs {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir(&dir)?;
            SourceRecord {
                schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
                origin: origin.into(),
                forge: harmony_repository::Forge::GitLab,
                git_ref: git_ref.map(Into::into),
                version: semver::Version::new(1, 0, 0),
                commit: "a".repeat(40),
                pinned,
                subpath: Some(format!("plugins/{id}")),
                via_repository: via_repository.map(Into::into),
                installed_at: None,
                installed_automatically: false,
            }
            .store(&dir)?;
        }
        std::fs::create_dir(plugins_dir.path().join("local"))?;
        let manifests: Vec<_> = installs
            .iter()
            .map(|(id, ..)| manifest(id))
            .chain([manifest("local")])
            .collect();
        let subscribed = [repositories_service::Subscription {
            repository: lyra_db::plugin_repositories::PluginRepository {
                db_id: None,
                id: "default".into(),
                origin: OFFICIAL.into(),
                name: "Official".into(),
                description: String::new(),
                git_ref: None,
                resolved_ref: None,
                commit: None,
                refreshed_at_ms: None,
            },
            entries: Vec::new(),
        }];

        for subscriptions in [&subscribed[..], &[]] {
            let json = serde_json::to_value(plugin_responses(
                &manifests,
                plugins_dir.path(),
                subscriptions,
            ))?;
            let official: Vec<_> = json
                .as_array()
                .expect("an array of manifests")
                .iter()
                .map(|plugin| plugin["source"]["official"].clone())
                .collect();
            assert_eq!(
                official,
                [
                    serde_json::json!(true),
                    serde_json::json!(false),
                    serde_json::json!(false),
                    serde_json::json!(false),
                    serde_json::json!(false),
                    serde_json::json!(false),
                    serde_json::Value::Null,
                ]
            );
        }
        Ok(())
    }

    #[test]
    fn plugin_responses_give_the_reason_a_status_is_unknown() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        let origin = "https://github.com/o/r";
        for id in ["unsubscribed", "unreleased"] {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir(&dir)?;
            SourceRecord {
                schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
                origin: origin.into(),
                forge: harmony_repository::Forge::GitHub,
                git_ref: (id == "unsubscribed").then(|| "v2".into()),
                version: semver::Version::new(1, 0, 0),
                commit: "a".repeat(40),
                pinned: false,
                subpath: None,
                via_repository: None,
                installed_at: None,
                installed_automatically: false,
            }
            .store(&dir)?;
        }
        let subscriptions = [repositories_service::Subscription {
            repository: lyra_db::plugin_repositories::PluginRepository {
                db_id: None,
                id: "catalog".into(),
                origin: origin.into(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                resolved_ref: None,
                commit: None,
                refreshed_at_ms: Some(1),
            },
            entries: vec![lyra_db::plugin_repositories::PluginRepositoryEntry {
                db_id: None,
                plugin_id: "unreleased".into(),
                subpath: None,
                source_origin: None,
                name: None,
                description: None,
                version: None,
                commit: None,
                scopes: None,
                scope_reasons: None,
                dependencies: None,
                position: None,
                requires: None,
                error: Some("no releases: the repository has no versions.json".into()),
            }],
        }];

        let responses = plugin_responses(
            &[manifest("unsubscribed"), manifest("unreleased")],
            plugins_dir.path(),
            &subscriptions,
        );
        let json = serde_json::to_value(&responses)?;

        assert_eq!(json[0]["source"]["status"], "unknown");
        assert_eq!(
            json[0]["source"]["reason"],
            "no subscribed repository has its origin and ref"
        );
        assert_eq!(json[1]["source"]["status"], "unknown");
        assert_eq!(
            json[1]["source"]["reason"],
            "no releases: the repository has no versions.json"
        );
        Ok(())
    }

    #[test]
    fn plugin_responses_report_offered_releases_and_holds() -> anyhow::Result<()> {
        let plugins_dir = tempfile::TempDir::new()?;
        let write_manifest = |id: &str, dependencies: serde_json::Value| {
            let dir = plugins_dir.path().join(id);
            std::fs::create_dir(&dir)?;
            let manifest = serde_json::json!({
                "schema_version": 1,
                "id": id,
                "name": id,
                "version": "1.0.0",
                "lyra": "*",
                "description": "",
                "scopes": [],
                "dependencies": dependencies,
            });
            std::fs::write(
                dir.join(harmony_core::plugin::PLUGIN_CONFIG_FILENAME),
                manifest.to_string(),
            )?;
            anyhow::Ok(dir)
        };
        let origin = "https://github.com/o/r";
        for (id, installed_automatically) in [("held", true), ("scoped", false)] {
            let dir = write_manifest(id, serde_json::json!({}))?;
            SourceRecord {
                schema_version: harmony_repository::manifest::SOURCE_RECORD_SCHEMA_VERSION,
                origin: origin.into(),
                forge: harmony_repository::Forge::GitHub,
                git_ref: None,
                version: semver::Version::new(1, 0, 0),
                commit: "a".repeat(40),
                pinned: false,
                subpath: Some(format!("plugins/{id}")),
                via_repository: None,
                installed_at: Some("2026-10-28T00:00:00Z".into()),
                installed_automatically,
            }
            .store(&dir)?;
        }
        write_manifest("dependent", serde_json::json!({ "held": "1" }))?;
        let entry =
            |id: &str, scopes: &[&str]| lyra_db::plugin_repositories::PluginRepositoryEntry {
                db_id: None,
                plugin_id: id.into(),
                subpath: Some(format!("plugins/{id}")),
                source_origin: None,
                name: None,
                description: None,
                version: Some("2.0.0".into()),
                commit: Some("b".repeat(40)),
                scopes: Some(scopes.iter().map(ToString::to_string).collect()),
                scope_reasons: Some(
                    scopes
                        .iter()
                        .map(|scope| lyra_db::plugin_repositories::PluginScopeReason {
                            scope: scope.to_string(),
                            reason: "The author's reason.".into(),
                        })
                        .collect(),
                ),
                dependencies: None,
                position: None,
                requires: None,
                error: None,
            };
        let subscriptions = [repositories_service::Subscription {
            repository: lyra_db::plugin_repositories::PluginRepository {
                db_id: None,
                id: "catalog".into(),
                origin: origin.into(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                resolved_ref: None,
                commit: None,
                refreshed_at_ms: None,
            },
            entries: vec![entry("held", &[]), entry("scoped", &["lyra.metadata"])],
        }];

        let responses = plugin_responses(
            &[manifest("dependent"), manifest("held"), manifest("scoped")],
            plugins_dir.path(),
            &subscriptions,
        );
        let json = serde_json::to_value(&responses)?;

        let held = &json[1]["source"];
        assert_eq!(held["status"], "update_available");
        assert_eq!(
            held["offered"],
            serde_json::json!({ "version": "2.0.0", "commit": "b".repeat(40), "scopes": [] })
        );
        assert_eq!(
            held["hold"],
            serde_json::json!({
                "reason": "breaks_dependents",
                "dependents": [{ "id": "dependent", "requires": "^1" }],
            })
        );
        assert_eq!(held["installed_automatically"], true);
        let scoped = &json[2]["source"];
        assert_eq!(
            scoped["hold"],
            serde_json::json!({
                "reason": "adds_scopes",
                "gains": [{
                    "plugin": "scoped",
                    "risk": { "level": "standard", "reasons": [] },
                    "previous_risk": "standard",
                    "effects": [{
                        "kind": "writes_library",
                        "data": "metadata",
                        "via": [{
                            "plugin": "scoped",
                            "scope": "lyra.metadata",
                            "author_reason": "The author's reason.",
                        }],
                        "description": "Changes the library's metadata",
                    }],
                    "scopes": ["lyra.metadata"],
                    "unresolved_dependencies": [],
                }],
            })
        );
        assert_eq!(scoped["installed_automatically"], false);
        Ok(())
    }

    #[test]
    fn effect_sources_omit_an_author_reason_they_lack() -> anyhow::Result<()> {
        use crate::plugins::scopes::{
            Effect,
            EffectSource,
            PluginEffect,
            Scope,
        };

        let source = |plugin: &str, author_reason: Option<&str>| EffectSource {
            plugin: plugin.into(),
            scope: Scope::HarmonyHttp,
            author_reason: author_reason.map(Into::into),
        };
        let json = serde_json::to_value(EffectResponse::from(PluginEffect {
            effect: Effect::Egress,
            via: [source("a", Some("Fetches things.")), source("b", None)].into(),
        }))?;

        assert_eq!(
            json["via"],
            serde_json::json!([
                { "plugin": "a", "scope": "harmony.http", "author_reason": "Fetches things." },
                { "plugin": "b", "scope": "harmony.http" },
            ])
        );
        Ok(())
    }

    #[test]
    fn repository_responses_flatten_statuses_and_carry_requested_plugins() -> anyhow::Result<()> {
        use crate::plugins::repositories::{
            CatalogStatus,
            PluginPreview,
            RepositoryPreview,
            UpdateStatus,
        };

        let preview = RepositoryPreview {
            origin: "https://github.com/o/r".into(),
            git_ref: Some("main".into()),
            resolved_ref: "main".into(),
            commit: Some("a".repeat(40)),
            name: "Catalog".into(),
            description: "Plugins".into(),
            plugins: vec![
                PluginPreview {
                    id: "here".into(),
                    name: "Here".into(),
                    version: "1.0.0".into(),
                    description: String::new(),
                    scopes: vec!["metadata".into()],
                    access: None,
                    commit: Some("a".repeat(40)),
                    status: CatalogStatus::Installed(UpdateStatus::UpdateAvailable),
                    source_origin: None,
                    official: true,
                    error: None,
                },
                PluginPreview {
                    id: "there".into(),
                    name: "There".into(),
                    version: "2.0.0".into(),
                    description: String::new(),
                    scopes: Vec::new(),
                    access: None,
                    commit: None,
                    status: CatalogStatus::Available,
                    source_origin: Some("https://github.com/o/other".into()),
                    official: false,
                    error: Some("requires another lyra".into()),
                },
                PluginPreview {
                    id: "blocked".into(),
                    name: String::new(),
                    version: String::new(),
                    description: String::new(),
                    scopes: Vec::new(),
                    access: None,
                    commit: None,
                    status: CatalogStatus::Installed(UpdateStatus::Incompatible {
                        requires: "^0.0.7".into(),
                    }),
                    source_origin: None,
                    official: false,
                    error: Some("requires lyra ^0.0.7".into()),
                },
                PluginPreview {
                    id: "unreleased".into(),
                    name: String::new(),
                    version: String::new(),
                    description: String::new(),
                    scopes: Vec::new(),
                    access: None,
                    commit: None,
                    status: CatalogStatus::Installed(UpdateStatus::Unknown {
                        reason: "no releases".into(),
                    }),
                    source_origin: None,
                    official: false,
                    error: Some("no releases".into()),
                },
            ],
        };
        let record = lyra_db::plugin_repositories::PluginRepository {
            db_id: None,
            id: "repo".into(),
            origin: "https://github.com/o/r".into(),
            name: "Stale".into(),
            description: String::new(),
            git_ref: Some("main".into()),
            resolved_ref: Some("main".into()),
            commit: None,
            refreshed_at_ms: Some(1_000),
        };

        let json = serde_json::to_value(repository_preview_response(preview.clone()))?;
        assert!(json.get("id").is_none());
        assert!(json.get("refreshed_at").is_none());
        assert_eq!(json["name"], "Catalog");
        assert_eq!(json["ref"], "main");
        assert_eq!(json["resolved_ref"], "main");
        assert_eq!(json["plugins"][0]["status"], "update_available");
        assert!(json["plugins"][0].get("requires").is_none());
        assert_eq!(json["plugins"][2]["status"], "incompatible");
        assert_eq!(json["plugins"][2]["requires"], "^0.0.7");
        assert!(json["plugins"][0].get("source").is_none());
        assert!(json["plugins"][0].get("error").is_none());
        assert_eq!(json["plugins"][0]["official"], true);
        assert_eq!(json["plugins"][1]["official"], false);
        assert!(json.get("official").is_none());
        assert_eq!(json["plugins"][1]["error"], "requires another lyra");
        assert_eq!(json["plugins"][1]["status"], "available");
        assert_eq!(
            json["plugins"][1]["source"]["origin"],
            "https://github.com/o/other"
        );
        assert_eq!(json["plugins"][3]["status"], "unknown");
        assert_eq!(json["plugins"][3]["reason"], "no releases");

        let view = |plugins| repositories_service::RepositoryView {
            repository: record.clone(),
            plugins,
        };
        let json = serde_json::to_value(repository_response(view(Some(preview.plugins))))?;
        assert_eq!(json["id"], "repo");
        assert_eq!(json["refreshed_at"], "1970-01-01T00:00:01Z");
        assert_eq!(json["name"], "Stale");
        assert_eq!(json["resolved_ref"], "main");
        assert_eq!(json["plugins"][2]["requires"], "^0.0.7");
        assert_eq!(json["plugins"][0]["official"], true);
        assert!(json.get("official").is_none());
        let json = serde_json::to_value(repository_response(view(None)))?;
        assert!(json.get("plugins").is_none());
        Ok(())
    }

    #[test]
    fn repository_response_omits_refresh_time_until_refreshed() -> anyhow::Result<()> {
        let json =
            serde_json::to_value(repository_response(repositories_service::RepositoryView {
                repository: lyra_db::plugin_repositories::PluginRepository {
                    db_id: None,
                    id: "repo".into(),
                    origin: "https://github.com/o/r".into(),
                    name: "Catalog".into(),
                    description: String::new(),
                    git_ref: None,
                    resolved_ref: None,
                    commit: Some("a".repeat(40)),
                    refreshed_at_ms: None,
                },
                plugins: None,
            }))?;
        assert_eq!(json["commit"], "a".repeat(40));
        assert!(json.get("refreshed_at").is_none());
        assert!(json.get("resolved_ref").is_none());
        assert!(json.get("ref").is_none());
        Ok(())
    }

    async fn initialize_auth_test_runtime() -> anyhow::Result<PathBuf> {
        let test_dir = std::env::temp_dir().join(format!(
            "lyra-plugin-routes-test-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&test_dir)?;
        init_library_test_state(test_dir.clone()).await?;
        Ok(test_dir)
    }

    async fn manage_plugins_headers() -> anyhow::Result<HeaderMap> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user = User {
                db_id: None,
                id: nanoid(),
                username: format!("plugin-route-test-{}", nanoid()),
                password: "unused".to_string(),
            };
            let user_db_id = lyra_db::users::create(&mut db, &user)?;
            let role_name = format!("plugin-route-test-{}", nanoid());
            lyra_db::roles::create(
                &mut db,
                &Role {
                    db_id: None,
                    id: nanoid(),
                    name: role_name.clone(),
                    permissions: vec![Permission::ManagePlugins],
                },
            )?;
            lyra_db::roles::ensure_user_has_role(&mut db, user_db_id, &role_name)?;
            user_db_id
        };

        let session =
            crate::services::testing::create_session(user_db_id, Default::default()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", session.token)
                .parse()
                .expect("valid auth header"),
        );
        Ok(headers)
    }

    #[test]
    fn field_to_response_rejects_invalid_stored_value_types() {
        let field = FieldDefinition::Number {
            key: "volume".to_string(),
            props: props(false),
            min: Some(0.0),
            max: Some(10.0),
        };
        let stored = HashMap::from([(
            "volume".to_string(),
            serde_json::Value::String("loud".to_string()),
        )]);

        let error = field_to_response(&field, &stored)
            .err()
            .expect("invalid stored value should be rejected");
        assert!(error.to_string().contains("must be a number"));
    }

    #[test]
    fn field_to_response_keeps_missing_bool_as_null() -> anyhow::Result<()> {
        let field = FieldDefinition::Bool {
            key: "enabled".to_string(),
            props: props(false),
        };
        let stored = HashMap::new();

        let response = field_to_response(&field, &stored)?;
        match response {
            FieldResponse::Bool { value, .. } => assert!(value.is_none()),
            _ => panic!("expected bool field"),
        }

        Ok(())
    }

    #[test]
    fn group_to_response_preserves_group_metadata_and_field_order() -> anyhow::Result<()> {
        let response = group_to_response(
            &group(
                "credentials",
                vec![
                    FieldDefinition::String {
                        key: "token".to_string(),
                        props: props(true),
                    },
                    FieldDefinition::Bool {
                        key: "enabled".to_string(),
                        props: props(false),
                    },
                ],
            ),
            &HashMap::from([
                ("token".to_string(), serde_json::json!("abc")),
                ("enabled".to_string(), serde_json::json!(true)),
            ]),
        )?;

        assert_eq!(response.id, "credentials");
        assert_eq!(response.label, "credentials label");
        assert_eq!(response.fields.len(), 2);
        assert!(matches!(response.fields[0], FieldResponse::String { .. }));
        assert!(matches!(response.fields[1], FieldResponse::Bool { .. }));

        Ok(())
    }

    #[test]
    fn restart_error_maps_missing_plugin_to_not_found() -> anyhow::Result<()> {
        let plugin_id = PluginId::new("demo")?;
        let response =
            map_plugin_restart_error(PluginRestartError::NotFound(plugin_id)).into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test]
    fn restart_error_maps_failed_restart_to_conflict() -> anyhow::Result<()> {
        let plugin_id = PluginId::new("demo")?;
        let response = map_plugin_restart_error(PluginRestartError::Failed {
            plugin_id,
            source: anyhow::anyhow!("boom"),
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        Ok(())
    }

    #[tokio::test]
    async fn restart_plugin_rejects_invalid_plugin_id_after_auth() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let response = restart_plugin(headers, Path("bad id".to_string()))
            .await
            .expect_err("invalid plugin id should be rejected")
            .into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn update_installed_plugins_reports_invalid_ids_as_failed() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let Json(response) = update_installed_plugins(
            headers,
            Json(UpdatePluginsRequest {
                plugins: Some(vec![PreviewedPluginRequest {
                    id: "bad id".to_string(),
                    commit: "a".repeat(40),
                }]),
            }),
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;

        assert!(response.updated.is_empty());
        assert!(response.up_to_date.is_empty());
        assert_eq!(response.failed.len(), 1);
        assert_eq!(response.failed[0].id, "bad id");
        assert!(response.failed[0].error.contains("invalid plugin id"));
        Ok(())
    }

    #[tokio::test]
    async fn install_repository_plugins_rejects_unknown_repository() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let Err(error) = install_repository_plugins(
            headers,
            Path("missing".to_string()),
            Json(RepositoryInstallRequest {
                plugins: vec![PreviewedPluginRequest {
                    id: "demo".to_string(),
                    commit: "a".repeat(40),
                }],
            }),
        )
        .await
        else {
            panic!("unknown repository should be rejected");
        };
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_repositories_include_their_stored_plugins_on_request() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;
        lyra_db::plugin_repositories::create(
            &mut *STATE.db.write().await,
            &lyra_db::plugin_repositories::PluginRepository {
                db_id: None,
                id: "catalog".into(),
                origin: "https://github.com/o/r".into(),
                name: "Catalog".into(),
                description: String::new(),
                git_ref: None,
                resolved_ref: Some("main".into()),
                commit: Some("a".repeat(40)),
                refreshed_at_ms: Some(1_000),
            },
            &[lyra_db::plugin_repositories::PluginRepositoryEntry {
                db_id: None,
                plugin_id: "foo".into(),
                subpath: Some("plugins/foo".into()),
                source_origin: None,
                version: Some("1.0.0".into()),
                name: Some("Foo".into()),
                description: Some("Does foo".into()),
                commit: Some("b".repeat(40)),
                scopes: Some(vec!["lyra.metadata".into()]),
                scope_reasons: Some(vec![lyra_db::plugin_repositories::PluginScopeReason {
                    scope: "lyra.metadata".into(),
                    reason: "Adds details.".into(),
                }]),
                dependencies: None,
                position: None,
                requires: None,
                error: None,
            }],
        )?;
        let inc = |values: &[&str]| {
            Query(PluginRepositoryQuery {
                inc: Some(values.iter().map(ToString::to_string).collect()),
            })
        };
        let get = async |inc| {
            let response =
                get_plugin_repository(headers.clone(), Path("catalog".to_string()), inc).await;
            response.map(|Json(response)| serde_json::to_value(response).expect("serializes"))
        };

        let Ok(json) = get(Query(PluginRepositoryQuery { inc: None })).await else {
            panic!("the repository is found");
        };
        assert_eq!(json["resolved_ref"], "main");
        assert!(json.get("plugins").is_none(), "{json}");

        let Ok(json) = get(inc(&["plugins"])).await else {
            panic!("the repository is found");
        };
        let plugin = &json["plugins"][0];
        assert_eq!(
            (&plugin["id"], &plugin["name"], &plugin["description"]),
            (&"foo".into(), &"Foo".into(), &"Does foo".into())
        );
        assert_eq!(plugin["status"], "available");
        assert_eq!(
            plugin["access"]["risk"],
            serde_json::json!({ "level": "standard", "reasons": [] })
        );
        assert_eq!(
            plugin["access"]["effects"][0],
            serde_json::json!({
                "kind": "writes_library",
                "data": "metadata",
                "via": [{
                    "plugin": "foo",
                    "scope": "lyra.metadata",
                    "author_reason": "Adds details.",
                }],
                "description": "Changes the library's metadata",
            })
        );

        let Ok(Json(listed)) = list_plugin_repositories(headers.clone(), inc(&["plugins"])).await
        else {
            panic!("repositories are listed");
        };
        let listed = serde_json::to_value(listed)?;
        assert_eq!(listed[0]["plugins"], json["plugins"]);

        let Err(error) = get(inc(&["bogus"])).await else {
            panic!("unsupported inc values are rejected");
        };
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
        let Err(error) = get_plugin_repository(
            headers,
            Path("missing".to_string()),
            Query(PluginRepositoryQuery { inc: None }),
        )
        .await
        else {
            panic!("unknown repositories are not found");
        };
        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn reload_plugins_requires_manage_plugins() -> anyhow::Result<()> {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;

        let response = reload_plugins(HeaderMap::new())
            .await
            .expect_err("unauthenticated reload should be rejected")
            .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn restart_plugin_returns_service_unavailable_until_runtime_ready() -> anyhow::Result<()>
    {
        let _registry_guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        let _test_dir = initialize_auth_test_runtime().await?;
        let headers = manage_plugins_headers().await?;

        let response = restart_plugin(headers, Path("demo".to_string()))
            .await
            .expect_err("missing Harmony runtime should return 503")
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[tokio::test]
    async fn load_registered_schema_returns_service_unavailable_while_registry_populates() {
        let _guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().expect("init test state");

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should report initializing while registry is mutable")
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn load_registered_schema_returns_not_found_after_registry_freezes() {
        let _guard = REGISTRY_TEST_GUARD.lock().await;
        let _runtime_guard = runtime_test_lock().await;
        crate::plugins::testing::init_default_test_state().expect("init test state");
        plugin_settings_registry::freeze_registry().await;

        let response = load_registered_schema("demo", SettingsScope::Global)
            .await
            .expect_err("missing schema should be a 404 once startup completes")
            .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        plugin_settings_registry::initialize_registry().await;
    }

    fn empty_schema() -> Schema {
        Schema { groups: Vec::new() }
    }

    #[test]
    fn build_status_returns_initializing_for_unknown_plugin_while_registry_is_open() {
        let db = lyra_db::test_db::new_test_db().expect("test db");
        let registry = Registry::default();

        let status = build_status(&registry, &db, "demo", SettingsTarget::Server);

        assert!(matches!(status, PluginSettingsStatus::Initializing));
    }

    #[test]
    fn build_status_returns_not_declared_for_unknown_plugin_when_registry_is_frozen() {
        let db = lyra_db::test_db::new_test_db().expect("test db");
        let mut registry = Registry::default();
        registry.freeze();

        let status = build_status(&registry, &db, "demo", SettingsTarget::Server);

        assert!(matches!(status, PluginSettingsStatus::NotDeclared));
    }

    #[test]
    fn build_status_returns_ready_when_schema_is_registered() -> anyhow::Result<()> {
        let db = lyra_db::test_db::new_test_db()?;
        let mut registry = Registry::default();
        registry.register_schema(
            PluginId::new("demo")?,
            SettingsScope::Global,
            empty_schema(),
        )?;

        let status = build_status(&registry, &db, "demo", SettingsTarget::Server);

        match status {
            PluginSettingsStatus::Ready { groups } => assert!(groups.is_empty()),
            _ => panic!("expected Ready"),
        }
        Ok(())
    }

    /// The `plugin_id` the route `router` matches for `method` and `uri`
    /// reads, if it matches one that takes a plugin id at all.
    async fn routed_plugin_id(router: Router, method: &str, uri: &str) -> Option<String> {
        use axum::{
            body::{
                Body,
                to_bytes,
            },
            extract::{
                FromRequestParts,
                RawPathParams,
            },
            http::Request,
            middleware::{
                self,
                Next,
            },
            response::Response,
        };
        use tower::ServiceExt;

        // Answers in place of the handler, so no route needs server state.
        async fn echo_plugin_id(request: Request<Body>, _next: Next) -> Response {
            let (mut parts, _) = request.into_parts();
            let params = RawPathParams::from_request_parts(&mut parts, &())
                .await
                .expect("route layers run on matched routes");
            let plugin_id = params
                .iter()
                .find_map(|(name, value)| (name == "plugin_id").then(|| value.to_string()))
                .unwrap_or_default();
            Response::new(Body::from(plugin_id))
        }

        let response = router
            .route_layer(middleware::from_fn(echo_plugin_id))
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("routers are infallible");
        if response.status() != StatusCode::OK {
            return None;
        }
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let plugin_id = String::from_utf8(body.to_vec()).expect("utf-8 plugin id");
        (!plugin_id.is_empty()).then_some(plugin_id)
    }

    /// Whether every per-plugin route reads `id` as its plugin id.
    async fn plugin_id_is_addressable(id: &str) -> bool {
        let settings = [
            ("GET", "/settings"),
            ("PATCH", "/settings"),
            ("DELETE", "/settings"),
        ];
        let plugin_requests = [("DELETE", ""), ("POST", "/restart")]
            .into_iter()
            .chain(settings)
            .map(|request| (plugin_routes(), request));
        let me_requests = settings
            .into_iter()
            .map(|request| (me_plugin_settings_routes(), request));
        for (router, (method, suffix)) in plugin_requests.chain(me_requests) {
            let routed = routed_plugin_id(router, method, &format!("/{id}{suffix}")).await;
            if routed.as_deref() != Some(id) {
                return false;
            }
        }
        true
    }

    #[tokio::test]
    async fn reserved_plugin_ids_are_the_ones_fixed_routes_shadow() {
        for id in RESERVED_PLUGIN_IDS {
            assert!(
                !plugin_id_is_addressable(id).await,
                "'{id}' reaches every plugin route, so it need not be reserved"
            );
        }
        for id in ["demo", "restart", "plugins", "Update"] {
            assert!(
                plugin_id_is_addressable(id).await,
                "'{id}' is shadowed by a fixed route but not reserved"
            );
        }
    }
}
