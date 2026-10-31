// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeTypeAlias,
    FunctionParameter,
    TypeAliasDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::plugins::args;
use crate::{
    plugins::db::{
        Db,
        ViewerDb,
    },
    services::{
        auth::Principal,
        listens::{
            self as listen_service,
            ListenUpdatePayload,
            MAX_SUBMITTED_PLAYS,
            RecordOutcome,
            SubmissionOutcome,
            SubmittedPlay,
        },
        playback_sessions,
        providers::provider_registry,
    },
};
use lyra_db::ResolveId;
use serde::Deserialize;

pub(crate) struct ListensModule;
#[cfg(feature = "docgen")]
struct ListenUpdateHandler;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ListenUpdateHandler {
    fn luau_type() -> LuauType {
        LuauType::literal("ListenUpdateHandler")
    }
}

#[cfg(feature = "docgen")]
impl DescribeTypeAlias for ListenUpdateHandler {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ListenUpdateHandler",
            LuauType::function(
                vec![FunctionParameter {
                    name: Some("payload"),
                    ty: LuauType::literal("ListenUpdatePayload"),
                    variadic: false,
                }],
                vec![],
            ),
            Some("Listen update callback."),
        )
    }
}

#[derive(Deserialize)]
struct SubmittedPlayRequest {
    play_id: String,
    track_id: i64,
    started_at_ms: u64,
    played_ms: u64,
}

fn submitted_play(value: &luau::Value) -> luau::runtime::Result<SubmittedPlay> {
    let request: SubmittedPlayRequest = args::from_json(value)?;
    if !listen_service::is_play_id(&request.play_id) {
        return Err(crate::plugins::runtime_error(format!(
            "malformed play_id: {}",
            request.play_id
        )));
    }
    Ok(SubmittedPlay {
        play_id: request.play_id,
        track: ResolveId::DbId(args::positive_id(request.track_id, "track_id")?),
        started_at_ms: request.started_at_ms,
        played_ms: request.played_ms,
    })
}

/// A batch of plays, each judged for the cap and its shape before any is recorded.
struct SubmittedPlays(Vec<SubmittedPlay>);

impl<'vm> luau::FromLuau<'vm> for SubmittedPlays {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let plays: luau::Table = reader.read()?;
        let plays = args::array_values(&plays)?;
        if plays.len() > MAX_SUBMITTED_PLAYS {
            return Err(crate::plugins::runtime_error(format!(
                "plays cap exceeded: {} > {MAX_SUBMITTED_PLAYS}",
                plays.len()
            )));
        }
        plays
            .iter()
            .map(|(index, value)| {
                submitted_play(value).map_err(|error| {
                    let message = match error {
                        luau::Error::Runtime(message) => message,
                        other => other.to_string(),
                    };
                    crate::plugins::runtime_error(format!("plays[{index}]: {message}"))
                })
            })
            .collect::<luau::runtime::Result<Vec<_>>>()
            .map(Self)
    }
}

impl LuauTypeInfo for SubmittedPlays {
    fn luau_type() -> LuauType {
        LuauType::array(SubmittedPlayRequest::luau_type())
    }
}

/// The `options` argument of `get`.
struct ListenStatsOptions {
    merge_unique_external_ids: bool,
}

impl<'vm> luau::FromLuau<'vm> for ListenStatsOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        let merge_unique_external_ids = match options.get("merge_unique_external_ids")? {
            luau::Value::Nil => false,
            luau::Value::Boolean(value) => value,
            other => {
                return Err(crate::plugins::runtime_error(format!(
                    "merge_unique_external_ids must be a boolean, got {}",
                    other.type_name()
                )));
            }
        };
        Ok(Self {
            merge_unique_external_ids,
        })
    }
}

impl LuauTypeInfo for ListenStatsOptions {
    fn luau_type() -> LuauType {
        LuauType::named("ListenStatsOptions")
    }
}

/// [`listen_service::ListenStats`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenStats;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LuauListenStats {
    fn luau_type() -> LuauType {
        LuauType::named("ListenStats")
    }
}

#[harmony_macros::module(
    id = "lyra/listens",
    description = "Listen counts and submitted plays of the dispatch principal, and every user's listens as they are recorded: like `playback_sessions.on_update` for playbacks, `on_update` delivers all users' listens, not only the caller's, and needs the `lyra.listens.all` scope too.",
    docs_feature = "docgen"
)]
impl ListensModule {
    #[harmony(
        description = "The caller's listens to each of the tracks, releases and artists, keyed by id: a release or artist totals the tracks it holds or is credited on, as the catalogs count them to sort by. Targets without listens, of another kind, and ones the caller cannot see are absent.",
        returns(HashMap<u64, LuauListenStats>)
    )]
    async fn get(
        target_ids: args::UniqueIds,
        options: Option<ListenStatsOptions>,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let merge = options.is_some_and(|options| options.merge_unique_external_ids);
        let unique_track_id_pairs = if merge {
            Some(
                provider_registry()
                    .read_owned()
                    .await
                    .unique_track_id_pairs(),
            )
        } else {
            None
        };
        let (db, viewer) = db.read().await?;
        let stats =
            listen_service::stats(&db, &viewer, &target_ids.0, unique_track_id_pairs.as_ref())
                .map_err(crate::plugins::catalog::error)?;

        let mut table = luau::OwnedTable::with_entry_capacity(0, 0, stats.len());
        for (id, stats) in stats {
            let mut entry = luau::OwnedTable::with_capacity(0, 2);
            entry.set_field("count", stats.count as i64);
            if let Some(last_played_at_ms) = stats.last_played_at_ms {
                entry.set_field("last_played_at_ms", last_played_at_ms as i64);
            }
            table.set_key(id.0, entry);
        }
        Ok(table)
    }

    #[harmony(
        description = "Judges the dispatch principal's plays as listens in one transaction and returns each play's outcome, keyed by `play_id`. A `play_id` repeated in the batch is judged by its first occurrence. Recorded listens reach `on_update`. Raises on a malformed play or more than 500 plays, judging none.",
        returns(HashMap<String, LuauListenOutcome>)
    )]
    async fn submit(
        plays: SubmittedPlays,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let now_ms = playback_sessions::now_ms().map_err(crate::plugins::runtime_error)?;
        let mut db = db.write_outside_catalog().await;
        let user_db_id = crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let outcomes =
            listen_service::record_submitted(&mut db, &principal, user_db_id, plays.0, now_ms)?;
        drop(db);
        let outcomes = outcomes
            .into_iter()
            .map(|(play_id, outcome)| (play_id, outcome_name(&outcome)))
            .collect::<HashMap<_, _>>();
        harmony_luau::serializable_to_luau_owned(outcomes)
    }

    #[harmony(
        description = "Registers a callback for every user's listens. A `recorded` update arrives once for each commit that stored listens, right after it, carrying all of that commit's listens for one user: one for a live play, up to a whole submitted batch. Each listen is announced once. Delivery is best effort: updates are dropped while the plugin runtime is saturated. Needs the `lyra.listens.all` scope too.",
        args(handler: ListenUpdateHandler)
    )]
    fn on_update(
        vm: &luau::Vm,
        context: &luau::CallContext,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        crate::plugins::update_callbacks::register::<ListenUpdatePayload>(
            vm,
            context,
            handler,
            "listens.on_update",
        )
    }
}

fn outcome_name(outcome: &SubmissionOutcome) -> &'static str {
    match outcome {
        SubmissionOutcome::Judged(RecordOutcome::Recorded(_)) => "recorded",
        SubmissionOutcome::Judged(RecordOutcome::Duplicate) => "duplicate",
        SubmissionOutcome::Judged(RecordOutcome::NotListened) => "not_listened",
        SubmissionOutcome::Judged(RecordOutcome::InvalidTime) => "invalid_time",
        SubmissionOutcome::TrackNotFound => "track_not_found",
    }
}

/// [`ListenUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenUpdatePayload;

/// [`crate::services::listens::ListenPayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenPayload;

#[cfg(feature = "docgen")]
fn described_field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauListenUpdatePayload {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "ListenUpdatePayload",
            description: Some("One user's listens recorded together, such as a submitted batch."),
            fields: vec![
                described_field(
                    "event",
                    LuauType::literal("\"recorded\""),
                    "What happened to the listens.",
                ),
                described_field("user_id", String::luau_type(), "The listener's public id."),
                described_field(
                    "listens",
                    LuauType::array(LuauType::named("ListenPayload")),
                    "The listens, at least one.",
                ),
            ],
        }
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauListenPayload {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "ListenPayload",
            description: None,
            fields: vec![
                described_field("listen_id", String::luau_type(), "The listen's public id."),
                described_field("track_id", String::luau_type(), "The track's public id."),
                described_field(
                    "started_at_ms",
                    u64::luau_type(),
                    "When the play started, in Unix milliseconds.",
                ),
                described_field(
                    "played_ms",
                    u64::luau_type(),
                    "Time the track actually played when the listen was recorded.",
                ),
            ],
        }
    }
}

const _: fn(&ListenUpdatePayload) = |value| {
    let ListenUpdatePayload {
        event: _,
        user_id: _,
        listens,
    } = value;
    for crate::services::listens::ListenPayload {
        listen_id: _,
        track_id: _,
        started_at_ms: _,
        played_ms: _,
    } in listens
    {}
};

impl LuauTypeInfo for SubmittedPlayRequest {
    fn luau_type() -> LuauType {
        LuauType::literal("SubmittedPlay")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for SubmittedPlayRequest {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "SubmittedPlay",
            description: Some("A play the player recorded but couldn't report live."),
            fields: vec![
                described_field(
                    "play_id",
                    String::luau_type(),
                    listen_service::PLAY_ID_DESCRIPTION,
                ),
                described_field("track_id", i64::luau_type(), "The track's database id."),
                described_field(
                    "started_at_ms",
                    u64::luau_type(),
                    "When the play started, in Unix milliseconds.",
                ),
                described_field(
                    "played_ms",
                    u64::luau_type(),
                    "Time the track actually played, excluding pauses and skipped spans.",
                ),
            ],
        }
    }
}

#[cfg(feature = "docgen")]
fn listen_stats_options_interface() -> InterfaceDescriptor {
    InterfaceDescriptor {
        name: "ListenStatsOptions",
        description: None,
        fields: vec![described_field(
            "merge_unique_external_ids",
            Option::<bool>::luau_type(),
            "Also counts, for each track, the listens to every track the caller can see that shares an external id a provider declares unique to one recording.",
        )],
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LuauListenStats {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "ListenStats",
            description: Some("The caller's listens to one track, release or artist."),
            fields: vec![
                described_field("count", u64::luau_type(), "The listens, at least one."),
                described_field(
                    "last_played_at_ms",
                    Option::<u64>::luau_type(),
                    "When the latest listen started, in Unix milliseconds.",
                ),
            ],
        }
    }
}

/// [`SubmissionOutcome`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauListenOutcome;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for LuauListenOutcome {
    fn luau_type() -> LuauType {
        LuauType::literal("ListenOutcome")
    }
}

#[cfg(feature = "docgen")]
impl DescribeTypeAlias for LuauListenOutcome {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "ListenOutcome",
            LuauType::union(
                [
                    "recorded",
                    "duplicate",
                    "not_listened",
                    "track_not_found",
                    "invalid_time",
                ]
                .into_iter()
                .map(LuauType::string_literal)
                .collect(),
            ),
            Some("What the server made of one submitted play. Every outcome is final."),
        )
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &ListensModule::module_descriptor(),
        &[
            ListenUpdateHandler::type_alias_descriptor(),
            LuauListenOutcome::type_alias_descriptor(),
        ],
        &[
            SubmittedPlayRequest::interface_descriptor(),
            listen_stats_options_interface(),
            LuauListenStats::interface_descriptor(),
            LuauListenUpdatePayload::interface_descriptor(),
            LuauListenPayload::interface_descriptor(),
        ],
        &[],
    )
}
