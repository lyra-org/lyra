// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeTypeAlias,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
    StringEnum,
};
use lyra_db::Library;
use serde::Serialize;

use crate::{
    plugins::{
        args::{
            self,
            UniqueIds,
        },
        auth::DispatchPrincipal,
        db::Db,
        ids::Resolved,
        types::{
            LuauSyncReleases,
            LuauSyncRunKind,
            LuauSyncRunStatus,
            LuauSyncStage,
            LuauSyncTotalState,
        },
    },
    services::{
        CoverSyncOptions,
        auth::{
            Principal,
            access,
            owners::Owners,
            resolve_principal,
        },
        libraries::{
            LibraryRun,
            LibrarySyncOptions,
            RunFilter,
            SyncRunStartResponse,
            SyncRunSummary,
            control::{
                self,
                LibraryControlError,
            },
            system_context,
        },
    },
};

pub(crate) struct LibrariesModule;

/// Who reads the libraries: a user as the database has them now, who sees the libraries they can
/// see and their paths with `ManageLibraries`, or the system, which sees every library and its
/// path.
struct Reader(Option<Principal>);

impl Reader {
    fn of(db: &agdb::DbAny, caller: DispatchPrincipal) -> luau::runtime::Result<Self> {
        let current = caller
            .0
            .map(|principal| principal.current(db))
            .transpose()
            .map_err(crate::plugins::errors::auth_error)?;
        Ok(Self(current))
    }

    fn libraries(&self, db: &impl lyra_db::DbAccess) -> luau::runtime::Result<Vec<Library>> {
        let libraries = match &self.0 {
            Some(principal) => access::libraries(db, principal),
            None => access::system_libraries(db, &system_context()),
        };
        libraries.map_err(crate::plugins::runtime_error)
    }

    fn libraries_for_entity(
        &self,
        db: &impl lyra_db::DbAccess,
        entity_db_id: agdb::DbId,
    ) -> luau::runtime::Result<Vec<Library>> {
        let libraries = match &self.0 {
            Some(principal) => access::libraries_for_entity(db, principal, entity_db_id),
            None => access::system_libraries_for_entity(db, &system_context(), entity_db_id),
        };
        libraries.map_err(crate::plugins::runtime_error)
    }

    fn record(&self, library: Library) -> luau::runtime::Result<luau::Value> {
        let shows_path = crate::plugins::auth::manages_libraries(self.0.as_ref());
        harmony_luau::serializable_to_luau_owned(LibraryRecord {
            path: shows_path.then(|| library.path.to_string_lossy().into_owned()),
            id: library.id,
            name: library.name,
            language: library.language,
            country: library.country,
        })
    }
}

#[harmony_macros::module(
    id = "lyra/libraries",
    description = "The libraries the caller can see, and their syncs and metadata refreshes. A library runs one sync or refresh at a time. `sync`, `refresh` and `cancel_sync` need the `lyra.libraries.manage` scope too, and a calling user with the manage_libraries permission, as the database has them when the call runs. `by_user` needs the `lyra.libraries.all` scope too.",
    docs_feature = "docgen"
)]
impl LibrariesModule {
    #[harmony(
        description = "The libraries the caller can see, in the order they were created.",
        returns(Vec<LibraryRecord>)
    )]
    async fn list(
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let reader = Reader::of(&db, caller)?;
        let mut list = luau::OwnedTable::new();
        for library in reader.libraries(&*db)? {
            list.push_array(reader.record(library)?);
        }
        Ok(list)
    }

    #[harmony(
        description = "The libraries with these ids, keyed by id. Libraries that are missing or hidden from the caller are absent.",
        returns(HashMap<String, LibraryRecord>)
    )]
    async fn get(
        library_ids: UniqueIds,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let reader = Reader::of(&db, caller)?;
        let mut libraries = luau::OwnedTable::new();
        for library in reader.libraries(&*db)? {
            if library_ids.0.contains(&library.id) {
                let id = library.id.clone();
                libraries.set_field(id, reader.record(library)?);
            }
        }
        Ok(libraries)
    }

    #[harmony(
        description = "The libraries holding each of these entities that the caller can see, keyed by entity id, each list starting with the one the server picks for the entity. A library holds itself. Entities in no library the caller can see are absent.",
        returns(HashMap<String, Vec<LibraryRecord>>)
    )]
    async fn by_entity(
        entity_ids: UniqueIds,
        #[context] db: Db,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let reader = Reader::of(&db, caller)?;
        let entities = Resolved::of(&*db, entity_ids.0)?;
        let mut values = Vec::with_capacity(entities.db_ids().len());
        for &entity_db_id in entities.db_ids() {
            let libraries = reader.libraries_for_entity(&*db, entity_db_id)?;
            if libraries.is_empty() {
                continue;
            }
            let mut list = luau::OwnedTable::with_capacity(libraries.len(), 0);
            for library in libraries {
                list.push_array(reader.record(library)?);
            }
            values.push((entity_db_id, list));
        }
        Ok(entities.keyed(values))
    }

    #[harmony(
        description = "The libraries each of these users can see, keyed by user id, in the order they were created: every library for an admin, none for a user granted none. Users that don't exist are absent. The caller's own id needs nothing more; another user's requires the manage_users permission and every permission that user holds, and raises without them. Needs the `lyra.libraries.all` scope too.",
        returns(HashMap<String, Vec<LibraryRecord>>)
    )]
    async fn by_user(
        user_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let libraries = lyra_db::libraries::get(&*db)?;
        let reader = Reader(Some(
            principal
                .current(&db)
                .map_err(crate::plugins::errors::auth_error)?,
        ));
        let mut by_user = luau::OwnedTable::with_capacity(0, user_ids.0.len());
        for user_id in user_ids.0 {
            let (_, users) =
                crate::plugins::auth::owned_users(&db, &principal, Owners::User(&user_id))?;
            let Some(user) = users.into_iter().next() else {
                continue;
            };
            let Some(user_db_id) = user.db_id else {
                continue;
            };
            let user = resolve_principal(&db, user_db_id, user);
            let mut list = luau::OwnedTable::new();
            for library in &libraries {
                if user.accessible_library_ids.contains(&library.id) {
                    list.push_array(reader.record(library.clone())?);
                }
            }
            by_user.set_field(user_id, list);
        }
        Ok(by_user)
    }

    #[harmony(
        description = "Starts a sync of the library in the background: a scan of its files, then the metadata of the releases `options.releases` picks from every enabled provider, and the covers of those releases and of the artists credited on them or their tracks as `options.covers` asks. While a sync or refresh of the library is running, starts nothing and returns that run with `started` false, ignoring `options`. Returns nil when the library is missing or hidden from the caller.",
        returns(Option<SyncStartRecord>)
    )]
    async fn sync(
        library_id: String,
        options: Option<SyncOptions>,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let options = options.map(|options| options.0).unwrap_or_default();
        sync_start(control::start(&principal, &library_id, LibraryRun::Sync(options)).await)
    }

    #[harmony(
        description = "Starts a refresh of the metadata of every release in the library from every enabled provider and of the covers of those releases and of the artists credited on them or their tracks, as `options.covers` asks, in the background, as `sync` starts a sync.",
        returns(Option<SyncStartRecord>)
    )]
    async fn refresh(
        library_id: String,
        options: Option<RefreshOptions>,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        let covers = options.map(|options| options.0).unwrap_or_default();
        sync_start(control::start(&principal, &library_id, LibraryRun::Refresh(covers)).await)
    }

    #[harmony(
        description = "Each library's running sync or refresh, else its latest, else an idle run without an `id` for a library without one, keyed by library id. `options` narrows the runs considered. Libraries that are missing or hidden from the caller are absent.",
        returns(HashMap<String, SyncRunRecord>)
    )]
    async fn sync_status(
        library_ids: UniqueIds,
        options: Option<SyncStatusOptions>,
        #[context] caller: DispatchPrincipal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let filter = options.map_or_else(RunFilter::default, |options| options.0);
        let statuses = control::sync_status(caller.0.as_ref(), &library_ids.0, filter)
            .await
            .map_err(crate::plugins::runtime_error)?;
        let mut table = luau::OwnedTable::with_capacity(0, statuses.len());
        for (library_id, summary) in statuses {
            table.set_field(library_id, run_value(&summary)?);
        }
        Ok(table)
    }

    #[harmony(
        description = "Asks the library's running sync or refresh to stop and returns it, `cancelling` until it stops. Without one running, cancels nothing and returns what `sync_status` does for the library. Returns nil when the library is missing or hidden from the caller.",
        returns(Option<SyncRunRecord>)
    )]
    async fn cancel_sync(
        library_id: String,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::Value> {
        match control::cancel_sync(&principal, &library_id).await {
            Ok(summary) => run_value(&summary),
            Err(LibraryControlError::NotFound(_)) => Ok(luau::Value::Nil),
            Err(error) => Err(crate::plugins::runtime_error(error)),
        }
    }
}

fn sync_start(
    result: Result<SyncRunStartResponse, LibraryControlError>,
) -> luau::runtime::Result<luau::Value> {
    match result {
        Ok(response) => harmony_luau::serializable_to_luau_owned(SyncStartRecord {
            started: response.started,
            run: SyncRunRecord::from(&response.run),
        }),
        Err(LibraryControlError::NotFound(_)) => Ok(luau::Value::Nil),
        Err(error) => Err(crate::plugins::runtime_error(error)),
    }
}

fn run_value(summary: &SyncRunSummary) -> luau::runtime::Result<luau::Value> {
    harmony_luau::serializable_to_luau_owned(SyncRunRecord::from(summary))
}

/// The `options` argument of `sync`.
struct SyncOptions(LibrarySyncOptions);

impl<'vm> luau::FromLuau<'vm> for SyncOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(
            &options,
            "options",
            &["releases", "covers", "bypass_cover_cache"],
        )?;
        Ok(Self(LibrarySyncOptions {
            releases: args::optional_string_enum::<LuauSyncReleases>(
                &options, "releases", "options",
            )?
            .map(Into::into)
            .unwrap_or_default(),
            covers: args::cover_sync_options(&options, "options")?,
        }))
    }
}

impl LuauTypeInfo for SyncOptions {
    fn luau_type() -> LuauType {
        LuauType::named("SyncOptions")
    }
}

/// The `options` argument of `refresh`.
struct RefreshOptions(CoverSyncOptions);

impl<'vm> luau::FromLuau<'vm> for RefreshOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["covers", "bypass_cover_cache"])?;
        Ok(Self(args::cover_sync_options(&options, "options")?))
    }
}

impl LuauTypeInfo for RefreshOptions {
    fn luau_type() -> LuauType {
        LuauType::named("RefreshOptions")
    }
}

/// The `options` argument of `sync_status`.
struct SyncStatusOptions(RunFilter);

impl<'vm> luau::FromLuau<'vm> for SyncStatusOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["kind", "finished"])?;
        Ok(Self(RunFilter {
            kind: args::optional_string_enum::<LuauSyncRunKind>(&options, "kind", "options")?
                .map(Into::into),
            finished: args::optional_bool(&options, "finished", "options")?.unwrap_or(false),
        }))
    }
}

impl LuauTypeInfo for SyncStatusOptions {
    fn luau_type() -> LuauType {
        LuauType::named("SyncStatusOptions")
    }
}

#[derive(Serialize)]
struct LibraryRecord {
    id: String,
    name: String,
    path: Option<String>,
    language: Option<String>,
    country: Option<String>,
}

impl LuauTypeInfo for LibraryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Library")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for LibraryRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("Library", None);
        descriptor.fields.extend([
            field("id", String::luau_type(), None),
            field("name", String::luau_type(), None),
            field(
                "path",
                Option::<String>::luau_type(),
                Some(
                    "The directory the library scans. Nil unless the caller has the manage_libraries permission, or the call runs without a user.",
                ),
            ),
            field(
                "language",
                Option::<String>::luau_type(),
                Some("The ISO 639 language code of the library's metadata, if set."),
            ),
            field(
                "country",
                Option::<String>::luau_type(),
                Some("The ISO 3166 country code of the library's metadata, if set."),
            ),
        ]);
        descriptor
    }
}

#[derive(Serialize)]
struct SyncStartRecord {
    started: bool,
    run: SyncRunRecord,
}

impl LuauTypeInfo for SyncStartRecord {
    fn luau_type() -> LuauType {
        LuauType::named("SyncStart")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for SyncStartRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("SyncStart", None);
        descriptor.fields.extend([
            field(
                "started",
                bool::luau_type(),
                Some("Whether the call started `run`, rather than finding it already running."),
            ),
            field(
                "run",
                SyncRunRecord::luau_type(),
                Some("The run started, or the one already running."),
            ),
        ]);
        descriptor
    }
}

#[derive(Serialize)]
struct SyncRunRecord {
    id: Option<String>,
    kind: &'static str,
    status: &'static str,
    started_at_ms: Option<u64>,
    finished_at_ms: Option<u64>,
    error: Option<String>,
    cancellation_requested: bool,
    total_state: &'static str,
    completed_units: u64,
    failed_units: u64,
    skipped_units: u64,
    total_units: u64,
    stage: Option<&'static str>,
    subject: Option<String>,
}

impl From<&SyncRunSummary> for SyncRunRecord {
    fn from(summary: &SyncRunSummary) -> Self {
        let run = &summary.run;
        let progress = &summary.progress;
        Self {
            id: run.id.clone(),
            kind: LuauSyncRunKind::from(run.kind).as_str(),
            status: LuauSyncRunStatus::from(run.status).as_str(),
            started_at_ms: run.started_at_ms,
            finished_at_ms: run.finished_at_ms,
            error: run.error.clone(),
            cancellation_requested: run.cancellation_requested,
            total_state: LuauSyncTotalState::from(progress.total_state).as_str(),
            completed_units: progress.completed_units,
            failed_units: progress.failed_units,
            skipped_units: progress.skipped_units,
            total_units: progress.total_units,
            stage: summary
                .current
                .as_ref()
                .map(|current| LuauSyncStage::from(current.stage).as_str()),
            subject: summary
                .current
                .as_ref()
                .and_then(|current| current.subject.clone()),
        }
    }
}

impl LuauTypeInfo for SyncRunRecord {
    fn luau_type() -> LuauType {
        LuauType::named("SyncRun")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for SyncRunRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let count = u64::luau_type;
        let mut descriptor = InterfaceDescriptor::new("SyncRun", None);
        descriptor.fields.extend([
            field(
                "id",
                Option::<String>::luau_type(),
                Some("Nil for the idle run of a library without one."),
            ),
            field(
                "kind",
                LuauSyncRunKind::luau_type(),
                Some("A sync scans files; a refresh only asks providers."),
            ),
            field(
                "status",
                LuauSyncRunStatus::luau_type(),
                Some("`queued`, `planning`, `running` and `cancelling` are running."),
            ),
            field(
                "started_at_ms",
                Option::<u64>::luau_type(),
                Some("Nil for an idle run."),
            ),
            field(
                "finished_at_ms",
                Option::<u64>::luau_type(),
                Some("Nil until the run ends."),
            ),
            field(
                "error",
                Option::<String>::luau_type(),
                Some(
                    "Why a `failed` run failed. Nil unless the caller has the manage_libraries permission, or the call runs without a user.",
                ),
            ),
            field(
                "cancellation_requested",
                bool::luau_type(),
                Some("Whether a running run was asked to stop."),
            ),
            field(
                "total_state",
                LuauSyncTotalState::luau_type(),
                Some("How far `total_units` is known."),
            ),
            field(
                "completed_units",
                count(),
                Some("Units of work done, across every stage."),
            ),
            field("failed_units", count(), Some("Units of work that failed.")),
            field(
                "skipped_units",
                count(),
                Some("Units of work skipped, including those a finished run left."),
            ),
            field("total_units", count(), Some("Units of work found so far.")),
            field(
                "stage",
                Option::<LuauSyncStage>::luau_type(),
                Some("The stage of the oldest work in progress; nil unless the run is running."),
            ),
            field(
                "subject",
                Option::<String>::luau_type(),
                Some(
                    "What that work is on, such as a release title, when known. Nil unless the caller has the manage_libraries permission, or the call runs without a user.",
                ),
            ),
        ]);
        descriptor
    }
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType, description: Option<&'static str>) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description,
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    let cover_fields = |covers: &'static str| {
        [
            field(
                "covers",
                Option::<crate::plugins::types::LuauCoverUpdate>::luau_type(),
                Some(covers),
            ),
            field(
                "bypass_cover_cache",
                LuauType::optional(bool::luau_type()),
                Some(
                    "Look covers up again instead of using what earlier lookups cached. Raises with `covers` `skip`. Default `false`.",
                ),
            ),
        ]
    };

    let mut sync = InterfaceDescriptor::new("SyncOptions", None);
    sync.fields.push(field(
        "releases",
        Option::<LuauSyncReleases>::luau_type(),
        Some(
            "The releases to ask the providers about. Default `changed`: only those the scan found new or changed.",
        ),
    ));
    sync.fields.extend(cover_fields(
        "The covers to fetch for the releases the providers are asked about and for the artists credited on them or their tracks. Default `missing`: only where none is set.",
    ));

    let mut refresh = InterfaceDescriptor::new("RefreshOptions", None);
    refresh.fields.extend(cover_fields(
        "The covers to fetch for the releases the providers refreshed and the artists credited on them or their tracks. Default `missing`: only where none is set.",
    ));

    let mut status = InterfaceDescriptor::new("SyncStatusOptions", None);
    status.fields.extend([
        field(
            "kind",
            Option::<LuauSyncRunKind>::luau_type(),
            Some("Consider only runs of this kind. Default either."),
        ),
        field(
            "finished",
            LuauType::optional(bool::luau_type()),
            Some("Report the latest run that ended, passing over a running one. Default `false`."),
        ),
    ]);

    harmony_luau::render_definition_file_with_support(
        &LibrariesModule::module_descriptor(),
        &[
            LuauSyncRunKind::type_alias_descriptor(),
            LuauSyncRunStatus::type_alias_descriptor(),
            LuauSyncTotalState::type_alias_descriptor(),
            LuauSyncStage::type_alias_descriptor(),
            crate::plugins::types::LuauCoverUpdate::type_alias_descriptor(),
            LuauSyncReleases::type_alias_descriptor(),
        ],
        &[
            LibraryRecord::interface_descriptor(),
            sync,
            refresh,
            status,
            SyncStartRecord::interface_descriptor(),
            SyncRunRecord::interface_descriptor(),
        ],
        &[],
    )
}
