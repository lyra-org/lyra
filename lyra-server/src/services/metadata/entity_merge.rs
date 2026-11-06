// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Merging duplicate artists, releases, genres and labels by hand, and renaming a genre. Each
//! runs under the library mutation guard, on libraries no sync, refresh or purge is running on.

use std::collections::{
    BTreeSet,
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbId,
};
use lyra_db::{
    Permission,
    external_ids::ExternalIdAlias,
    metadata::layers::edges,
};

use super::cleanup::{
    merge_artist_into,
    migrate_metadata,
};
use super::layers::MANUAL_SOURCE_ID;
use super::merging::{
    LayerKeys,
    apply_merged_metadata_to_entity_inside_tx,
};
use crate::services::{
    STATE,
    auth::{
        Principal,
        Recheck,
        access,
    },
    libraries::{
        library_has_active_run,
        library_mutation_guard,
    },
    providers::{
        collect_track_unique_ids,
        merge_release_into_inside_tx,
        provider_registry,
        release_library_ids,
    },
};

const MAX_LOSERS: usize = 50;

#[derive(Debug, thiserror::Error)]
pub(crate) enum EntityMergeError {
    #[error("{0}")]
    Invalid(String),
    #[error("entity not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<Recheck> for EntityMergeError {
    fn from(error: Recheck) -> Self {
        match error {
            Recheck::Internal(error) => Self::Internal(error),
            other => Self::Forbidden(other.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeKind {
    Artist,
    Release,
    Genre,
    Label,
}

impl MergeKind {
    fn of(db: &impl lyra_db::DbAccess, entity: DbId) -> anyhow::Result<Option<Self>> {
        Ok(match lyra_db::graph::element_kind(db, entity)?.as_deref() {
            Some("Artist") => Some(Self::Artist),
            Some("Release") => Some(Self::Release),
            Some("Genre") => Some(Self::Genre),
            Some("Label") => Some(Self::Label),
            _ => None,
        })
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Release => "release",
            Self::Genre => "genre",
            Self::Label => "label",
        }
    }
}

/// What the losers hold that moves to the winner. A kind counts only what it has.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MergeCounts {
    pub(crate) losers: usize,
    pub(crate) releases: usize,
    pub(crate) tracks: usize,
    pub(crate) relatives: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MergeOutcome {
    pub(crate) winner_id: String,
    pub(crate) kind: MergeKind,
    pub(crate) counts: MergeCounts,
}

#[derive(Clone, Debug)]
pub(crate) struct MergePreview {
    pub(crate) outcome: MergeOutcome,
    /// The differing external IDs the winner keeps as aliases.
    pub(crate) kept_external_ids: Vec<ExternalIdAlias>,
    /// The `(disc, track)` numbers more than one of the releases hold.
    pub(crate) track_collisions: Vec<(u32, u32)>,
    /// The public IDs of the locked entities among the winner and losers.
    pub(crate) locked_ids: Vec<String>,
    /// The manual keys the winner has empty and takes from a loser, which pass through a lock.
    pub(crate) manual_fills: Vec<String>,
}

/// What a change works on as the data stands now, with the caller allowed to make it, and the
/// public IDs of the libraries it reaches.
type Resolved<T> = Result<(T, HashSet<String>), EntityMergeError>;

/// The entities of a merge.
struct Targets {
    kind: MergeKind,
    winner: DbId,
    losers: Vec<DbId>,
}

/// The public IDs of the request's losers without repeats, once the request itself makes sense.
fn checked_loser_ids(
    winner_id: &str,
    loser_ids: &[String],
) -> Result<Vec<String>, EntityMergeError> {
    if loser_ids.is_empty() {
        return Err(EntityMergeError::Invalid(
            "loser_ids must name at least one entity".to_string(),
        ));
    }
    if loser_ids.len() > MAX_LOSERS {
        return Err(EntityMergeError::Invalid(format!(
            "a merge takes at most {MAX_LOSERS} losers"
        )));
    }
    if loser_ids.iter().any(|id| id == winner_id) {
        return Err(EntityMergeError::Invalid(
            "an entity can't be merged into itself".to_string(),
        ));
    }
    let mut seen = HashSet::new();
    Ok(loser_ids
        .iter()
        .filter(|id| seen.insert(id.as_str()))
        .cloned()
        .collect())
}

/// The entity's libraries, which `principal` must reach it through unless they are an admin. A
/// hidden entity reads as a missing one.
fn reached(
    db: &DbAny,
    principal: &Principal,
    public_id: &str,
) -> Result<(DbId, Vec<lyra_db::libraries::Library>), EntityMergeError> {
    let not_found = || EntityMergeError::NotFound(public_id.to_string());
    let entity = lyra_db::lookup::find_node_id_by_id(db, public_id)?.ok_or_else(not_found)?;
    let libraries = access::affected_libraries(db, entity)?;
    if !access::any_library_accessible(principal, &libraries) {
        return Err(not_found());
    }
    Ok((entity, libraries))
}

fn require_every_library(
    principal: &Principal,
    libraries: &[lyra_db::libraries::Library],
) -> Result<(), EntityMergeError> {
    if access::every_library_accessible(principal, libraries) {
        Ok(())
    } else {
        Err(EntityMergeError::Forbidden(
            "this change reaches a library you can't access".to_string(),
        ))
    }
}

/// Everything about a merge that holds or fails on the data alone.
fn targets(
    db: &DbAny,
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
    unique_track_ids: &HashSet<(String, String)>,
) -> Resolved<Targets> {
    let (winner, mut libraries) = reached(db, principal, winner_id)?;
    let kind = MergeKind::of(db, winner)?.ok_or_else(|| {
        EntityMergeError::Invalid("only artists, releases, genres and labels merge".to_string())
    })?;
    let mut losers = Vec::with_capacity(loser_ids.len());
    for loser_id in loser_ids {
        let (loser, loser_libraries) = reached(db, principal, loser_id)?;
        if MergeKind::of(db, loser)? != Some(kind) {
            return Err(EntityMergeError::Invalid(format!(
                "{loser_id} is not a {}",
                kind.as_str()
            )));
        }
        libraries.extend(loser_libraries);
        losers.push(loser);
    }
    require_every_library(principal, &libraries)?;

    match kind {
        MergeKind::Release => check_releases(db, winner, &losers, unique_track_ids)?,
        MergeKind::Genre => check_genre_name(db, winner)?,
        MergeKind::Artist | MergeKind::Label => {}
    }
    Ok((
        Targets {
            kind,
            winner,
            losers,
        },
        libraries.into_iter().map(|library| library.id).collect(),
    ))
}

fn check_releases(
    db: &DbAny,
    winner: DbId,
    losers: &[DbId],
    unique_track_ids: &HashSet<(String, String)>,
) -> Result<(), EntityMergeError> {
    let winner_libraries = release_library_ids(db, winner)?;
    let mut track_ids = collect_track_unique_ids(db, winner, unique_track_ids)?;
    for &loser in losers {
        if release_library_ids(db, loser)? != winner_libraries {
            return Err(EntityMergeError::Conflict(
                "releases merge only within the same libraries".to_string(),
            ));
        }
        let loser_track_ids = collect_track_unique_ids(db, loser, unique_track_ids)?;
        if !track_ids.is_disjoint(&loser_track_ids) {
            return Err(EntityMergeError::Conflict(
                "these releases hold the same track".to_string(),
            ));
        }
        track_ids.extend(loser_track_ids);
    }
    Ok(())
}

fn check_genre_name(db: &DbAny, winner: DbId) -> Result<(), EntityMergeError> {
    let genre = lyra_db::genres::get_by_id(db, winner)?
        .ok_or_else(|| anyhow::anyhow!("genre {} missing", winner.0))?;
    if genre.name.trim().is_empty() {
        return Err(EntityMergeError::Invalid(
            "a genre with a blank name can't win a merge".to_string(),
        ));
    }
    Ok(())
}

/// Fails if a run holds any of `libraries`. It takes the database lock itself, so it can't run
/// under a guard of the caller's.
async fn check_no_active_run(libraries: &HashSet<String>) -> Result<(), EntityMergeError> {
    for library in libraries {
        if library_has_active_run(library).await? {
            return Err(EntityMergeError::Conflict(
                "a sync, refresh or purge of an affected library is running".to_string(),
            ));
        }
    }
    Ok(())
}

/// A run can start on a library the entities reach only now, so its check would have been missed.
fn check_libraries_known(
    known: &HashSet<String>,
    current: &HashSet<String>,
) -> Result<(), EntityMergeError> {
    if current.is_subset(known) {
        Ok(())
    } else {
        Err(EntityMergeError::Conflict(
            "the entities reached another library while this was checked".to_string(),
        ))
    }
}

async fn unique_track_ids() -> HashSet<(String, String)> {
    provider_registry()
        .read_owned()
        .await
        .unique_track_id_pairs()
}

/// A change that passed its checks, holding the library mutation guard so that no run starts
/// on its libraries, to check again under the database guard that carries it out.
struct Checked<R> {
    resolve: R,
    libraries: HashSet<String>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl<T, R: Fn(&DbAny, &Principal) -> Resolved<T>> Checked<R> {
    async fn new(principal: &Principal, resolve: R) -> Result<Self, EntityMergeError> {
        let guard = library_mutation_guard().await;
        let libraries = {
            let db = STATE.db.read().await;
            let current = principal.current_with(&db, Permission::ManageMetadata)?;
            resolve(&db, &current)?.1
        };
        check_no_active_run(&libraries).await?;
        Ok(Self {
            resolve,
            libraries,
            _guard: guard,
        })
    }

    /// Everything but the run check again, on the data under `db`'s guard.
    fn revalidated(&self, db: &DbAny, principal: &Principal) -> Result<T, EntityMergeError> {
        let current = principal.current_with(db, Permission::ManageMetadata)?;
        let (target, libraries) = (self.resolve)(db, &current)?;
        check_libraries_known(&self.libraries, &libraries)?;
        Ok(target)
    }
}

fn counts(db: &DbAny, targets: &Targets) -> anyhow::Result<MergeCounts> {
    let mut counts = MergeCounts {
        losers: targets.losers.len(),
        ..MergeCounts::default()
    };
    for &loser in &targets.losers {
        match targets.kind {
            MergeKind::Artist => {
                counts.releases += lyra_db::releases::get_by_artist(db, loser)?.len();
                counts.tracks += lyra_db::tracks::get_by_artist(db, loser)?.len();
            }
            MergeKind::Release => {
                counts.tracks += lyra_db::tracks::get_direct(db, loser)?.len();
            }
            MergeKind::Genre => {
                counts.releases += lyra_db::genres::get_releases(db, loser)?.len();
                counts.relatives += lyra_db::genres::get_parents(db, loser)?.len()
                    + lyra_db::genres::get_children(db, loser)?.len();
            }
            MergeKind::Label => {
                counts.releases += lyra_db::labels::get_releases(db, loser)?.len();
            }
        }
    }
    Ok(counts)
}

/// Merges each of `loser_ids` into `winner_id`, in one transaction: what is attached to them moves
/// to the winner, their names and differing external IDs keep finding it, and they are gone.
pub(crate) async fn merge(
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<MergeOutcome, EntityMergeError> {
    let loser_ids = checked_loser_ids(winner_id, loser_ids)?;
    let unique_track_ids = unique_track_ids().await;
    let checked = Checked::new(principal, |db: &DbAny, current: &Principal| {
        targets(db, current, winner_id, &loser_ids, &unique_track_ids)
    })
    .await?;

    let mut db = STATE.db.write().await;
    let targets = checked.revalidated(&db, principal)?;
    let counts = counts(&db, &targets)?;
    db.transaction_mut(|t| -> anyhow::Result<()> {
        for &loser in &targets.losers {
            merge_into(t, targets.kind, targets.winner, loser)?;
        }
        Ok(())
    })?;
    Ok(MergeOutcome {
        winner_id: winner_id.to_string(),
        kind: targets.kind,
        counts,
    })
}

/// What [`merge`] would do, under the same checks. It binds nothing: the merge checks again.
pub(crate) async fn preview(
    principal: &Principal,
    winner_id: &str,
    loser_ids: &[String],
) -> Result<MergePreview, EntityMergeError> {
    let loser_ids = checked_loser_ids(winner_id, loser_ids)?;
    let unique_track_ids = unique_track_ids().await;
    let checked = Checked::new(principal, |db: &DbAny, current: &Principal| {
        targets(db, current, winner_id, &loser_ids, &unique_track_ids)
    })
    .await?;

    let db = STATE.db.read().await;
    let targets = checked.revalidated(&db, principal)?;
    Ok(MergePreview {
        outcome: MergeOutcome {
            winner_id: winner_id.to_string(),
            kind: targets.kind,
            counts: counts(&db, &targets)?,
        },
        kept_external_ids: lyra_db::external_ids::absorb_plan(
            &*db,
            targets.winner,
            &targets.losers,
        )?,
        track_collisions: track_collisions(&db, &targets)?,
        locked_ids: locked_ids(&db, &targets)?,
        manual_fills: manual_fills(&db, &targets)?,
    })
}

fn track_collisions(db: &DbAny, targets: &Targets) -> anyhow::Result<Vec<(u32, u32)>> {
    if targets.kind != MergeKind::Release {
        return Ok(Vec::new());
    }
    let mut seen: HashMap<(u32, u32), DbId> = HashMap::new();
    let mut collisions = BTreeSet::new();
    for &release in std::iter::once(&targets.winner).chain(&targets.losers) {
        for track in lyra_db::tracks::get_direct(db, release)? {
            let Some(number) = track.track else {
                continue;
            };
            let position = (track.disc.unwrap_or(1), number);
            if *seen.entry(position).or_insert(release) != release {
                collisions.insert(position);
            }
        }
    }
    Ok(collisions.into_iter().collect())
}

fn locked_ids(db: &DbAny, targets: &Targets) -> anyhow::Result<Vec<String>> {
    let mut locked = Vec::new();
    for &entity in std::iter::once(&targets.winner).chain(&targets.losers) {
        let id = match targets.kind {
            MergeKind::Artist => lyra_db::artists::get_by_id(db, entity)?
                .filter(|artist| artist.locked.unwrap_or(false))
                .map(|artist| artist.id),
            MergeKind::Release => lyra_db::releases::get_by_id(db, entity)?
                .filter(|release| release.locked.unwrap_or(false))
                .map(|release| release.id),
            MergeKind::Genre | MergeKind::Label => None,
        };
        locked.extend(id);
    }
    Ok(locked)
}

fn manual_fills(db: &DbAny, targets: &Targets) -> anyhow::Result<Vec<String>> {
    let manual_keys = |entity| -> anyhow::Result<LayerKeys> {
        Ok(lyra_db::metadata::layers::get_for_entity(db, entity)?
            .iter()
            .find(|layer| layer.source_id == MANUAL_SOURCE_ID)
            .map(LayerKeys::supplied_by)
            .unwrap_or_default())
    };
    let winner = manual_keys(targets.winner)?;
    let mut fills = BTreeSet::new();
    for &loser in &targets.losers {
        let loser = manual_keys(loser)?;
        fills.extend(
            loser
                .fields
                .difference(&winner.fields)
                .map(ToString::to_string),
        );
        fills.extend(
            loser
                .credits
                .difference(&winner.credits)
                .map(ToString::to_string),
        );
        fills.extend(
            loser
                .relations
                .difference(&winner.relations)
                .map(ToString::to_string),
        );
    }
    Ok(fills.into_iter().collect())
}

fn merge_into(
    t: &mut DbAnyTransactionMut<'_>,
    kind: MergeKind,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    match kind {
        MergeKind::Artist => {
            migrate_metadata(t, winner, loser)?;
            lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
            merge_artist_into(t, winner, loser)
        }
        MergeKind::Release => merge_release_into_inside_tx(t, winner, loser),
        MergeKind::Genre => merge_genre_into(t, winner, loser),
        MergeKind::Label => merge_label_into(t, winner, loser),
    }
}

/// The entities whose stored values name `loser`, by a layer or as `linked`.
fn owners(t: &DbAnyTransactionMut<'_>, loser: DbId, linked: &[DbId]) -> anyhow::Result<Vec<DbId>> {
    let mut owners = edges::referencing_owners(t, loser)?;
    for &release in linked {
        if !owners.contains(&release) {
            owners.push(release);
        }
    }
    Ok(owners)
}

fn release_locked(t: &DbAnyTransactionMut<'_>, release: DbId) -> anyhow::Result<bool> {
    Ok(lyra_db::releases::get_by_id(t, release)?
        .is_some_and(|release| release.locked.unwrap_or(false)))
}

fn merge_genre_into(
    t: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let linked = lyra_db::genres::get_releases(t, loser)?;
    let owners = owners(t, loser, &linked)?;
    edges::retarget_genre(t, winner, loser)?;
    // A locked release keeps what is stored through the apply below, so its link moves.
    for &release in &linked {
        if release_locked(t, release)? {
            lyra_db::genres::move_release_link(t, release, winner, loser)?;
        }
    }
    for owner in owners {
        apply_merged_metadata_to_entity_inside_tx(t, owner)?;
    }
    lyra_db::genres::absorb_relatives(t, winner, loser)?;
    lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
    lyra_db::genres::absorb_aliases(t, winner, loser)?;
    lyra_db::genres::remove_inside_tx(t, loser)
}

fn merge_label_into(
    t: &mut DbAnyTransactionMut<'_>,
    winner: DbId,
    loser: DbId,
) -> anyhow::Result<()> {
    let linked = lyra_db::labels::get_releases(t, loser)?;
    let owners = owners(t, loser, &linked)?;
    edges::retarget_label(t, winner, loser)?;
    // The apply below can collect the loser with its last link, so what it holds moves first.
    lyra_db::external_ids::absorb_inside_tx(t, winner, loser)?;
    lyra_db::labels::absorb_aliases(t, winner, loser)?;
    for &release in &linked {
        if release_locked(t, release)? {
            lyra_db::labels::move_release_link(t, release, winner, loser)?;
        }
    }
    for owner in owners {
        apply_merged_metadata_to_entity_inside_tx(t, owner)?;
    }
    lyra_db::labels::collect_unreferenced(t, &[loser])?;
    anyhow::ensure!(
        lyra_db::labels::get_by_id(t, loser)?.is_none(),
        "merged label {} is still referenced",
        loser.0
    );
    Ok(())
}

/// Renames the genre `id`, under a genre merge's access rule and locking order. Returns the name
/// it now goes by.
pub(crate) async fn rename_genre(
    principal: &Principal,
    id: &str,
    name: &str,
) -> Result<String, EntityMergeError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(EntityMergeError::Invalid(
            "a genre's name can't be blank".to_string(),
        ));
    }

    let checked = Checked::new(principal, |db: &DbAny, current: &Principal| {
        genre_target(db, current, id)
    })
    .await?;

    let mut db = STATE.db.write().await;
    let genre = checked.revalidated(&db, principal)?;
    if lyra_db::genres::name_taken(&*db, name, genre)? {
        return Err(EntityMergeError::Conflict(format!(
            "another genre goes by {name}"
        )));
    }
    db.transaction_mut(|t| lyra_db::genres::rename(t, genre, name))?;
    Ok(name.to_string())
}

fn genre_target(db: &DbAny, principal: &Principal, id: &str) -> Resolved<DbId> {
    let (genre, libraries) = reached(db, principal, id)?;
    if MergeKind::of(db, genre)? != Some(MergeKind::Genre) {
        return Err(EntityMergeError::NotFound(id.to_string()));
    }
    require_every_library(principal, &libraries)?;
    Ok((
        genre,
        libraries.into_iter().map(|library| library.id).collect(),
    ))
}

/// Removes an external ID alias, so a wrong merge stops routing the value to the entity. It takes
/// a merge's access rule, and no run is at risk from it.
pub(crate) async fn remove_external_id_alias(
    principal: &Principal,
    id: &str,
    provider_id: &str,
    id_type: &str,
    value: &str,
) -> Result<(), EntityMergeError> {
    let mut db = STATE.db.write().await;
    let current = principal.current_with(&db, Permission::ManageMetadata)?;
    let (entity, libraries) = reached(&db, &current, id)?;
    require_every_library(&current, &libraries)?;
    let removed = db.transaction_mut(|t| {
        lyra_db::external_ids::remove_alias(t, entity, provider_id, id_type, value)
    })?;
    if removed {
        Ok(())
    } else {
        Err(EntityMergeError::NotFound(format!(
            "{provider_id}/{id_type}/{value}"
        )))
    }
}

#[cfg(test)]
mod tests;
