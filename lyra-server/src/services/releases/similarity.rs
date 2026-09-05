// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    future::Future,
    time::{
        Duration,
        Instant,
    },
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use serde_json::Value;

use crate::{
    STATE,
    db::{
        self,
        Permission,
        Release,
    },
    plugins::executor::{
        PluginExecutorHandle,
        SimilarReleaseCandidate,
        SimilarReleaseExternalRef,
        SimilarReleasesDispatchRequest,
    },
    services::{
        EntityType,
        providers::{
            MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT,
            ProviderSimilarReleasesSpec,
            enabled_provider_configs_by_priority,
            requirements_match,
        },
    },
};

pub(crate) const DEFAULT_SIMILAR_RELEASE_LIMIT: usize = 20;
pub(crate) const MAX_SIMILAR_RELEASE_LIMIT: usize = 100;

const MAX_PROVIDER_CANDIDATES: usize = 400;
const DISPATCH_BUDGET: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub(crate) struct SimilarReleaseOptions {
    pub(crate) limit: usize,
    /// `None` means all libraries are visible (server-internal/admin use).
    pub(crate) accessible_library_ids: Option<HashSet<String>>,
}

impl Default for SimilarReleaseOptions {
    fn default() -> Self {
        Self {
            limit: DEFAULT_SIMILAR_RELEASE_LIMIT,
            accessible_library_ids: None,
        }
    }
}

#[derive(Clone)]
struct ProviderHandler {
    provider_id: String,
    spec: ProviderSimilarReleasesSpec,
}

struct ProviderSnapshot {
    runtime: Option<PluginExecutorHandle>,
    handlers: Vec<ProviderHandler>,
    release_id_pairs: HashSet<(String, String)>,
    unique_release_id_pairs: HashSet<(String, String)>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReleaseIdentity {
    db_id: DbId,
    public_id: String,
}

struct SeedContext {
    public_id: String,
    provider_contexts: HashMap<String, Value>,
    library_ids: HashSet<String>,
}

pub(crate) fn accessible_library_ids_for_user(
    db: &DbAny,
    user_db_id: DbId,
) -> Result<Option<HashSet<String>>> {
    if db::users::get_by_id(db, user_db_id)?.is_none() {
        return Err(anyhow!("user not found: {}", user_db_id.0));
    }
    let is_admin = db::roles::get_role_for_user(db, user_db_id)?
        .is_some_and(|role| role.permissions.contains(&Permission::Admin));
    if is_admin {
        Ok(None)
    } else {
        Ok(Some(db::libraries::accessible_library_ids(db, user_db_id)?))
    }
}

pub(crate) async fn similar(
    seed_db_id: DbId,
    options: &SimilarReleaseOptions,
) -> Result<Option<Vec<Release>>> {
    similar_with_dispatch(seed_db_id, options, None, |runtime, request| async move {
        runtime
            .context("plugin runtime is not initialized")?
            .dispatch_similar_releases(request)
            .await
    })
    .await
}

pub(crate) async fn similar_in_vm(
    seed_db_id: DbId,
    options: &SimilarReleaseOptions,
    provider_vm: harmony_luau::Vm,
) -> Result<Option<Vec<Release>>> {
    let expected_vm_id = provider_vm.id();
    similar_with_dispatch(
        seed_db_id,
        options,
        Some(expected_vm_id),
        move |_runtime, request| {
            let provider_vm = provider_vm.clone();
            async move {
                crate::plugins::executor::dispatch_similar_releases_in_vm(provider_vm, request)
                    .await
            }
        },
    )
    .await
}

async fn similar_with_dispatch<F, Fut>(
    seed_db_id: DbId,
    options: &SimilarReleaseOptions,
    expected_vm_id: Option<u64>,
    dispatch_provider: F,
) -> Result<Option<Vec<Release>>>
where
    F: Fn(Option<PluginExecutorHandle>, SimilarReleasesDispatchRequest) -> Fut + Clone,
    Fut: Future<Output = Result<crate::plugins::executor::SimilarReleasesDispatchResult>>,
{
    let limit = options.limit.min(MAX_SIMILAR_RELEASE_LIMIT);
    if limit == 0 {
        return Ok(Some(Vec::new()));
    }

    let generation = STATE.generation();
    let snapshot = provider_snapshot(&generation).await?;
    if let (Some(expected_vm_id), Some(runtime)) = (expected_vm_id, &snapshot.runtime)
        && expected_vm_id != runtime.vm_id()
    {
        return Err(anyhow!(
            "plugin runtime changed while starting similar releases dispatch"
        ));
    }
    let runtime = snapshot.runtime;
    let handlers = snapshot.handlers;
    let seed = {
        let db = STATE.db.read().await;
        build_seed_context(
            &db,
            seed_db_id,
            limit,
            &handlers,
            options.accessible_library_ids.as_ref(),
        )?
    };
    let Some(seed) = seed else {
        return Ok(None);
    };

    let started = Instant::now();
    let mut selected = Vec::with_capacity(limit);
    let mut selected_ids = HashSet::with_capacity(limit + 1);
    selected_ids.insert(seed_db_id);

    for handler in handlers {
        if selected.len() >= limit {
            break;
        }
        let Some(context) = seed.provider_contexts.get(&handler.provider_id) else {
            continue;
        };
        if !requirements_match(context, &handler.spec.require) {
            continue;
        }
        let Some(remaining) = DISPATCH_BUDGET.checked_sub(started.elapsed()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }
        debug_assert!(handler.spec.timeout <= MAX_SIMILAR_RELEASES_HANDLER_TIMEOUT);
        let handler_timeout = handler.spec.timeout;
        let provider_id = handler.provider_id.clone();
        let handler_id = handler.spec.handler.handler_id;
        let context = context.clone();
        let cancellation = crate::plugins::executor::MetadataRefreshCancellation::default();
        let cancellation_for_call = cancellation.clone();
        let runtime = runtime.clone();
        let provider_id_for_call = provider_id.clone();
        let dispatch_provider = dispatch_provider.clone();
        let request = SimilarReleasesDispatchRequest {
            provider_id: provider_id_for_call,
            handler_id,
            context,
            timeout: handler_timeout,
            cancellation: cancellation_for_call,
            max_candidates: MAX_PROVIDER_CANDIDATES,
        };
        // Executor-dispatched handlers are already serialized by the executor. Holding the
        // provider call lock while waiting for it inverts the lock order with in-VM calls.
        let dispatch = tokio::time::timeout(remaining, dispatch_provider(runtime, request)).await;

        let dispatch = match dispatch {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                tracing::warn!(
                    provider_id,
                    error = %err,
                    "similar releases provider failed"
                );
                continue;
            }
            Err(_) => {
                cancellation.cancel();
                tracing::warn!(
                    provider_id,
                    timeout_ms = remaining.as_millis() as u64,
                    "similar releases request budget expired while waiting for provider"
                );
                break;
            }
        };

        let candidates = validate_external_candidates(
            dispatch.candidates,
            &snapshot.release_id_pairs,
            &provider_id,
        );
        let resolved = {
            let db = STATE.db.read().await;
            if !release_identity_is_current(&db, seed_db_id, &seed.public_id)? {
                return Ok(None);
            }
            if !release_is_accessible(&db, seed_db_id, options.accessible_library_ids.as_ref())? {
                return Ok(None);
            }
            resolve_candidates(
                &db,
                candidates,
                CandidateResolution {
                    seed_library_ids: &seed.library_ids,
                    accessible_library_ids: options.accessible_library_ids.as_ref(),
                    seed_db_id,
                    unique_id_pairs: &snapshot.unique_release_id_pairs,
                },
                &selected_ids,
                limit - selected.len(),
            )?
        };
        for identity in resolved {
            if selected_ids.insert(identity.db_id) {
                selected.push(identity);
                if selected.len() >= limit {
                    break;
                }
            }
        }
    }

    let db = STATE.db.read().await;
    if !release_identity_is_current(&db, seed_db_id, &seed.public_id)? {
        return Ok(None);
    }
    if !release_is_accessible(&db, seed_db_id, options.accessible_library_ids.as_ref())? {
        return Ok(None);
    }
    let mut releases = Vec::with_capacity(selected.len());
    for identity in selected {
        if !release_identity_is_current(&db, identity.db_id, &identity.public_id)? {
            continue;
        }
        if !release_is_accessible(&db, identity.db_id, options.accessible_library_ids.as_ref())? {
            continue;
        }
        if let Some(release) = db::releases::get_by_id(&db, identity.db_id)? {
            releases.push(release);
        }
    }
    Ok(Some(releases))
}

async fn provider_snapshot(generation: &crate::GenerationState) -> Result<ProviderSnapshot> {
    let configs = {
        let db = STATE.db.read().await;
        enabled_provider_configs_by_priority(&db, None)?
    };

    for _ in 0..3 {
        let before = generation.plugin_runtime.get();
        let (handlers, release_id_pairs, unique_release_id_pairs) = {
            let registry = generation.providers.registry().read_owned().await;
            let handlers = configs
                .iter()
                .filter_map(|config| {
                    registry
                        .get_similar_releases_handler(&config.provider_id)
                        .cloned()
                        .map(|spec| ProviderHandler {
                            provider_id: config.provider_id.clone(),
                            spec,
                        })
                })
                .collect();
            (
                handlers,
                registry.id_pairs(EntityType::Release),
                registry.unique_id_pairs(EntityType::Release),
            )
        };
        let after = generation.plugin_runtime.get();
        let stable = match (&before, &after) {
            (Some(before), Some(after)) => before.same_instance(after),
            (None, None) => true,
            _ => false,
        };
        if stable {
            return Ok(ProviderSnapshot {
                runtime: before,
                handlers,
                release_id_pairs,
                unique_release_id_pairs,
            });
        }
    }
    Err(anyhow!(
        "plugin runtime changed repeatedly while snapshotting similar releases providers"
    ))
}

fn build_seed_context(
    db: &DbAny,
    seed_db_id: DbId,
    limit: usize,
    handlers: &[ProviderHandler],
    accessible_library_ids: Option<&HashSet<String>>,
) -> Result<Option<SeedContext>> {
    let Some(release) = db::releases::get_by_id(db, seed_db_id)? else {
        return Ok(None);
    };
    let library_ids = release_library_ids(db, seed_db_id)?;
    if !libraries_are_accessible(&library_ids, accessible_library_ids) {
        return Ok(None);
    }
    let mut external_ids: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for external_id in db::external_ids::get_for_entity(db, seed_db_id)? {
        let provider_id = external_id.provider_id.trim();
        let id_type = external_id.id_type.trim();
        let id_value = external_id.id_value.trim();
        if provider_id.is_empty() || id_type.is_empty() || id_value.is_empty() {
            continue;
        }
        external_ids
            .entry(provider_id.to_string())
            .or_default()
            .insert(id_type.to_string(), id_value.to_string());
    }
    let artist_names = db::artists::get(db, seed_db_id)?
        .into_iter()
        .map(|artist| artist.artist_name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    let genres = db::genres::get_names_for_release(db, seed_db_id)?.unwrap_or_default();
    let mut provider_contexts = HashMap::with_capacity(handlers.len());
    for handler in handlers {
        let ids = external_ids
            .get(&handler.provider_id)
            .cloned()
            .unwrap_or_default();
        provider_contexts.insert(
            handler.provider_id.clone(),
            serde_json::json!({
                "db_id": seed_db_id.0,
                "id": release.id,
                "release_title": release.release_title,
                "sort_title": release.sort_title,
                "release_date": release.release_date,
                "ids": ids,
                "external_ids": external_ids,
                "artist_names": artist_names,
                "genres": genres,
                "limit": limit,
            }),
        );
    }

    Ok(Some(SeedContext {
        public_id: release.id,
        provider_contexts,
        library_ids,
    }))
}

fn validate_external_candidates(
    candidates: Vec<SimilarReleaseCandidate>,
    release_id_pairs: &HashSet<(String, String)>,
    source_provider_id: &str,
) -> Vec<SimilarReleaseCandidate> {
    candidates
        .into_iter()
        .filter(|candidate| {
            let SimilarReleaseCandidate::External(external) = candidate else {
                return true;
            };
            let registered = release_id_pairs
                .contains(&(external.provider_id.clone(), external.id_type.clone()));
            if !registered {
                tracing::warn!(
                    source_provider_id,
                    external_provider_id = external.provider_id,
                    id_type = external.id_type,
                    id_value = external.id_value,
                    "dropping similar release candidate with unregistered external release ID"
                );
            }
            registered
        })
        .collect()
}

struct CandidateResolution<'a> {
    seed_library_ids: &'a HashSet<String>,
    accessible_library_ids: Option<&'a HashSet<String>>,
    seed_db_id: DbId,
    unique_id_pairs: &'a HashSet<(String, String)>,
}

fn resolve_candidates(
    db: &DbAny,
    candidates: Vec<SimilarReleaseCandidate>,
    context: CandidateResolution<'_>,
    already_selected_ids: &HashSet<DbId>,
    max_results: usize,
) -> Result<Vec<ReleaseIdentity>> {
    let CandidateResolution {
        seed_library_ids,
        accessible_library_ids,
        seed_db_id,
        unique_id_pairs,
    } = context;
    let mut resolved = Vec::with_capacity(candidates.len().min(max_results));
    let mut resolved_ids = HashSet::with_capacity(max_results);
    let mut seen_candidates = HashSet::new();
    for candidate in candidates {
        if !seen_candidates.insert(candidate.clone()) {
            continue;
        }
        let owners = match candidate {
            SimilarReleaseCandidate::Local {
                release_db_id,
                release_id,
            } if DbId(release_db_id) != seed_db_id
                && release_identity_is_current(db, DbId(release_db_id), &release_id)? =>
            {
                vec![DbId(release_db_id)]
            }
            SimilarReleaseCandidate::Local { .. } => continue,
            SimilarReleaseCandidate::External(ref external) => {
                let owners = db::external_ids::get_owners(
                    db,
                    &external.provider_id,
                    &external.id_type,
                    &external.id_value,
                    Some("Release"),
                )?;
                if unique_id_pairs
                    .contains(&(external.provider_id.clone(), external.id_type.clone()))
                {
                    diagnose_unique_id_collisions(db, external, &owners)?;
                }
                owners
            }
        };
        if let Some(identity) = representative_release(
            db,
            owners,
            seed_library_ids,
            accessible_library_ids,
            seed_db_id,
        )? && !already_selected_ids.contains(&identity.db_id)
            && resolved_ids.insert(identity.db_id)
        {
            resolved.push(identity);
            if resolved.len() >= max_results {
                break;
            }
        }
    }
    Ok(resolved)
}

fn diagnose_unique_id_collisions(
    db: &DbAny,
    external: &SimilarReleaseExternalRef,
    owners: &[DbId],
) -> Result<()> {
    let mut owner_count_by_library = HashMap::<String, usize>::new();
    for owner in owners {
        for library_id in release_library_ids(db, *owner)? {
            *owner_count_by_library.entry(library_id).or_default() += 1;
        }
    }
    for (library_id, owner_count) in owner_count_by_library {
        if owner_count > 1 {
            tracing::warn!(
                provider_id = external.provider_id,
                id_type = external.id_type,
                id_value = external.id_value,
                library_id,
                owner_count,
                "unique release external ID has multiple owners in one library"
            );
        }
    }
    Ok(())
}

fn representative_release(
    db: &DbAny,
    owners: Vec<DbId>,
    seed_library_ids: &HashSet<String>,
    accessible_library_ids: Option<&HashSet<String>>,
    seed_db_id: DbId,
) -> Result<Option<ReleaseIdentity>> {
    let mut representatives = Vec::new();
    let mut seen = HashSet::new();
    for db_id in owners {
        if db_id == seed_db_id {
            continue;
        }
        if !seen.insert(db_id) {
            continue;
        }
        let Some(release) = db::releases::get_by_id(db, db_id)? else {
            continue;
        };
        let library_ids = release_library_ids(db, db_id)?;
        if !libraries_are_accessible(&library_ids, accessible_library_ids) {
            continue;
        }
        let shares_seed_library = !library_ids.is_disjoint(seed_library_ids);
        representatives.push((
            !shares_seed_library,
            release.id.clone(),
            ReleaseIdentity {
                db_id,
                public_id: release.id,
            },
        ));
    }
    representatives.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    Ok(representatives
        .into_iter()
        .next()
        .map(|(_, _, identity)| identity))
}

fn release_is_accessible(
    db: &DbAny,
    release_db_id: DbId,
    accessible_library_ids: Option<&HashSet<String>>,
) -> Result<bool> {
    Ok(libraries_are_accessible(
        &release_library_ids(db, release_db_id)?,
        accessible_library_ids,
    ))
}

fn libraries_are_accessible(
    release_library_ids: &HashSet<String>,
    accessible_library_ids: Option<&HashSet<String>>,
) -> bool {
    accessible_library_ids.is_none_or(|accessible| !release_library_ids.is_disjoint(accessible))
}

fn release_library_ids(db: &DbAny, release_db_id: DbId) -> Result<HashSet<String>> {
    Ok(db::libraries::get_by_release(db, release_db_id)?
        .into_iter()
        .map(|library| library.id)
        .collect())
}

fn release_identity_is_current(db: &DbAny, db_id: DbId, public_id: &str) -> Result<bool> {
    Ok(
        db::lookup::find_node_id_by_id(db, public_id)? == Some(db_id)
            && db::releases::get_by_id(db, db_id)?.is_some(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        connect,
        insert_library,
        insert_release,
        new_test_db,
    };
    use std::{
        path::PathBuf,
        time::{
            SystemTime,
            UNIX_EPOCH,
        },
    };

    struct TempSimilarPluginDir {
        root: PathBuf,
    }

    impl TempSimilarPluginDir {
        fn new() -> Result<Self> {
            Self::with_source(
                &["lyra.metadata"],
                r#"
                    local metadata = require("@lyra/metadata")
                    local provider = metadata.Provider.new("similartest")
                    provider:id({
                        id_type = "release_group_id",
                        entity = metadata.EntityType.Release,
                    })
                    provider:similar_releases({
                        require = { all_of = { "ids.release_group_id" } },
                    }, function(ctx)
                        if ctx.limit ~= 5 or ctx.ids.release_group_id ~= "group-1" then
                            error("unexpected similar release context")
                        end
                        return {
                            candidates = {
                                {
                                    external_id = {
                                        provider_id = "similartest",
                                        id_type = "release_group_id",
                                        id_value = ctx.ids.release_group_id,
                                    },
                                },
                            },
                        }
                    end)
                "#,
            )
        }

        fn with_source(scopes: &[&str], source: &str) -> Result<Self> {
            let root = std::env::temp_dir().join(format!(
                "lyra-similar-plugin-test-{}-{}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            ));
            let plugin = root.join("similartest");
            std::fs::create_dir_all(&plugin)?;
            std::fs::write(
                plugin.join("plugin.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "id": "similartest",
                    "name": "Similar Test",
                    "version": "1.0.0",
                    "description": "Similar releases contract test",
                    "entrypoint": "init.luau",
                    "scopes": scopes,
                }))?,
            )?;
            std::fs::write(plugin.join("init.luau"), source)?;
            Ok(Self { root })
        }
    }

    impl Drop for TempSimilarPluginDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    async fn publish_test_runtime(plugin: &TempSimilarPluginDir) -> Result<PluginExecutorHandle> {
        let server_info = crate::plugins::server::load_server_info().await?;
        let auth_capabilities =
            crate::plugins::auth::AuthCapabilities::from_config(&STATE.config().auth);
        let (runtime, errors) =
            crate::plugins::executor::PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
                plugin.root.clone(),
                server_info,
                auth_capabilities,
                STATE.db.get(),
                Vec::new(),
                Some(STATE.settings.clone()),
            )?;
        assert!(errors.is_empty(), "{errors:?}");
        runtime.exec_plugin("similartest").await?;
        crate::plugins::bootstrap::publish_runtime(runtime.clone());
        Ok(runtime)
    }

    #[test]
    fn representative_prefers_seed_library_then_public_id() -> Result<()> {
        let mut db = new_test_db()?;
        let seed_library = insert_library(&mut db, "Seed", "/seed")?;
        let other_library = insert_library(&mut db, "Other", "/other")?;
        let seed = insert_release(&mut db, "Seed")?;
        let same_library = insert_release(&mut db, "Same Library")?;
        let other = insert_release(&mut db, "Other Library")?;
        connect(&mut db, seed_library, seed)?;
        connect(&mut db, seed_library, same_library)?;
        connect(&mut db, other_library, other)?;

        let seed_library_ids = release_library_ids(&db, seed)?;
        let chosen = representative_release(
            &db,
            vec![seed, other, same_library],
            &seed_library_ids,
            None,
            seed,
        )?
        .ok_or_else(|| anyhow!("expected representative"))?;
        assert_eq!(chosen.db_id, same_library);

        let other_library_id = db::libraries::get_by_id(&db, other_library)?
            .ok_or_else(|| anyhow!("other library missing"))?
            .id;
        let chosen = representative_release(
            &db,
            vec![same_library, other],
            &seed_library_ids,
            Some(&HashSet::from([other_library_id])),
            seed,
        )?
        .ok_or_else(|| anyhow!("expected accessible representative"))?;
        assert_eq!(chosen.db_id, other);
        Ok(())
    }

    #[test]
    fn seed_context_is_hidden_when_no_seed_library_is_accessible() -> Result<()> {
        let mut db = new_test_db()?;
        let library = insert_library(&mut db, "Private", "/private")?;
        let seed = insert_release(&mut db, "Private Seed")?;
        connect(&mut db, library, seed)?;

        assert!(build_seed_context(&db, seed, 20, &[], Some(&HashSet::new()))?.is_none());
        Ok(())
    }

    #[test]
    fn local_candidate_requires_matching_public_identity() -> Result<()> {
        let mut db = new_test_db()?;
        let seed = insert_release(&mut db, "Seed")?;
        let candidate = insert_release(&mut db, "Candidate")?;
        let selected = HashSet::from([seed]);

        let resolved = resolve_candidates(
            &db,
            vec![SimilarReleaseCandidate::Local {
                release_db_id: candidate.0,
                release_id: "stale-release-id".to_string(),
            }],
            CandidateResolution {
                seed_library_ids: &HashSet::new(),
                accessible_library_ids: None,
                seed_db_id: seed,
                unique_id_pairs: &HashSet::new(),
            },
            &selected,
            1,
        )?;

        assert!(resolved.is_empty());
        Ok(())
    }

    #[test]
    fn invalid_external_candidate_does_not_drop_valid_batch_entries() {
        let local = SimilarReleaseCandidate::Local {
            release_db_id: 7,
            release_id: "release-7".to_string(),
        };
        let registered = SimilarReleaseCandidate::External(SimilarReleaseExternalRef {
            provider_id: "registered".to_string(),
            id_type: "release_group_id".to_string(),
            id_value: "group-1".to_string(),
        });
        let unregistered = SimilarReleaseCandidate::External(SimilarReleaseExternalRef {
            provider_id: "unregistered".to_string(),
            id_type: "release_id".to_string(),
            id_value: "release-1".to_string(),
        });

        let validated = validate_external_candidates(
            vec![unregistered, local.clone(), registered.clone()],
            &HashSet::from([("registered".to_string(), "release_group_id".to_string())]),
            "source",
        );

        assert_eq!(validated, vec![local, registered]);
    }

    #[tokio::test]
    async fn provider_external_reference_resolves_all_owners_and_excludes_seed() -> Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::env::temp_dir(),
            language: None,
            country: None,
        })
        .await?;
        let plugin = TempSimilarPluginDir::new()?;

        let (seed, same_library) = {
            let mut db = STATE.db.write().await;
            let seed_library = insert_library(&mut db, "Seed", "/seed")?;
            let other_library = insert_library(&mut db, "Other", "/other")?;
            let seed = insert_release(&mut db, "Seed")?;
            let same_library = insert_release(&mut db, "Same Library")?;
            let other = insert_release(&mut db, "Other Library")?;
            connect(&mut db, seed_library, seed)?;
            connect(&mut db, seed_library, same_library)?;
            connect(&mut db, other_library, other)?;
            for release in [seed, same_library, other] {
                db::external_ids::upsert(
                    &mut db,
                    release,
                    "similartest",
                    "release_group_id",
                    "group-1",
                    db::IdSource::Plugin,
                )?;
            }
            (seed, same_library)
        };

        publish_test_runtime(&plugin).await?;

        let releases = similar(
            seed,
            &SimilarReleaseOptions {
                limit: 5,
                accessible_library_ids: None,
            },
        )
        .await?
        .ok_or_else(|| anyhow!("seed missing"))?;
        assert_eq!(releases.len(), 1);
        assert_eq!(
            releases[0].db_id.clone().map(DbId::from),
            Some(same_library)
        );

        STATE.generation().plugin_runtime.replace(None);
        Ok(())
    }

    #[tokio::test]
    async fn timed_out_provider_falls_through_to_lower_priority_provider() -> Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::env::temp_dir(),
            language: None,
            country: None,
        })
        .await?;
        let (seed, candidate, candidate_public_id) = {
            let mut db = STATE.db.write().await;
            let seed = insert_release(&mut db, "Seed")?;
            let candidate = insert_release(&mut db, "Candidate")?;
            let public_id = db::releases::get_by_id(&db, candidate)?
                .ok_or_else(|| anyhow!("candidate missing"))?
                .id;
            (seed, candidate, public_id)
        };
        let plugin = TempSimilarPluginDir::with_source(
            &["lyra.metadata", "harmony.task"],
            &format!(
                r#"
                    local metadata = require("@lyra/metadata")
                    local task = require("@harmony/task")

                    local slow = metadata.Provider.new("slow")
                    slow:similar_releases({{ timeout_ms = 25 }}, function()
                        task.wait(1)
                        return {{ candidates = {{}} }}
                    end)

                    local fallback = metadata.Provider.new("fallback")
                    fallback:similar_releases({{}}, function()
                        return {{
                            candidates = {{{{
                                release_db_id = {},
                                release_id = {:?},
                            }}}},
                        }}
                    end)
                "#,
                candidate.0, candidate_public_id
            ),
        )?;
        let runtime = publish_test_runtime(&plugin).await?;
        {
            let mut db = STATE.db.write().await;
            db::providers::update_priority(&mut db, "slow", 100)?;
            db::providers::update_priority(&mut db, "fallback", 50)?;
        }

        let started = Instant::now();
        let releases = tokio::time::timeout(
            Duration::from_secs(1),
            similar(
                seed,
                &SimilarReleaseOptions {
                    limit: 1,
                    accessible_library_ids: None,
                },
            ),
        )
        .await??
        .ok_or_else(|| anyhow!("seed missing"))?;

        assert!(started.elapsed() < Duration::from_millis(750));
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].db_id.clone().map(DbId::from), Some(candidate));
        assert!(runtime.has_plugin("similartest").await?);
        STATE.generation().plugin_runtime.replace(None);
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_queue_delay_does_not_consume_handler_timeout() -> Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::env::temp_dir(),
            language: None,
            country: None,
        })
        .await?;
        let (seed, candidate, candidate_public_id) = {
            let mut db = STATE.db.write().await;
            let seed = insert_release(&mut db, "Seed")?;
            let candidate = insert_release(&mut db, "Candidate")?;
            let public_id = db::releases::get_by_id(&db, candidate)?
                .ok_or_else(|| anyhow!("candidate missing"))?
                .id;
            (seed, candidate, public_id)
        };
        let plugin = TempSimilarPluginDir::with_source(
            &["lyra.metadata"],
            r#"
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("similartest")
                provider:similar_releases({ timeout_ms = 25 }, function()
                    return { candidates = {} }
                end)
            "#,
        )?;
        publish_test_runtime(&plugin).await?;

        let releases = similar_with_dispatch(
            seed,
            &SimilarReleaseOptions {
                limit: 1,
                accessible_library_ids: None,
            },
            None,
            move |_runtime, request| {
                let candidate_public_id = candidate_public_id.clone();
                async move {
                    assert_eq!(request.timeout, Duration::from_millis(25));
                    tokio::time::sleep(Duration::from_millis(75)).await;
                    Ok(crate::plugins::executor::SimilarReleasesDispatchResult {
                        candidates: vec![SimilarReleaseCandidate::Local {
                            release_db_id: candidate.0,
                            release_id: candidate_public_id,
                        }],
                    })
                }
            },
        )
        .await?
        .ok_or_else(|| anyhow!("seed missing"))?;

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].db_id.clone().map(DbId::from), Some(candidate));
        STATE.generation().plugin_runtime.replace(None);
        Ok(())
    }

    #[tokio::test]
    async fn in_vm_dispatch_is_not_wrapped_in_provider_call_lock() -> Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::env::temp_dir(),
            language: None,
            country: None,
        })
        .await?;
        let (seed, candidate, candidate_public_id) = {
            let mut db = STATE.db.write().await;
            let seed = insert_release(&mut db, "Seed")?;
            let candidate = insert_release(&mut db, "Candidate")?;
            let public_id = db::releases::get_by_id(&db, candidate)?
                .ok_or_else(|| anyhow!("candidate missing"))?
                .id;
            (seed, candidate, public_id)
        };
        let plugin = TempSimilarPluginDir::with_source(
            &["lyra.metadata"],
            r#"
                local metadata = require("@lyra/metadata")
                local provider = metadata.Provider.new("similartest")
                provider:similar_releases({}, function()
                    return { candidates = {} }
                end)
            "#,
        )?;
        let runtime = publish_test_runtime(&plugin).await?;

        let releases = similar_with_dispatch(
            seed,
            &SimilarReleaseOptions {
                limit: 1,
                accessible_library_ids: None,
            },
            Some(runtime.vm_id()),
            move |_runtime, request| {
                let candidate_public_id = candidate_public_id.clone();
                async move {
                    tokio::time::timeout(
                        Duration::from_millis(250),
                        crate::services::providers::with_provider_call(
                            &request.provider_id,
                            crate::services::providers::ProviderCallStage::MetadataRefresh,
                            || async move {
                                Ok(crate::plugins::executor::SimilarReleasesDispatchResult {
                                    candidates: vec![SimilarReleaseCandidate::Local {
                                        release_db_id: candidate.0,
                                        release_id: candidate_public_id,
                                    }],
                                })
                            },
                        ),
                    )
                    .await
                    .context("in-VM dispatch waited on an outer provider call lock")?
                }
            },
        )
        .await?
        .ok_or_else(|| anyhow!("seed missing"))?;

        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].db_id.clone().map(DbId::from), Some(candidate));
        STATE.generation().plugin_runtime.replace(None);
        Ok(())
    }

    #[tokio::test]
    async fn recursive_similar_call_fails_without_blocking_executor() -> Result<()> {
        let _guard = crate::testing::runtime_test_lock().await;
        crate::testing::initialize_runtime(&crate::testing::LibraryFixtureConfig {
            directory: std::env::temp_dir(),
            language: None,
            country: None,
        })
        .await?;
        let seed = {
            let mut db = STATE.db.write().await;
            insert_release(&mut db, "Seed")?
        };
        let plugin = TempSimilarPluginDir::with_source(
            &["lyra.metadata", "lyra.releases"],
            r#"
                local metadata = require("@lyra/metadata")
                local releases = require("@lyra/releases")
                local provider = metadata.Provider.new("similartest")
                provider:similar_releases({}, function(ctx)
                    return releases.similar(ctx.db_id)
                end)
            "#,
        )?;
        let runtime = publish_test_runtime(&plugin).await?;

        let releases = tokio::time::timeout(
            Duration::from_secs(1),
            similar(seed, &SimilarReleaseOptions::default()),
        )
        .await??
        .ok_or_else(|| anyhow!("seed missing"))?;

        assert!(releases.is_empty());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(250),
                runtime.has_plugin("similartest")
            )
            .await??
        );
        STATE.generation().plugin_runtime.replace(None);
        Ok(())
    }
}
