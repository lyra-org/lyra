// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    path::PathBuf,
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use agdb::DbId;
use anyhow::{
    Context,
    Result,
    anyhow,
};

use crate::{
    plugins::executor::PluginExecutorHandle,
    services::{
        STATE,
        plugin_dispatch::{
            SimilarReleaseCandidate,
            SimilarReleasesDispatchResult,
        },
        releases::{
            SimilarReleaseOptions,
            similar,
            similar_with_dispatch,
        },
    },
};
use lyra_db::fixtures::{
    connect,
    insert_release,
};
use lyra_db::test_db::insert_library;

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
                "lyra": crate::plugins::testing::HOST_REQUIREMENT,
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
    let (runtime, errors) = PluginExecutorHandle::discover_from_plugins_dir_with_db_and_modules(
        plugin.root.clone(),
        server_info,
        auth_capabilities,
        STATE.db.get(),
        Vec::new(),
        Some(STATE.settings.clone()),
    )?;
    assert!(errors.is_empty(), "{errors:?}");
    runtime.exec_plugin("similartest").await?;
    crate::plugins::STATE.publish_runtime(runtime.clone());
    Ok(runtime)
}

#[tokio::test]
async fn provider_external_reference_resolves_all_owners_and_excludes_seed() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
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
            lyra_db::external_ids::upsert(
                &mut db,
                release,
                "similartest",
                "release_group_id",
                "group-1",
                lyra_db::IdSource::Resolved,
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
            viewer: None,
        },
    )
    .await?
    .ok_or_else(|| anyhow!("seed missing"))?;
    assert_eq!(releases.len(), 1);
    assert_eq!(
        releases[0].db_id.clone().map(DbId::from),
        Some(same_library)
    );

    crate::plugins::STATE.clear_runtime();
    Ok(())
}

#[tokio::test]
async fn timed_out_provider_falls_through_to_lower_priority_provider() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
    let (seed, candidate, candidate_public_id) = {
        let mut db = STATE.db.write().await;
        let seed = insert_release(&mut db, "Seed")?;
        let candidate = insert_release(&mut db, "Candidate")?;
        let public_id = lyra_db::releases::get_by_id(&db, candidate)?
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
        lyra_db::providers::move_to(&mut db, "slow", 0)?;
    }

    let started = Instant::now();
    let releases = tokio::time::timeout(
        Duration::from_secs(1),
        similar(
            seed,
            &SimilarReleaseOptions {
                limit: 1,
                accessible_library_ids: None,
                viewer: None,
            },
        ),
    )
    .await??
    .ok_or_else(|| anyhow!("seed missing"))?;

    assert!(started.elapsed() < Duration::from_millis(750));
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].db_id.clone().map(DbId::from), Some(candidate));
    assert!(runtime.has_plugin("similartest").await?);
    crate::plugins::STATE.clear_runtime();
    Ok(())
}

#[tokio::test]
async fn dispatcher_queue_delay_does_not_consume_handler_timeout() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
    let (seed, candidate, candidate_public_id) = {
        let mut db = STATE.db.write().await;
        let seed = insert_release(&mut db, "Seed")?;
        let candidate = insert_release(&mut db, "Candidate")?;
        let public_id = lyra_db::releases::get_by_id(&db, candidate)?
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
            viewer: None,
        },
        move |_runtime, request| {
            let candidate_public_id = candidate_public_id.clone();
            async move {
                assert_eq!(request.timeout, Duration::from_millis(25));
                tokio::time::sleep(Duration::from_millis(75)).await;
                Ok(SimilarReleasesDispatchResult {
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
    crate::plugins::STATE.clear_runtime();
    Ok(())
}

#[tokio::test]
async fn in_vm_dispatch_is_not_wrapped_in_provider_call_lock() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
    let (seed, candidate, candidate_public_id) = {
        let mut db = STATE.db.write().await;
        let seed = insert_release(&mut db, "Seed")?;
        let candidate = insert_release(&mut db, "Candidate")?;
        let public_id = lyra_db::releases::get_by_id(&db, candidate)?
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
    publish_test_runtime(&plugin).await?;

    let releases = similar_with_dispatch(
        seed,
        &SimilarReleaseOptions {
            limit: 1,
            accessible_library_ids: None,
            viewer: None,
        },
        move |_runtime, request| {
            let candidate_public_id = candidate_public_id.clone();
            async move {
                tokio::time::timeout(
                    Duration::from_millis(250),
                    crate::services::providers::with_provider_call(
                        &request.provider_id,
                        crate::services::providers::ProviderCallStage::MetadataRefresh,
                        || async move {
                            Ok(SimilarReleasesDispatchResult {
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
    crate::plugins::STATE.clear_runtime();
    Ok(())
}

#[tokio::test]
async fn recursive_similar_call_fails_without_blocking_executor() -> Result<()> {
    let _guard = crate::services::testing::runtime_test_lock().await;
    crate::plugins::testing::init_library_test_state(std::env::temp_dir()).await?;
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
    crate::plugins::STATE.clear_runtime();
    Ok(())
}
