// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;

use crate::services::{
    EntityType,
    STATE,
    covers::{
        CoverPaths,
        CoverSyncOptions,
        artist_cover_wanted,
        sync_artist_cover,
    },
    entities::build_entity_provider_context,
    libraries::{
        SyncRunControlError,
        SyncRunProgress,
        SyncStageKey,
        SyncTotalState,
        SyncWorkDetails,
    },
    providers::{
        IdSchemes,
        ProviderCallError,
        id_schemes,
        provider_registry,
    },
};

use super::{
    PROVIDER_NOT_REGISTERED,
    UnregisteredProviders,
    dispatch::dispatch_refresh,
    read_active_providers,
    refresh_providers_for,
};

/// An artist a cover pass fetches the cover of, as it was listed.
pub(crate) struct ListedArtist {
    pub(super) db_id: DbId,
    pub(super) id: String,
    pub(super) artist_name: String,
}

impl ListedArtist {
    pub(crate) fn list(artists: Vec<lyra_db::Artist>) -> Vec<Self> {
        artists
            .into_iter()
            .filter_map(|artist| {
                Some(Self {
                    db_id: artist.db_id.map(DbId::from)?,
                    id: artist.id,
                    artist_name: artist.artist_name,
                })
            })
            .collect()
    }
}

/// The active providers `provider_filter` allows with an artist refresh handler that also look
/// artist covers up, in provider order: the refreshes whose ids those lookups read. Other
/// providers' artist refreshes are left to the refreshes that run them.
async fn artist_refresher_ids(provider_filter: Option<&str>) -> Vec<String> {
    let providers = match read_active_providers().await {
        Ok(providers) => providers.only(provider_filter),
        Err(err) => {
            tracing::warn!(error = %format_args!("{err:#}"), "could not list the active providers");
            return Vec::new();
        }
    };
    let providers = {
        let registry = provider_registry().read_owned().await;
        providers
            .into_iter()
            .filter(|provider| {
                registry.has_cover_handler(&provider.provider_id, EntityType::Artist)
            })
            .collect::<Vec<_>>()
    };
    refresh_providers_for(&providers, EntityType::Artist).await
}

/// The listed artist as it is now, or none once it is gone or its id went to another node.
async fn current_artist(listed: &ListedArtist) -> anyhow::Result<Option<lyra_db::Artist>> {
    let db = STATE.db.read().await;
    Ok(lyra_db::artists::get_by_id(&db, listed.db_id)?.filter(|artist| artist.id == listed.id))
}

struct ArtistRefreshers {
    provider_ids: Vec<String>,
    unregistered: UnregisteredProviders,
}

enum ArtistCoverOutcome {
    Gone,
    /// A provider was not registered for its refresh and no cover came from the others.
    ProviderNotRegistered,
    Settled {
        got_cover: bool,
    },
}

/// Fetches the artist's cover when an automatic sync would, after its providers refreshed it, so
/// the cover handlers see the ids they settled on. A failing refresh leaves the stored ids to
/// the cover handlers. A cancelled run ends it between handlers or before the fetch, with the
/// cancellation as its error.
async fn refresh_and_sync_artist_cover(
    listed: &ListedArtist,
    refreshers: &ArtistRefreshers,
    schemes: &IdSchemes,
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    progress: &SyncRunProgress,
) -> anyhow::Result<ArtistCoverOutcome> {
    if current_artist(listed).await?.is_none() {
        return Ok(ArtistCoverOutcome::Gone);
    }
    if !artist_cover_wanted(&STATE.db.get(), listed.db_id, &listed.id, options).await? {
        return Ok(ArtistCoverOutcome::Settled { got_cover: false });
    }
    let context = {
        let db = STATE.db.read().await;
        if !crate::services::providers::node_is_entity(&db, listed.db_id, &listed.id)? {
            return Ok(ArtistCoverOutcome::Gone);
        }
        build_entity_provider_context(&db, listed.db_id, None, schemes).map(|(_, context)| context)
    };
    let mut provider_missing = false;
    match context {
        Ok(context) => {
            for provider_id in &refreshers.provider_ids {
                progress.check_cancelled()?;
                match dispatch_refresh(provider_id, EntityType::Artist, context.clone()).await {
                    Ok(()) => {}
                    Err(ProviderCallError::NotRegistered) => {
                        refreshers.unregistered.note(provider_id);
                        provider_missing = true;
                    }
                    Err(ProviderCallError::Failed(err) | ProviderCallError::TargetGone(err)) => {
                        tracing::warn!(
                            provider_id = %provider_id,
                            artist_id = %listed.id,
                            error = %format_args!("{err:#}"),
                            "artist refresh before its cover failed"
                        )
                    }
                }
            }
        }
        Err(err) => tracing::warn!(
            artist_id = %listed.id,
            error = %err,
            "artist context before its cover failed"
        ),
    }
    progress.check_cancelled()?;
    let Some(artist) = current_artist(listed).await? else {
        return Ok(ArtistCoverOutcome::Gone);
    };
    let got_cover =
        sync_artist_cover(&STATE.db.get(), &artist, paths, options, provider_filter).await?;
    if provider_missing && !got_cover {
        return Ok(ArtistCoverOutcome::ProviderNotRegistered);
    }
    Ok(ArtistCoverOutcome::Settled { got_cover })
}

/// Fetches the covers of `artists` from the providers, one after another, reporting one
/// `provider_cover` unit for each, then finalises the run's totals, as the last work to count.
/// Each artist an automatic sync would fetch a cover for is refreshed first by its providers.
/// An artist gone meanwhile is skipped, and one failing leaves the rest to run; only
/// cancellation ends the pass early. Returns how many got a cover.
pub(crate) async fn sync_artist_covers(
    artists: &[ListedArtist],
    paths: CoverPaths<'_>,
    options: CoverSyncOptions,
    provider_filter: Option<&str>,
    progress: &SyncRunProgress,
) -> Result<usize, SyncRunControlError> {
    progress
        .add_stage_total(
            SyncStageKey::ProviderCover,
            artists.len() as u64,
            SyncTotalState::Final,
        )
        .await;
    progress.set_determinate().await;
    let refreshers = ArtistRefreshers {
        provider_ids: artist_refresher_ids(provider_filter).await,
        unregistered: UnregisteredProviders::default(),
    };
    let schemes = id_schemes().await;
    let mut synced = 0;
    for listed in artists {
        progress.check_cancelled()?;
        let details = SyncWorkDetails::artist(
            "artist_provider_cover",
            Some(listed.id.clone()),
            Some(listed.artist_name.clone()),
        );
        let work_id = progress
            .start_work(SyncStageKey::ProviderCover, details.clone())
            .await;
        let result = refresh_and_sync_artist_cover(
            listed,
            &refreshers,
            &schemes,
            paths,
            options,
            provider_filter,
            progress,
        )
        .await;
        if let Err(err) = &result
            && err.downcast_ref::<SyncRunControlError>().is_some()
        {
            return Err(SyncRunControlError::Cancelled);
        }
        match result {
            Ok(ArtistCoverOutcome::Settled { got_cover }) => {
                synced += usize::from(got_cover);
                progress
                    .complete_work(work_id, SyncStageKey::ProviderCover)
                    .await;
            }
            Ok(ArtistCoverOutcome::Gone) => {
                progress
                    .skip_work(work_id, SyncStageKey::ProviderCover, "artist_gone")
                    .await;
            }
            Ok(ArtistCoverOutcome::ProviderNotRegistered) => {
                progress
                    .skip_work(
                        work_id,
                        SyncStageKey::ProviderCover,
                        PROVIDER_NOT_REGISTERED,
                    )
                    .await;
            }
            Err(err) => {
                let error = format!("{err:#}");
                progress
                    .fail_work(work_id, SyncStageKey::ProviderCover, details, error)
                    .await;
            }
        }
    }
    progress.check_cancelled()?;
    Ok(synced)
}
