// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! A user's choice of the cover an artist, release or playlist shows: its stored covers per
//! source, uploading or removing a manual cover, adopting a provider's cover, and pinning a
//! source.

use std::path::{
    Path,
    PathBuf,
};

use agdb::{
    DbAny,
    DbId,
};
use lyra_db::{
    Cover,
    Permission,
    covers::CoverSource,
};

use super::{
    display::{
        CoverFilter,
        playlists::generated_cover_for_playlist,
    },
    providers::{
        EntityType,
        search_artist_cover_candidates,
        search_release_cover_candidates,
    },
    resolve::{
        configured_cover_dir_for_artist,
        configured_cover_dir_for_playlist,
        configured_cover_dir_for_release,
        configured_covers_root,
    },
    sync::{
        NewCoverFile,
        discard_cover_file,
        download_cover,
        store_cover_file,
    },
};
use crate::services::{
    STATE,
    auth::{
        Principal,
        access,
    },
    catalog::Viewer,
    providers::provider_registry,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoverOwnerKind {
    Release,
    Artist,
    Playlist,
}

impl CoverOwnerKind {
    fn label(self) -> &'static str {
        match self {
            Self::Release => "Release",
            Self::Artist => "Artist",
            Self::Playlist => "Playlist",
        }
    }

    fn entity_type(self) -> Option<EntityType> {
        match self {
            Self::Release => Some(EntityType::Release),
            Self::Artist => Some(EntityType::Artist),
            Self::Playlist => None,
        }
    }

    fn cover_dir(self, covers_root: &Path, owner_db_id: DbId) -> PathBuf {
        match self {
            Self::Release => configured_cover_dir_for_release(covers_root, owner_db_id),
            Self::Artist => configured_cover_dir_for_artist(covers_root, owner_db_id),
            Self::Playlist => configured_cover_dir_for_playlist(covers_root, owner_db_id),
        }
    }
}

/// Where a cover candidate comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CandidateSource {
    Stored(CoverSource),
    /// A playlist's display cover, picked from its releases' covers.
    Generated,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverCandidate {
    pub(crate) source: CandidateSource,
    pub(crate) cover: Cover,
    pub(crate) active: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverCandidates {
    pub(crate) candidates: Vec<CoverCandidate>,
    pub(crate) selection: Option<CoverSource>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CoverSelectionError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    Invalid(String),
    #[error("no {} cover is stored", .0.as_str())]
    SourceMissing(CoverSource),
    #[error("{0}")]
    Upstream(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    Read,
    Write,
}

/// The owner `id` names, if the principal may read it or, for `Access::Write`, change its covers:
/// artists and releases with `ManageMetadata` and visibility, playlists as their owner.
fn resolve_owner(
    db: &DbAny,
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
    access: Access,
) -> Result<DbId, CoverSelectionError> {
    let not_found = || CoverSelectionError::NotFound(format!("{} not found: {id}", kind.label()));
    if access == Access::Write
        && kind != CoverOwnerKind::Playlist
        && !lyra_db::roles::has_permission(&principal.permissions, Permission::ManageMetadata)
    {
        return Err(CoverSelectionError::Forbidden(
            "ManageMetadata permission required".to_string(),
        ));
    }
    let owner_db_id = lyra_db::lookup::find_node_id_by_id(db, id)?.ok_or_else(not_found)?;
    match kind {
        CoverOwnerKind::Release | CoverOwnerKind::Artist => {
            access::require_entity_accessible(db, principal, owner_db_id, not_found)?;
            let exists = match kind {
                CoverOwnerKind::Release => lyra_db::releases::get_by_id(db, owner_db_id)?.is_some(),
                _ => lyra_db::artists::get_by_id(db, owner_db_id)?.is_some(),
            };
            if !exists {
                return Err(not_found());
            }
        }
        CoverOwnerKind::Playlist => {
            if lyra_db::playlists::get_by_id(db, owner_db_id)?.is_none() {
                return Err(not_found());
            }
            match access {
                Access::Read => {
                    if !access::playlist_accessible(db, principal, owner_db_id)? {
                        return Err(not_found());
                    }
                }
                Access::Write => {
                    if !access::playlist_owned(db, principal, owner_db_id)? {
                        return Err(CoverSelectionError::Forbidden(
                            "you do not own this playlist".to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(owner_db_id)
}

fn require_selectable(kind: CoverOwnerKind) -> Result<EntityType, CoverSelectionError> {
    kind.entity_type().ok_or_else(|| {
        CoverSelectionError::Invalid(format!(
            "{} covers have no provider source to choose from",
            kind.label().to_ascii_lowercase()
        ))
    })
}

fn collect_candidates(
    db: &DbAny,
    principal: &Principal,
    kind: CoverOwnerKind,
    owner_db_id: DbId,
) -> anyhow::Result<CoverCandidates> {
    let active_id = lyra_db::covers::get(db, owner_db_id)?.map(|cover| cover.id);
    let mut candidates = lyra_db::covers::sources(db, owner_db_id)?
        .into_iter()
        .map(|sourced| CoverCandidate {
            source: CandidateSource::Stored(sourced.source),
            active: active_id.as_deref() == Some(sourced.cover.id.as_str()),
            cover: sourced.cover,
        })
        .collect::<Vec<_>>();
    if kind == CoverOwnerKind::Playlist
        && let Some(playlist) = lyra_db::playlists::get_by_id(db, owner_db_id)?
    {
        let viewer = Viewer::user(db, principal.clone())?;
        if let Some(generated) =
            generated_cover_for_playlist(db, &mut CoverFilter::new(&viewer), &playlist)?
        {
            candidates.push(CoverCandidate {
                source: CandidateSource::Generated,
                active: active_id.is_none(),
                cover: generated.cover,
            });
        }
    }
    Ok(CoverCandidates {
        candidates,
        selection: lyra_db::covers::selection(db, owner_db_id)?,
    })
}

async fn current_candidates(
    principal: &Principal,
    kind: CoverOwnerKind,
    owner_db_id: DbId,
) -> Result<CoverCandidates, CoverSelectionError> {
    let db = STATE.db.read().await;
    Ok(collect_candidates(&db, principal, kind, owner_db_id)?)
}

/// The owner's stored covers per source, which one it shows, and the source it pinned.
pub(crate) async fn candidates(
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
) -> Result<CoverCandidates, CoverSelectionError> {
    let db = STATE.db.read().await;
    let owner_db_id = resolve_owner(&db, principal, kind, id, Access::Read)?;
    Ok(collect_candidates(&db, principal, kind, owner_db_id)?)
}

/// Stores `bytes`, which must decode as a JPEG, PNG or WebP image, as the owner's manual cover
/// and, for artists and releases, pins it.
pub(crate) async fn set_manual(
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
    bytes: Vec<u8>,
) -> Result<CoverCandidates, CoverSelectionError> {
    let owner_db_id = {
        let db = STATE.db.read().await;
        resolve_owner(&db, principal, kind, id, Access::Write)?
    };
    let (bytes, ext) = tokio::task::spawn_blocking(move || {
        super::image::validate_cover_image(&bytes).map(|ext| (bytes, ext))
    })
    .await
    .map_err(|err| anyhow::anyhow!("cover validation task failed: {err}"))?
    .map_err(|err| CoverSelectionError::Invalid(format!("{err:#}")))?;

    let covers_root = configured_covers_root();
    store_cover_file(
        &STATE.db.get(),
        &covers_root,
        NewCoverFile {
            owner_id: owner_db_id,
            source: CoverSource::Manual,
            dir: &kind.cover_dir(&covers_root, owner_db_id),
            ext,
            bytes: &bytes,
            provider_id: None,
            select: kind != CoverOwnerKind::Playlist,
        },
    )
    .await?;
    current_candidates(principal, kind, owner_db_id).await
}

/// Removes the owner's manual cover, and the pin with it when it pointed there.
pub(crate) async fn remove_manual(
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
) -> Result<CoverCandidates, CoverSelectionError> {
    let (owner_db_id, detached) = {
        let mut db = STATE.db.write().await;
        let owner_db_id = resolve_owner(&db, principal, kind, id, Access::Write)?;
        let detached =
            db.transaction_mut(|t| lyra_db::covers::remove(t, owner_db_id, CoverSource::Manual))?;
        (owner_db_id, detached)
    };
    let detached = detached
        .ok_or_else(|| CoverSelectionError::NotFound("no manual cover is stored".to_string()))?;
    if detached.orphaned {
        discard_cover_file(&configured_covers_root(), &detached.cover.path);
    }
    current_candidates(principal, kind, owner_db_id).await
}

/// Downloads `url` as the owner's provider cover and pins it. The URL must be one of the
/// provider's current cover candidates for the owner, so a caller can only adopt images the
/// provider's own cover handler offers.
pub(crate) async fn set_provider(
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
    provider_id: &str,
    url: &str,
) -> Result<CoverCandidates, CoverSelectionError> {
    let entity_type = require_selectable(kind)?;
    let owner_db_id = {
        let db = STATE.db.read().await;
        resolve_owner(&db, principal, kind, id, Access::Write)?
    };
    if provider_registry()
        .read()
        .await
        .cover_handler_owner(provider_id, entity_type)
        .is_none()
    {
        return Err(CoverSelectionError::Invalid(format!(
            "provider '{provider_id}' has no {} cover handler",
            entity_type.as_str()
        )));
    }

    let found = match kind {
        CoverOwnerKind::Release => {
            search_release_cover_candidates(id, Some(provider_id), false).await?
        }
        _ => search_artist_cover_candidates(id, Some(provider_id), false).await?,
    };
    let offered = found
        .iter()
        .filter(|result| result.provider_id == provider_id)
        .flat_map(|result| &result.candidates)
        .any(|candidate| candidate.url == url);
    if !offered {
        return Err(CoverSelectionError::Invalid(format!(
            "the URL is not one of provider '{provider_id}''s cover candidates for this {}",
            entity_type.as_str()
        )));
    }

    let (bytes, ext) = download_cover(provider_id, entity_type, url)
        .await
        .map_err(|err| CoverSelectionError::Upstream(format!("{err:#}")))?;
    let covers_root = configured_covers_root();
    store_cover_file(
        &STATE.db.get(),
        &covers_root,
        NewCoverFile {
            owner_id: owner_db_id,
            source: CoverSource::Provider,
            dir: &kind.cover_dir(&covers_root, owner_db_id),
            ext,
            bytes: &bytes,
            provider_id: Some(provider_id),
            select: true,
        },
    )
    .await?;
    current_candidates(principal, kind, owner_db_id).await
}

/// Pins the stored `source` as the cover the owner shows, or with `None` returns it to the
/// automatic precedence: manual, then provider, then local.
pub(crate) async fn select(
    principal: &Principal,
    kind: CoverOwnerKind,
    id: &str,
    source: Option<CoverSource>,
) -> Result<CoverCandidates, CoverSelectionError> {
    require_selectable(kind)?;
    let owner_db_id = {
        let mut db = STATE.db.write().await;
        let owner_db_id = resolve_owner(&db, principal, kind, id, Access::Write)?;
        let selected = db.transaction_mut(|t| lyra_db::covers::select(t, owner_db_id, source))?;
        if !selected {
            return Err(CoverSelectionError::SourceMissing(
                source.expect("clearing the pin always succeeds"),
            ));
        }
        owner_db_id
    };
    current_candidates(principal, kind, owner_db_id).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::services::{
        plugin_scope::PluginId,
        providers::ProviderCoverSpec,
        testing::{
            FakePluginDispatch,
            FakeResponse,
            init_default_test_state,
            runtime_test_lock,
            user_principal,
        },
    };
    use lyra_db::fixtures::{
        insert_release,
        test_user,
    };

    /// A 1x1 PNG that decodes.
    fn png() -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        ::image::RgbImage::new(1, 1)
            .write_to(&mut bytes, ::image::ImageFormat::Png)
            .expect("encode png");
        bytes.into_inner()
    }

    async fn user(name: &str, admin: bool) -> anyhow::Result<Principal> {
        let user_db_id = {
            let mut db = STATE.db.write().await;
            lyra_db::roles::ensure_builtin_roles(&mut db)?;
            let user_db_id = lyra_db::users::create(&mut db, &test_user(name)?)?;
            if admin {
                lyra_db::roles::ensure_user_has_role(
                    &mut db,
                    user_db_id,
                    lyra_db::roles::BUILTIN_ADMIN_ROLE,
                )?;
            }
            user_db_id
        };
        user_principal(user_db_id).await
    }

    /// A release with a cover beside its audio files in `library`.
    async fn release_with_local_cover(library: &Path) -> anyhow::Result<(DbId, String, PathBuf)> {
        let release_db_id = insert_release(&mut *STATE.db.write().await, "Release")?;
        let public_id = lyra_db::releases::get_by_id(&*STATE.db.read().await, release_db_id)?
            .expect("release exists")
            .id;
        let library_cover = library.join("Release").join("cover.png");
        std::fs::create_dir_all(library_cover.parent().expect("parent"))?;
        std::fs::write(&library_cover, png())?;
        super::super::sync::upsert_local_cover_metadata(
            &STATE.db.get(),
            release_db_id,
            &library_cover,
        )
        .await?;
        Ok((release_db_id, public_id, library_cover))
    }

    /// Stores a provider cover the way an automatic provider sync does.
    async fn store_provider_cover(release_db_id: DbId) -> anyhow::Result<Cover> {
        let covers_root = configured_covers_root();
        store_cover_file(
            &STATE.db.get(),
            &covers_root,
            NewCoverFile {
                owner_id: release_db_id,
                source: CoverSource::Provider,
                dir: &configured_cover_dir_for_release(&covers_root, release_db_id),
                ext: "png",
                bytes: &png(),
                provider_id: Some("provider"),
                select: false,
            },
        )
        .await
    }

    fn active(found: &CoverCandidates) -> Option<CandidateSource> {
        found
            .candidates
            .iter()
            .find(|candidate| candidate.active)
            .map(|candidate| candidate.source)
    }

    fn stored(found: &CoverCandidates, source: CoverSource) -> Option<&Cover> {
        found
            .candidates
            .iter()
            .find(|candidate| candidate.source == CandidateSource::Stored(source))
            .map(|candidate| &candidate.cover)
    }

    #[tokio::test]
    async fn switching_to_a_provider_cover_and_back_keeps_every_cover() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = tempfile::tempdir()?;
        let admin = user("admin", true).await?;
        let (release_db_id, id, library_cover) = release_with_local_cover(library.path()).await?;
        let found = candidates(&admin, CoverOwnerKind::Release, &id).await?;
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Local))
        );

        let provider = store_provider_cover(release_db_id).await?;
        let found = select(
            &admin,
            CoverOwnerKind::Release,
            &id,
            Some(CoverSource::Provider),
        )
        .await?;
        assert_eq!(found.selection, Some(CoverSource::Provider));
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Provider))
        );
        assert_eq!(
            stored(&found, CoverSource::Provider).map(|cover| cover.provider_id.as_deref()),
            Some(Some("provider"))
        );

        let found = select(
            &admin,
            CoverOwnerKind::Release,
            &id,
            Some(CoverSource::Local),
        )
        .await?;
        assert_eq!(found.selection, Some(CoverSource::Local));
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Local))
        );
        let local = stored(&found, CoverSource::Local).expect("the local cover is kept");
        assert_eq!(PathBuf::from(&local.path), library_cover);
        assert_eq!(
            stored(&found, CoverSource::Provider).map(|cover| cover.path.clone()),
            Some(provider.path.clone()),
            "the provider cover is kept"
        );
        assert_eq!(std::fs::read(&library_cover)?, png());
        assert!(Path::new(&provider.path).is_file());
        assert_eq!(
            lyra_db::covers::get(&*STATE.db.read().await, release_db_id)?.map(|cover| cover.path),
            Some(library_cover.to_string_lossy().into_owned()),
            "inc=covers shows the pinned cover"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_manual_cover_wins_until_removed() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = tempfile::tempdir()?;
        let admin = user("admin", true).await?;
        let (release_db_id, id, library_cover) = release_with_local_cover(library.path()).await?;
        store_provider_cover(release_db_id).await?;
        let found = candidates(&admin, CoverOwnerKind::Release, &id).await?;
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Provider)),
            "provider covers come before local ones"
        );

        let found = set_manual(&admin, CoverOwnerKind::Release, &id, png()).await?;
        assert_eq!(found.selection, Some(CoverSource::Manual));
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Manual))
        );
        let first_manual = stored(&found, CoverSource::Manual)
            .expect("manual cover")
            .path
            .clone();
        assert!(first_manual.starts_with(&*configured_covers_root().to_string_lossy()));

        let found = set_manual(&admin, CoverOwnerKind::Release, &id, png()).await?;
        let second_manual = stored(&found, CoverSource::Manual)
            .expect("manual cover")
            .path
            .clone();
        assert!(
            !Path::new(&first_manual).exists(),
            "the replaced upload is discarded"
        );
        assert!(Path::new(&second_manual).is_file());

        let found = remove_manual(&admin, CoverOwnerKind::Release, &id).await?;
        assert_eq!(found.selection, None);
        assert!(stored(&found, CoverSource::Manual).is_none());
        assert!(!Path::new(&second_manual).exists());
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Provider))
        );
        assert!(library_cover.is_file());

        let error = remove_manual(&admin, CoverOwnerKind::Release, &id)
            .await
            .expect_err("nothing left to remove");
        assert!(matches!(error, CoverSelectionError::NotFound(_)), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn uploads_and_pins_are_checked() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let library = tempfile::tempdir()?;
        let admin = user("admin", true).await?;
        let listener = user("listener", false).await?;
        let (_, id, _) = release_with_local_cover(library.path()).await?;

        let error = set_manual(
            &admin,
            CoverOwnerKind::Release,
            &id,
            b"not an image".to_vec(),
        )
        .await
        .expect_err("bytes that do not decode are refused");
        assert!(matches!(error, CoverSelectionError::Invalid(_)), "{error}");

        let error = select(
            &admin,
            CoverOwnerKind::Release,
            &id,
            Some(CoverSource::Provider),
        )
        .await
        .expect_err("no provider cover is stored");
        assert!(
            matches!(
                error,
                CoverSelectionError::SourceMissing(CoverSource::Provider)
            ),
            "{error}"
        );

        let error = set_manual(&listener, CoverOwnerKind::Release, &id, png())
            .await
            .expect_err("changing a release cover needs ManageMetadata");
        assert!(
            matches!(error, CoverSelectionError::Forbidden(_)),
            "{error}"
        );
        let error = select(&listener, CoverOwnerKind::Release, &id, None)
            .await
            .expect_err("clearing a pin needs ManageMetadata");
        assert!(
            matches!(error, CoverSelectionError::Forbidden(_)),
            "{error}"
        );
        let error = candidates(&listener, CoverOwnerKind::Release, &id)
            .await
            .expect_err("the release is in no library the listener sees");
        assert!(matches!(error, CoverSelectionError::NotFound(_)), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn only_a_playlists_owner_changes_its_cover() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let owner = user("owner", false).await?;
        let admin = user("admin", true).await?;
        let owner_db_id = owner.require(&*STATE.db.read().await)?;
        let playlist_db_id = lyra_db::playlists::create(
            &mut *STATE.db.write().await,
            &lyra_db::Playlist {
                db_id: None,
                id: nanoid::nanoid!(),
                name: "Mix".to_string(),
                description: None,
                is_public: Some(true),
                created_at_ms: None,
                updated_at_ms: None,
            },
            owner_db_id,
        )?;
        let id = lyra_db::playlists::get_by_id(&*STATE.db.read().await, playlist_db_id)?
            .expect("playlist exists")
            .id;

        let error = set_manual(&admin, CoverOwnerKind::Playlist, &id, png())
            .await
            .expect_err("an admin does not own the playlist");
        assert!(
            matches!(error, CoverSelectionError::Forbidden(_)),
            "{error}"
        );

        let found = set_manual(&owner, CoverOwnerKind::Playlist, &id, png()).await?;
        assert_eq!(found.selection, None, "playlists have no pin");
        assert_eq!(
            active(&found),
            Some(CandidateSource::Stored(CoverSource::Manual))
        );
        let path = stored(&found, CoverSource::Manual)
            .expect("manual cover")
            .path
            .clone();
        assert!(path.contains("playlists"));
        assert!(
            candidates(&admin, CoverOwnerKind::Playlist, &id)
                .await
                .is_ok()
        );

        let error = select(&owner, CoverOwnerKind::Playlist, &id, None)
            .await
            .expect_err("playlists have no provider source to choose from");
        assert!(matches!(error, CoverSelectionError::Invalid(_)), "{error}");

        crate::services::playlists::delete(
            &mut *STATE.db.write().await,
            agdb::QueryId::Id(playlist_db_id),
        )?;
        assert!(
            !Path::new(&path).exists(),
            "deleting the playlist discards its upload"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_provider_url_must_be_one_of_its_candidates() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let dispatch = FakePluginDispatch::install();
        let library = tempfile::tempdir()?;
        let admin = user("admin", true).await?;
        let (release_db_id, id, _) = release_with_local_cover(library.path()).await?;

        let error = set_provider(
            &admin,
            CoverOwnerKind::Release,
            &id,
            "provider",
            "http://127.0.0.1:9/cover.png",
        )
        .await
        .expect_err("the provider has no cover handler");
        assert!(matches!(error, CoverSelectionError::Invalid(_)), "{error}");

        let callback = dispatch.add(FakeResponse::Value(serde_json::json!({
            "candidates": [{ "url": "http://127.0.0.1:9/offered.png" }],
            "selected_index": 1,
        })));
        lyra_db::providers::register(&mut *STATE.db.write().await, "provider")?;
        {
            let registry = provider_registry();
            let mut registry = registry.write().await;
            registry.register(
                PluginId::new("test_plugin").expect("valid plugin id"),
                "provider".to_string(),
                None,
            )?;
            registry.set_cover_handler(
                "provider",
                EntityType::Release,
                ProviderCoverSpec {
                    timeout: Duration::from_secs(5),
                    require: Default::default(),
                    handler: callback,
                },
            );
        }

        let error = set_provider(
            &admin,
            CoverOwnerKind::Release,
            &id,
            "provider",
            "http://127.0.0.1:9/elsewhere.png",
        )
        .await
        .expect_err("the URL is not a candidate");
        assert!(error.to_string().contains("not one of provider"), "{error}");
        assert_eq!(dispatch.calls(callback).len(), 1);
        assert!(
            lyra_db::covers::get_source(
                &*STATE.db.read().await,
                release_db_id,
                CoverSource::Provider
            )?
            .is_none()
        );
        Ok(())
    }
}
