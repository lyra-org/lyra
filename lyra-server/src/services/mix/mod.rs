// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbAny,
    DbId,
};

use crate::services::STATE;
use crate::services::auth::{
    Principal,
    access::PlaylistView,
};
use lyra_db::{
    Permission,
    Track,
    chains::ChainLink,
};

mod admin;
mod builtin;
mod dispatch;
mod registry;
mod request;

pub(crate) use admin::{
    MixerAdminError,
    MixerUpdate,
    MixersListing,
    list_mixers,
    update_mixer,
};
pub(crate) use registry::{
    MixHandlerRegistration,
    MixHandlerSpec,
    MixRegistries,
    MixRequirements,
    MixSeedType,
    MixShape,
    mix_registry,
    teardown_plugin_mixers,
};
use request::ResolvedSeeds;
pub(crate) use request::{
    MAX_QUERY_LEN,
    MAX_SEEDS,
    MixError,
    MixRequest,
    MixRequestFields,
    MixSeed,
};

const DEFAULT_LIMIT: usize = 200;
pub(crate) const MAX_LIMIT: usize = 2000;
/// Per-request budget across all plugin handlers, and so the most one handler may take.
pub(crate) const MIX_DISPATCH_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);
/// A handler's timeout unless its spec sets one.
pub(crate) const DEFAULT_MIXER_HANDLER_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

/// Who a mix is for.
#[derive(Clone, Debug, Default)]
pub(crate) struct MixViewer {
    /// Every seed's access check goes by this user, as do the history seeds and the built-in
    /// mixer's unheard-first order.
    pub(crate) principal: Option<Principal>,
    /// The libraries the user can see, or `None` for every one. History seeds, built-in
    /// candidates and mixer results keep to them.
    pub(crate) accessible_library_ids: Option<HashSet<String>>,
    /// Which playlists the user can seed from.
    pub(crate) playlists: PlaylistView,
}

impl MixViewer {
    pub(crate) fn for_principal(principal: &Principal) -> Self {
        Self {
            principal: Some(principal.clone()),
            accessible_library_ids: (!principal.permissions.contains(&Permission::Admin))
                .then(|| principal.accessible_library_ids.clone()),
            playlists: PlaylistView::Role,
        }
    }

    fn principal(&self) -> anyhow::Result<&Principal> {
        self.principal
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("mixes need a viewer"))
    }

    /// The viewer's `DbId`, verified under `db`'s guard.
    fn require(&self, db: &DbAny) -> anyhow::Result<DbId> {
        Ok(self.principal()?.require(db)?)
    }
}

/// A mixed track, with the score the mixer gave it.
#[derive(Clone, Debug)]
pub(crate) struct MixedTrack {
    pub(crate) track: Track,
    /// Relevance to the request in `0..=1`, higher being more relevant, comparable only within one
    /// mix. `None` when the mixer gave none, for a pinned seed the mixer didn't return, and for
    /// tracks the built-in mixer chose.
    pub(crate) score: Option<f64>,
}

/// Mixes `request` for `viewer`. Seeds are resolved by public id under each guard taken, so a
/// dispatch that outlives a seed's `DbId` fails with [`MixError::SeedNotFound`] as a missing or
/// hidden seed does.
///
/// Blends keep each chain together, truncate to the limit and, with `pin_seed`, lead with the
/// seed. Paths keep the mixer's order and length up to [`MAX_LIMIT`] tracks, dropping only the
/// tracks the viewer can't see.
pub(crate) async fn mix(
    request: &MixRequest,
    viewer: &MixViewer,
) -> Result<Vec<MixedTrack>, MixError> {
    request.validate()?;
    let resolved = {
        let db = STATE.db.read().await;
        ResolvedSeeds::resolve(&db, request, viewer)?
    };
    let requirements = request::requirements(request, &resolved.explicit);
    let builtin = (request.mixer.is_none() && request.shape == MixShape::Blend)
        .then(builtin::spec)
        .filter(|spec| spec.covers(&requirements));
    let choice = dispatch::choose_mixers(request, &requirements).await?;
    if choice.candidates.is_empty() && builtin.is_none() {
        return Err(dispatch::unsupported(request, &requirements, &choice));
    }
    if resolved.is_empty() && request.query().is_none() {
        return Ok(Vec::new());
    }

    let dispatched = dispatch::dispatch(&choice.candidates, request, &resolved, viewer).await?;
    let db = STATE.db.read().await;
    resolved.verify(&db, request, viewer)?;
    let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
    let (tracks, scores) = match dispatched {
        Some(dispatched) => (
            filter_tracks_by_library_visibility(
                &db,
                filter_existing_tracks(&db, dispatched.tracks)?,
                viewer.accessible_library_ids.as_ref(),
            )?,
            dispatched.scores,
        ),
        None if builtin.is_some() => (builtin::mix(&db, &resolved, viewer, limit)?, HashMap::new()),
        None => (Vec::new(), HashMap::new()),
    };
    let tracks = if request.shape == MixShape::Path {
        tracks
    } else {
        let pinned = request.pin_seed.then(|| resolved.explicit[0].db_id);
        let links = chain_links(&db, &tracks, pinned)?;
        let tracks = keep_chains_together(tracks, &links);
        match pinned {
            Some(pinned) => {
                let seed = lyra_db::tracks::get_by_id(&db, pinned)?
                    .ok_or_else(|| MixError::SeedNotFound(request.seeds[0].id.clone()))?;
                pin_seed_and_truncate(seed, tracks, &links, limit)
            }
            None => tracks,
        }
    };
    Ok(with_scores(tracks, &scores))
}

fn with_scores(tracks: Vec<Track>, scores: &HashMap<String, f64>) -> Vec<MixedTrack> {
    tracks
        .into_iter()
        .map(|track| MixedTrack {
            score: scores.get(&track.id).copied(),
            track,
        })
        .collect()
}

/// Drops missing or wrong-type tracks, keeping the rest in provider order for the chain
/// regrouping that follows.
fn filter_existing_tracks(db: &DbAny, tracks: Vec<Track>) -> anyhow::Result<Vec<Track>> {
    if tracks.is_empty() {
        return Ok(tracks);
    }
    let track_ids: Vec<DbId> = tracks
        .iter()
        .filter_map(|t| t.db_id.clone().map(DbId::from))
        .collect();
    if track_ids.is_empty() {
        return Ok(Vec::new());
    }
    let existing = lyra_db::tracks::get_existing_by_ids(db, &track_ids)?;
    Ok(tracks
        .into_iter()
        .filter_map(|track| {
            let track_db_id = track.db_id.clone().map(DbId::from)?;
            existing
                .get(&track_db_id)
                .filter(|current| current.id == track.id)
                .cloned()
        })
        .collect())
}

/// Applies server-owned mixer result policy while preserving provider order. Blends truncate to
/// the limit; paths keep every visible step of the handler's at most [`MAX_LIMIT`].
fn finalize_mixer_tracks(
    db: &DbAny,
    tracks: Vec<Track>,
    shape: MixShape,
    viewer: &MixViewer,
    limit: usize,
) -> anyhow::Result<Vec<Track>> {
    let tracks = filter_existing_tracks(db, tracks)?;
    let mut tracks =
        filter_tracks_by_library_visibility(db, tracks, viewer.accessible_library_ids.as_ref())?;
    if shape == MixShape::Blend {
        tracks.truncate(limit);
    }
    Ok(tracks)
}

fn filter_tracks_by_library_visibility(
    db: &DbAny,
    mut tracks: Vec<Track>,
    accessible_library_ids: Option<&HashSet<String>>,
) -> anyhow::Result<Vec<Track>> {
    let Some(accessible_library_ids) = accessible_library_ids else {
        return Ok(tracks);
    };
    let track_ids = tracks
        .iter()
        .filter_map(|track| track.db_id.clone().map(DbId::from))
        .collect::<Vec<_>>();
    let library_db_ids = lyra_db::libraries::db_ids_for_public_ids(db, accessible_library_ids)?;
    let accessible_track_ids = lyra_db::libraries::accessible_track_ids_for_library_db_ids(
        db,
        &library_db_ids,
        &track_ids,
    )?;
    tracks.retain(|track| {
        track
            .db_id
            .clone()
            .map(DbId::from)
            .is_some_and(|id| accessible_track_ids.contains(&id))
    });
    Ok(tracks)
}

/// The chain place of each chained track among `tracks` and `extra`.
fn chain_links(
    db: &DbAny,
    tracks: &[Track],
    extra: Option<DbId>,
) -> anyhow::Result<HashMap<DbId, ChainLink>> {
    let track_ids = tracks
        .iter()
        .filter_map(|track| track.db_id.clone().map(DbId::from))
        .chain(extra)
        .collect::<Vec<_>>();
    lyra_db::chains::links_for_tracks(db, &track_ids)
}

/// Gathers the chained tracks of `tracks` where their chain's first one sits, in chain order.
/// Members the mix left out stay out.
fn keep_chains_together(tracks: Vec<Track>, links: &HashMap<DbId, ChainLink>) -> Vec<Track> {
    if links.is_empty() {
        return tracks;
    }

    let mut chain_starts = HashMap::new();
    let mut keyed = tracks
        .into_iter()
        .enumerate()
        .map(|(index, track)| {
            let link = track
                .db_id
                .clone()
                .map(DbId::from)
                .and_then(|track_db_id| links.get(&track_db_id));
            let key = match link {
                Some(link) => (
                    *chain_starts.entry(link.chain_db_id).or_insert(index),
                    link.position,
                ),
                None => (index, 0),
            };
            (key, track)
        })
        .collect::<Vec<_>>();
    keyed.sort_by_key(|(key, _)| *key);
    keyed.into_iter().map(|(_, track)| track).collect()
}

/// Pins `seed` at index 0 (moves if already in `tracks`), then truncates. The seed's later chain
/// members in `tracks` follow it in chain order; its earlier ones are dropped, as playing them
/// after the seed would reverse the chain.
fn pin_seed_and_truncate(
    seed: Track,
    tracks: Vec<Track>,
    links: &HashMap<DbId, ChainLink>,
    limit: usize,
) -> Vec<Track> {
    let seed_db_id = seed.db_id.clone().map(DbId::from);
    let seed_link = seed_db_id.and_then(|seed_db_id| links.get(&seed_db_id));
    let mut followers = Vec::new();
    let mut rest = Vec::with_capacity(tracks.len());
    for track in tracks {
        let track_db_id = track.db_id.clone().map(DbId::from);
        if track_db_id.is_some() && track_db_id == seed_db_id {
            continue;
        }
        let link = track_db_id.and_then(|track_db_id| links.get(&track_db_id));
        match (seed_link, link) {
            (Some(seed_link), Some(link)) if link.chain_db_id == seed_link.chain_db_id => {
                if link.position > seed_link.position {
                    followers.push((link.position, track));
                }
            }
            _ => rest.push(track),
        }
    }
    followers.sort_by_key(|(position, _)| *position);

    let mut pinned = Vec::with_capacity(1 + followers.len() + rest.len());
    pinned.push(seed);
    pinned.extend(followers.into_iter().map(|(_, track)| track));
    pinned.extend(rest);
    pinned.truncate(limit);
    pinned
}

#[cfg(test)]
mod tests {
    use agdb::QueryBuilder;

    use super::*;
    use lyra_db::{
        fixtures::{
            connect,
            insert_release,
            insert_track,
        },
        genres,
        test_db::{
            insert_library,
            new_test_db,
        },
    };

    #[test]
    fn viewers_keep_to_their_libraries_unless_admin() {
        let principal = |permissions| {
            Principal::from_parts(
                DbId(7),
                "viewer".to_string(),
                "viewer".to_string(),
                permissions,
                None,
                HashSet::from(["visible-library".to_string()]),
            )
        };
        let viewer = MixViewer::for_principal(&principal(Vec::new()));
        assert_eq!(
            viewer.principal.map(|viewer| viewer.user_public_id),
            Some("viewer".to_string())
        );
        assert_eq!(
            viewer.accessible_library_ids,
            Some(HashSet::from(["visible-library".to_string()]))
        );
        let admin = MixViewer::for_principal(&principal(vec![Permission::Admin]));
        assert!(admin.principal.is_some());
        assert_eq!(admin.accessible_library_ids, None);
    }

    /// A blend of the seed with public id `seed`.
    fn blend_of(seed: String) -> MixRequest {
        MixRequest {
            seeds: vec![request::MixSeedRequest::new(seed)],
            ..MixRequest::default()
        }
    }

    /// Mixes from a track seed while, at gap `gap`, the seed is deleted and a new track takes
    /// its `DbId`. Returns whether the gap ran and whether a mix came back.
    async fn mix_with_recycled_seed(gap: Option<usize>) -> anyhow::Result<(bool, bool)> {
        crate::services::testing::init_default_test_state()?;
        let (viewer, seed_db_id, seed_id, library) = {
            let mut db = STATE.db.write().await;
            let viewer_id = lyra_db::test_db::insert_user(&mut db, "mix-recycle-viewer")?;
            let library = insert_library(&mut db, "Recycle Mix", "/tmp/lyra-recycle-mix")?;
            let seed = insert_track(&mut db, "Seed")?;
            connect(&mut db, library, seed)?;
            let seed_id = lyra_db::lookup::find_id_by_db_id(&*db, seed)?.expect("seed public id");
            (
                Principal::for_user(&*db, viewer_id, vec![Permission::Admin], HashSet::new()),
                seed,
                seed_id,
                library,
            )
        };
        let viewer = MixViewer::for_principal(&viewer);
        let request = blend_of(seed_id);
        let (reached, mix) =
            crate::services::testing::run_with_recycle_at(gap, mix(&request, &viewer), |db| {
                db.exec_mut(QueryBuilder::remove().ids(seed_db_id).query())
                    .expect("remove seed");
                let recycled = insert_track(db, "Replacement").expect("insert replacement");
                assert_eq!(recycled, seed_db_id, "agdb must reuse the seed DbId");
                connect(db, library, recycled).expect("connect replacement");
            })
            .await;
        match mix {
            Ok(_) => Ok((reached, true)),
            Err(MixError::SeedNotFound(_)) => Ok((reached, false)),
            Err(error) => Err(error.into()),
        }
    }

    #[tokio::test]
    async fn mixes_drop_a_seed_whose_id_was_recycled_mid_mix() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        assert_eq!(mix_with_recycled_seed(None).await?, (false, true));
        crate::services::testing::for_each_db_gap(mix_with_recycled_seed, async |gap, mixed| {
            assert!(!mixed, "mixed a recycled seed at gap {gap}");
            Ok(())
        })
        .await
    }

    // --- pin_seed_and_truncate ---

    #[test]
    fn plugin_candidates_filter_before_limit_and_preserve_instant_pin() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let visible_library = insert_library(&mut db, "Visible", "/music/visible-plugin-mix")?;
        let hidden_library = insert_library(&mut db, "Hidden", "/music/hidden-plugin-mix")?;
        let visible_library_public_id = lyra_db::libraries::get_by_id(&db, visible_library)?
            .expect("visible library exists")
            .id;
        let visible_release = insert_release(&mut db, "Visible Release")?;
        let hidden_release = insert_release(&mut db, "Hidden Release")?;
        connect(&mut db, visible_library, visible_release)?;
        connect(&mut db, hidden_library, hidden_release)?;

        let hidden_a = insert_track(&mut db, "Hidden A")?;
        let hidden_b = insert_track(&mut db, "Hidden B")?;
        let visible_a = insert_track(&mut db, "Visible A")?;
        let visible_b = insert_track(&mut db, "Visible B")?;
        let seed_id = insert_track(&mut db, "Visible Seed")?;
        connect(&mut db, hidden_release, hidden_a)?;
        connect(&mut db, hidden_release, hidden_b)?;
        connect(&mut db, visible_release, visible_a)?;
        connect(&mut db, visible_release, visible_b)?;
        connect(&mut db, visible_release, seed_id)?;

        let replaced_id = insert_track(&mut db, "Replaced Candidate")?;
        connect(&mut db, visible_release, replaced_id)?;
        let replaced = lyra_db::tracks::get_by_id(&db, replaced_id)?.expect("candidate exists");
        let mut replacement = replaced.clone();
        replacement.id = nanoid::nanoid!();
        replacement.track_title = "Replacement Track".to_string();
        lyra_db::tracks::update(&mut db, &replacement)?;

        let missing_id = insert_track(&mut db, "Deleted Candidate")?;
        let missing = lyra_db::tracks::get_by_id(&db, missing_id)?.expect("candidate exists");
        db.exec_mut(QueryBuilder::remove().ids(missing_id).query())?;
        let hidden_a = lyra_db::tracks::get_by_id(&db, hidden_a)?.expect("hidden A exists");
        let hidden_b = lyra_db::tracks::get_by_id(&db, hidden_b)?.expect("hidden B exists");
        let visible_a = lyra_db::tracks::get_by_id(&db, visible_a)?.expect("visible A exists");
        let visible_b = lyra_db::tracks::get_by_id(&db, visible_b)?.expect("visible B exists");
        let seed = lyra_db::tracks::get_by_id(&db, seed_id)?.expect("seed exists");
        let provider_candidates = vec![
            hidden_a.clone(),
            missing.clone(),
            hidden_b.clone(),
            replaced,
            visible_b.clone(),
            visible_a.clone(),
            seed.clone(),
        ];

        let restricted = finalize_mixer_tracks(
            &db,
            provider_candidates.clone(),
            MixShape::Blend,
            &MixViewer {
                accessible_library_ids: Some(HashSet::from([visible_library_public_id.clone()])),
                ..MixViewer::default()
            },
            2,
        )?;
        assert_eq!(
            restricted
                .iter()
                .map(|track| track.track_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Visible B", "Visible A"],
        );

        let unrestricted = finalize_mixer_tracks(
            &db,
            provider_candidates.clone(),
            MixShape::Blend,
            &MixViewer::default(),
            2,
        )?;
        assert_eq!(
            unrestricted
                .iter()
                .map(|track| track.track_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Hidden A", "Hidden B"],
        );

        let path = finalize_mixer_tracks(
            &db,
            provider_candidates,
            MixShape::Path,
            &MixViewer {
                accessible_library_ids: Some(HashSet::from([visible_library_public_id])),
                ..MixViewer::default()
            },
            2,
        )?;
        assert_eq!(
            path.iter()
                .map(|track| track.track_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Visible B", "Visible A", "Visible Seed"],
            "a path keeps every visible step in order, past the limit",
        );

        let instant = pin_seed_and_truncate(seed, restricted, &HashMap::new(), 2);
        assert_eq!(
            instant
                .iter()
                .map(|track| track.track_title.as_str())
                .collect::<Vec<_>>(),
            vec!["Visible Seed", "Visible B"],
        );
        Ok(())
    }

    /// Urn: "Libera (Part I) – Saturnine Spheres" → "Libera (Part II) – Ascent of Burning Moths"
    /// → "Eyrie", among songs from another release.
    fn urn_with_others(db: &mut DbAny) -> anyhow::Result<(DbId, Vec<DbId>, Vec<DbId>)> {
        let metal = genres::resolve_by_name(db, "Metal")?;
        let urn = insert_release(db, "Urn")?;
        let portal = insert_release(db, "Portal of I")?;
        genres::link_to_release(db, metal, urn)?;
        genres::link_to_release(db, metal, portal)?;
        let mut chain = Vec::new();
        for title in [
            "Libera (Part I) – Saturnine Spheres",
            "Libera (Part II) – Ascent of Burning Moths",
            "Eyrie",
        ] {
            let track = insert_track(db, title)?;
            connect(db, urn, track)?;
            chain.push(track);
        }
        let mut others = Vec::new();
        for number in 0..8 {
            let track = insert_track(db, &format!("Portal {number}"))?;
            connect(db, portal, track)?;
            others.push(track);
        }
        lyra_db::chains::replace_for_release(db, urn, std::slice::from_ref(&chain))??;
        Ok((metal, chain, others))
    }

    fn ids_of(tracks: &[Track]) -> Vec<DbId> {
        tracks
            .iter()
            .filter_map(|track| track.db_id.clone().map(DbId::from))
            .collect()
    }

    #[test]
    fn chained_tracks_gather_at_the_first_in_chain_order() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, others) = urn_with_others(&mut db)?;
        let tracks = lyra_db::tracks::get_by_ids(&db, &[chain[0], chain[2]])?;
        let others_by_id = lyra_db::tracks::get_by_ids(&db, &others)?;
        let mix = vec![
            others_by_id[&others[0]].clone(),
            tracks[&chain[2]].clone(),
            others_by_id[&others[1]].clone(),
            tracks[&chain[0]].clone(),
            others_by_id[&others[2]].clone(),
        ];

        let links = chain_links(&db, &mix, None)?;
        let kept = keep_chains_together(mix, &links);

        assert_eq!(
            ids_of(&kept),
            vec![others[0], chain[0], chain[2], others[1], others[2]]
        );
        Ok(())
    }

    /// A blend of `mix` pinned on `seed`, as `pin_seed` orders it.
    fn instant(db: &DbAny, seed: DbId, mix: &[DbId]) -> anyhow::Result<Vec<DbId>> {
        let mut by_id = lyra_db::tracks::get_by_ids(db, mix)?;
        let seed_track = lyra_db::tracks::get_by_id(db, seed)?.expect("seed exists");
        let mix = mix
            .iter()
            .map(|id| by_id.remove(id).expect("mix track exists"))
            .collect::<Vec<_>>();
        let links = chain_links(db, &mix, Some(seed))?;
        let mix = keep_chains_together(mix, &links);
        Ok(ids_of(&pin_seed_and_truncate(seed_track, mix, &links, 100)))
    }

    #[test]
    fn a_first_member_seed_brings_its_chain_up_behind_it() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, o) = urn_with_others(&mut db)?;
        assert_eq!(
            instant(&db, chain[0], &[o[0], chain[0], chain[1]])?,
            vec![chain[0], chain[1], o[0]]
        );
        Ok(())
    }

    #[test]
    fn an_interior_seed_drops_earlier_members_and_leads_later_ones() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, o) = urn_with_others(&mut db)?;
        assert_eq!(
            instant(&db, chain[1], &[o[0], chain[0], chain[1], chain[2], o[1]])?,
            vec![chain[1], chain[2], o[0], o[1]]
        );
        Ok(())
    }

    #[test]
    fn a_last_member_seed_drops_the_rest_of_its_chain() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, o) = urn_with_others(&mut db)?;
        assert_eq!(
            instant(&db, chain[2], &[o[0], chain[0], chain[1], chain[2]])?,
            vec![chain[2], o[0]]
        );
        Ok(())
    }

    #[test]
    fn an_inserted_seed_brings_up_its_later_member() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, o) = urn_with_others(&mut db)?;
        assert_eq!(
            instant(&db, chain[0], &[o[0], chain[1], o[1]])?,
            vec![chain[0], chain[1], o[0], o[1]]
        );
        Ok(())
    }

    #[test]
    fn an_unchained_seed_is_pinned_before_the_kept_chains() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (_, chain, o) = urn_with_others(&mut db)?;
        assert_eq!(
            instant(&db, o[0], &[o[1], chain[1], o[0], chain[0], o[2]])?,
            vec![o[0], o[1], chain[0], chain[1], o[2]]
        );
        Ok(())
    }

    #[tokio::test]
    async fn mixes_play_a_chain_together_and_in_order() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;
        let (viewer, genre, chain) = {
            let mut db = STATE.db.write().await;
            let viewer_id = lyra_db::test_db::insert_user(&mut db, "mix-chain-viewer")?;
            let (metal, chain, _) = urn_with_others(&mut db)?;
            let genre = lyra_db::lookup::find_id_by_db_id(&*db, metal)?.expect("genre public id");
            (
                Principal::for_user(&*db, viewer_id, vec![Permission::Admin], HashSet::new()),
                genre,
                chain,
            )
        };
        let viewer = MixViewer::for_principal(&viewer);

        for _ in 0..16 {
            let mix = mix(&blend_of(genre.clone()), &viewer).await?;
            assert!(mix.iter().all(|mixed| mixed.score.is_none()));
            let tracks = mix.into_iter().map(|mixed| mixed.track).collect::<Vec<_>>();
            let ids = ids_of(&tracks);
            assert_eq!(ids.len(), 11);
            let start = ids
                .iter()
                .position(|id| *id == chain[0])
                .expect("chain in mix");
            assert_eq!(ids[start..start + 3], chain[..]);
        }
        Ok(())
    }

    fn track_with_id(db: &mut DbAny, title: &str) -> anyhow::Result<Track> {
        let id = insert_track(db, title)?;
        Ok(lyra_db::tracks::get_by_id(db, id)?.expect("track should exist after insert"))
    }

    #[test]
    fn pin_seed_prepends_when_missing_from_engine_output() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = track_with_id(&mut db, "Seed")?;
        let other_a = track_with_id(&mut db, "A")?;
        let other_b = track_with_id(&mut db, "B")?;

        let result =
            pin_seed_and_truncate(seed.clone(), vec![other_a, other_b], &HashMap::new(), 10);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].track_title, "Seed");
        assert_eq!(result[1].track_title, "A");
        assert_eq!(result[2].track_title, "B");
        Ok(())
    }

    #[test]
    fn pin_seed_moves_existing_seed_to_front() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = track_with_id(&mut db, "Seed")?;
        let other_a = track_with_id(&mut db, "A")?;
        let other_b = track_with_id(&mut db, "B")?;

        // Engine output already contains the seed at index 1 — move, don't dup.
        let result = pin_seed_and_truncate(
            seed.clone(),
            vec![other_a, seed.clone(), other_b],
            &HashMap::new(),
            10,
        );
        assert_eq!(result.len(), 3, "no duplication: count unchanged");
        assert_eq!(result[0].track_title, "Seed");
        let seed_count = result.iter().filter(|t| t.track_title == "Seed").count();
        assert_eq!(seed_count, 1, "seed should not be duplicated");
        Ok(())
    }

    #[test]
    fn pin_seed_truncates_to_limit_after_prepend() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = track_with_id(&mut db, "Seed")?;
        let a = track_with_id(&mut db, "A")?;
        let b = track_with_id(&mut db, "B")?;
        let c = track_with_id(&mut db, "C")?;

        // [Seed, A, B, C] truncated to 2 — seed kept, tail dropped.
        let result = pin_seed_and_truncate(seed.clone(), vec![a, b, c], &HashMap::new(), 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].track_title, "Seed");
        assert_eq!(result[1].track_title, "A");
        Ok(())
    }

    #[test]
    fn pin_seed_handles_limit_one() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = track_with_id(&mut db, "Seed")?;
        let a = track_with_id(&mut db, "A")?;

        // limit=1: seed wins the only slot.
        let result = pin_seed_and_truncate(seed.clone(), vec![a], &HashMap::new(), 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].track_title, "Seed");
        Ok(())
    }

    #[test]
    fn pin_seed_handles_empty_engine_output() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let seed = track_with_id(&mut db, "Seed")?;

        // Genre-less seed: engine returns nothing, caller still gets the seed.
        let result = pin_seed_and_truncate(seed.clone(), vec![], &HashMap::new(), 200);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].track_title, "Seed");
        Ok(())
    }
}
