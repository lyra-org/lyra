// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    CountComparison,
    DbElement,
    DbId,
    DbValue,
    QueryBuilder,
};

use super::DbAccess;

pub(crate) const LIST_IDS_CAP: usize = 10_000;

const KIND_KEY: &str = "favorite_kind";
const FIRST_FAVORITED_KEY: &str = "first_favorited_at_ms";
const LAST_REFRESHED_KEY: &str = "last_refreshed_at_ms";

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum FavoriteKind {
    Track,
    Release,
    Artist,
    Playlist,
}

impl FavoriteKind {
    pub(crate) const ALL: [FavoriteKind; 4] =
        [Self::Track, Self::Release, Self::Artist, Self::Playlist];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::Release => "release",
            Self::Artist => "artist",
            Self::Playlist => "playlist",
        }
    }

    pub(crate) fn collection_alias(self) -> &'static str {
        match self {
            Self::Track => "tracks",
            Self::Release => "releases",
            Self::Artist => "artists",
            Self::Playlist => "playlists",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown favorite kind: {0}")]
pub struct FavoriteKindParseError(pub String);

impl TryFrom<&str> for FavoriteKind {
    type Error = FavoriteKindParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "track" => Ok(Self::Track),
            "release" => Ok(Self::Release),
            "artist" => Ok(Self::Artist),
            "playlist" => Ok(Self::Playlist),
            other => Err(FavoriteKindParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FavoriteEdge {
    pub db_id: DbId,
    pub target_db_id: DbId,
    pub kind: FavoriteKind,
    pub first_favorited_at_ms: i64,
    pub last_refreshed_at_ms: i64,
}

pub fn target_kind(db: &impl DbAccess, target_db_id: DbId) -> anyhow::Result<Option<FavoriteKind>> {
    if target_db_id.0 <= 0 {
        return Ok(None);
    }
    for kind in FavoriteKind::ALL {
        if collection_neighbor(db, kind.collection_alias(), target_db_id)? {
            return Ok(Some(kind));
        }
    }
    Ok(None)
}

fn collection_neighbor(
    db: &impl DbAccess,
    collection_alias: &str,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(collection_alias)
            .where_()
            .neighbor()
            .and()
            .ids(target_db_id)
            .query(),
    )?;
    Ok(!result.elements.is_empty())
}

/// Insert a favorite edge, or refresh `last_refreshed_at_ms` on an existing one.
/// `first_favorited_at_ms` is immutable for pagination stability.
pub fn add(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
    kind: FavoriteKind,
    now_ms: i64,
) -> anyhow::Result<()> {
    if let Some(edge_id) = find_favorite_edge(db, user_db_id, target_db_id)? {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(LAST_REFRESHED_KEY, now_ms).into()])
                .ids(edge_id)
                .query(),
        )?;
        return Ok(());
    }

    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(user_db_id)
                .to(target_db_id)
                .query(),
        )?
        .ids()[0];
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([
                (KIND_KEY, kind.as_str()).into(),
                (FIRST_FAVORITED_KEY, now_ms).into(),
                (LAST_REFRESHED_KEY, now_ms).into(),
            ])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

/// Idempotent.
pub fn remove(
    db: &mut impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<bool> {
    let Some(edge_id) = find_favorite_edge(db, user_db_id, target_db_id)? else {
        return Ok(false);
    };
    db.exec_mut(QueryBuilder::remove().ids(edge_id).query())?;
    Ok(true)
}

pub fn has(db: &impl DbAccess, user_db_id: DbId, target_db_id: DbId) -> anyhow::Result<bool> {
    Ok(find_favorite_edge(db, user_db_id, target_db_id)?.is_some())
}

/// The user's favorites of `target_db_ids`, keyed by target; unfavorited targets are absent.
pub fn for_targets(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, FavoriteEdge>> {
    if target_db_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let requested: HashSet<DbId> = target_db_ids.iter().copied().collect();
    Ok(read_outbound_favorite_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_favorite_edge)
        .filter(|edge| requested.contains(&edge.target_db_id))
        .map(|edge| (edge.target_db_id, edge))
        .collect())
}

/// Every favorite target DbId for a user+kind. Errs above [`LIST_IDS_CAP`].
pub fn list_ids(
    db: &impl DbAccess,
    user_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<Vec<DbId>> {
    list_ids_with_cap(db, user_db_id, kind, LIST_IDS_CAP)
}

/// Every favorite target of `kind` for a user.
pub fn target_ids(
    db: &impl DbAccess,
    user_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<HashSet<DbId>> {
    Ok(read_outbound_favorite_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_favorite_edge)
        .filter(|edge| edge.kind == kind)
        .map(|edge| edge.target_db_id)
        .collect())
}

/// When the user first favorited each favorite target of `kind`.
pub fn first_favorited_at(
    db: &impl DbAccess,
    user_db_id: DbId,
    kind: FavoriteKind,
) -> anyhow::Result<HashMap<DbId, i64>> {
    Ok(read_outbound_favorite_edges(db, user_db_id)?
        .into_iter()
        .filter_map(parse_favorite_edge)
        .filter(|edge| edge.kind == kind)
        .map(|edge| (edge.target_db_id, edge.first_favorited_at_ms))
        .collect())
}

fn list_ids_with_cap(
    db: &impl DbAccess,
    user_db_id: DbId,
    kind: FavoriteKind,
    cap: usize,
) -> anyhow::Result<Vec<DbId>> {
    let mut ids: Vec<DbId> = Vec::new();
    for element in read_outbound_favorite_edges(db, user_db_id)? {
        let Some(parsed) = parse_favorite_edge(element) else {
            continue;
        };
        if parsed.kind != kind {
            continue;
        }
        if ids.len() >= cap {
            anyhow::bail!(
                "list_ids cap exceeded: >{cap} favorites of kind {}",
                kind.as_str(),
            );
        }
        ids.push(parsed.target_db_id);
    }
    Ok(ids)
}

pub(crate) fn remove_outbound_for_user(
    db: &mut impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_outbound_favorite_edges(db, user_db_id)?
        .into_iter()
        .map(|element| element.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

pub(crate) fn remove_inbound_for_target(
    db: &mut impl DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<()> {
    let edge_ids: Vec<DbId> = read_inbound_favorite_edges(db, target_db_id)?
        .into_iter()
        .map(|element| element.id)
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }
    Ok(())
}

fn find_favorite_edge(
    db: &impl DbAccess,
    user_db_id: DbId,
    target_db_id: DbId,
) -> anyhow::Result<Option<DbId>> {
    // Query from the user's side. Avoids an `Id not found` error on a target
    // that has been deleted since the caller's last observation — in that case
    // the cascade has already removed the edge and we correctly return `None`.
    for element in read_outbound_favorite_edges(db, user_db_id)? {
        if element.to == target_db_id {
            return Ok(Some(element.id));
        }
    }
    Ok(None)
}

fn read_outbound_favorite_edges(
    db: &impl DbAccess,
    user_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .from(user_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.from == user_db_id && element_is_favorite(element))
        .collect())
}

fn read_inbound_favorite_edges(
    db: &impl DbAccess,
    target_db_id: DbId,
) -> anyhow::Result<Vec<DbElement>> {
    let result = db.exec(
        QueryBuilder::select()
            .search()
            .to(target_db_id)
            .where_()
            .edge()
            .and()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    Ok(result
        .elements
        .into_iter()
        .filter(|element| element.to == target_db_id && element_is_favorite(element))
        .collect())
}

fn element_is_favorite(element: &DbElement) -> bool {
    element
        .values
        .iter()
        .any(|kv| matches!(&kv.key, DbValue::String(k) if k == KIND_KEY))
}

fn parse_favorite_edge(element: DbElement) -> Option<FavoriteEdge> {
    if element.id.0 >= 0 {
        return None;
    }
    let target_db_id = (element.to.0 != 0).then_some(element.to)?;
    let mut kind: Option<FavoriteKind> = None;
    let mut first_favorited_at_ms: Option<i64> = None;
    let mut last_refreshed_at_ms: Option<i64> = None;

    for kv in &element.values {
        let DbValue::String(key) = &kv.key else {
            continue;
        };
        match key.as_str() {
            KIND_KEY => {
                if let DbValue::String(s) = &kv.value {
                    kind = FavoriteKind::try_from(s.as_str()).ok();
                }
            }
            FIRST_FAVORITED_KEY => first_favorited_at_ms = kv_to_i64(&kv.value),
            LAST_REFRESHED_KEY => last_refreshed_at_ms = kv_to_i64(&kv.value),
            _ => {}
        }
    }

    Some(FavoriteEdge {
        db_id: element.id,
        target_db_id,
        kind: kind?,
        first_favorited_at_ms: first_favorited_at_ms?,
        last_refreshed_at_ms: last_refreshed_at_ms?,
    })
}

fn kv_to_i64(value: &DbValue) -> Option<i64> {
    match value {
        DbValue::I64(v) => Some(*v),
        DbValue::U64(v) => Some(*v as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::new_test_db;
    use agdb::DbAny;

    fn create_test_user(db: &mut DbAny) -> anyhow::Result<DbId> {
        let user_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("username", "testuser").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(user_db_id)
                .query(),
        )?;
        Ok(user_db_id)
    }

    fn create_test_track(db: &mut DbAny) -> anyhow::Result<DbId> {
        let track_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("track_title", "Track").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("tracks")
                .to(track_db_id)
                .query(),
        )?;
        Ok(track_db_id)
    }

    fn create_test_release(db: &mut DbAny) -> anyhow::Result<DbId> {
        let release_db_id = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("name", "Release").into()]])
                    .query(),
            )?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("releases")
                .to(release_db_id)
                .query(),
        )?;
        Ok(release_db_id)
    }

    #[test]
    fn target_kind_resolves_whitelisted_collections() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track = create_test_track(&mut db)?;
        let release = create_test_release(&mut db)?;

        assert_eq!(target_kind(&db, track)?, Some(FavoriteKind::Track));
        assert_eq!(target_kind(&db, release)?, Some(FavoriteKind::Release));

        Ok(())
    }

    #[test]
    fn target_kind_rejects_non_whitelisted_node() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;

        assert_eq!(target_kind(&db, user)?, None);

        Ok(())
    }

    #[test]
    fn add_inserts_favorite_edge_with_timestamps_and_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user, track, FavoriteKind::Track, 1000)?;

        assert!(has(&db, user, track)?);
        let edges = read_outbound_favorite_edges(&db, user)?;
        assert_eq!(edges.len(), 1);
        let parsed = parse_favorite_edge(edges.into_iter().next().unwrap()).unwrap();
        assert_eq!(parsed.kind, FavoriteKind::Track);
        assert_eq!(parsed.first_favorited_at_ms, 1000);
        assert_eq!(parsed.last_refreshed_at_ms, 1000);

        Ok(())
    }

    #[test]
    fn add_refresh_preserves_first_favorited_and_advances_last_refreshed() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user, track, FavoriteKind::Track, 1000)?;
        add(&mut db, user, track, FavoriteKind::Track, 5000)?;

        let edges = read_outbound_favorite_edges(&db, user)?;
        assert_eq!(edges.len(), 1, "re-add must not create a duplicate edge");
        let parsed = parse_favorite_edge(edges.into_iter().next().unwrap()).unwrap();
        assert_eq!(parsed.first_favorited_at_ms, 1000);
        assert_eq!(parsed.last_refreshed_at_ms, 5000);

        Ok(())
    }

    #[test]
    fn remove_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user, track, FavoriteKind::Track, 100)?;
        assert!(remove(&mut db, user, track)?);
        assert!(!remove(&mut db, user, track)?);
        assert!(!has(&db, user, track)?);

        Ok(())
    }

    #[test]
    fn per_user_isolation_on_has_and_for_targets_and_list() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_test_user(&mut db)?;
        let user_b = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user_a, track, FavoriteKind::Track, 1000)?;

        assert!(has(&db, user_a, track)?);
        assert!(
            !has(&db, user_b, track)?,
            "user A's favorite must not leak to user B via has"
        );

        let a_map = for_targets(&db, user_a, &[track])?;
        let b_map = for_targets(&db, user_b, &[track])?;
        assert_eq!(
            a_map.get(&track).map(|edge| edge.first_favorited_at_ms),
            Some(1000)
        );
        assert!(
            b_map.is_empty(),
            "user A's favorite must not leak to user B via for_targets"
        );

        assert_eq!(
            first_favorited_at(&db, user_a, FavoriteKind::Track)?,
            HashMap::from([(track, 1000)])
        );
        assert!(
            first_favorited_at(&db, user_b, FavoriteKind::Track)?.is_empty(),
            "user A's favorite must not leak to user B via first_favorited_at"
        );

        let b_ids = list_ids(&db, user_b, FavoriteKind::Track)?;
        assert!(
            b_ids.is_empty(),
            "user A's favorite must not leak to user B via list_ids"
        );

        Ok(())
    }

    #[test]
    fn first_favorited_at_keeps_the_first_time_and_kind() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;
        let track_c = create_test_track(&mut db)?;

        add(&mut db, user, track_a, FavoriteKind::Track, 1000)?;
        add(&mut db, user, track_b, FavoriteKind::Track, 2000)?;
        add(&mut db, user, track_a, FavoriteKind::Track, 3000)?;
        add(&mut db, user, track_c, FavoriteKind::Release, 4000)?;

        assert_eq!(
            first_favorited_at(&db, user, FavoriteKind::Track)?,
            HashMap::from([(track_a, 1000), (track_b, 2000)]),
            "a refresh keeps the first time, and other kinds stay out",
        );

        Ok(())
    }

    #[test]
    fn remove_outbound_for_user_clears_all_favorites() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;

        add(&mut db, user, track_a, FavoriteKind::Track, 1)?;
        add(&mut db, user, track_b, FavoriteKind::Track, 2)?;

        remove_outbound_for_user(&mut db, user)?;

        assert!(!has(&db, user, track_a)?);
        assert!(!has(&db, user, track_b)?);

        Ok(())
    }

    #[test]
    fn remove_inbound_for_target_clears_all_user_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = create_test_user(&mut db)?;
        let user_b = create_test_user(&mut db)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user_a, track, FavoriteKind::Track, 1)?;
        add(&mut db, user_b, track, FavoriteKind::Track, 2)?;

        remove_inbound_for_target(&mut db, track)?;

        assert!(!has(&db, user_a, track)?);
        assert!(!has(&db, user_b, track)?);

        Ok(())
    }

    #[test]
    fn list_ids_errors_on_overflow() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;

        for _ in 0..3 {
            let track = create_test_track(&mut db)?;
            add(&mut db, user, track, FavoriteKind::Track, 0)?;
        }

        let err =
            list_ids_with_cap(&db, user, FavoriteKind::Track, 2).expect_err("expected cap err");
        assert!(
            err.to_string().contains("cap exceeded"),
            "error should mention cap: got {err}",
        );

        let ok = list_ids_with_cap(&db, user, FavoriteKind::Track, 5)?;
        assert_eq!(ok.len(), 3);

        Ok(())
    }

    #[test]
    fn delete_user_cascades_favorites() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user_a = crate::users::create(&mut db, &crate::fixtures::test_user("alice")?)?;
        let user_b = crate::users::create(&mut db, &crate::fixtures::test_user("bob")?)?;
        let track = create_test_track(&mut db)?;

        add(&mut db, user_a, track, FavoriteKind::Track, 1)?;
        add(&mut db, user_b, track, FavoriteKind::Track, 2)?;

        crate::users::delete_user(&mut db, user_a)?;

        let inbound = read_inbound_favorite_edges(&db, track)?;
        assert_eq!(
            inbound.len(),
            1,
            "only user_b's favorite edge should remain after deleting user_a",
        );
        assert_eq!(
            inbound[0].from, user_b,
            "remaining favorite edge must originate from user_b",
        );

        Ok(())
    }

    #[test]
    fn cascade_remove_entities_cascades_inbound_favorites() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let track_a = create_test_track(&mut db)?;
        let track_b = create_test_track(&mut db)?;

        add(&mut db, user, track_a, FavoriteKind::Track, 1)?;
        add(&mut db, user, track_b, FavoriteKind::Track, 2)?;

        crate::metadata::cascade_remove_entities(&mut db, &[track_a])?;

        assert!(
            !has(&db, user, track_a)?,
            "favorite edge to deleted track must be cascaded away",
        );
        assert!(
            has(&db, user, track_b)?,
            "unrelated favorite must be left intact",
        );

        Ok(())
    }

    #[test]
    fn delete_playlist_cascades_inbound_favorites() -> anyhow::Result<()> {
        use nanoid::nanoid;

        let mut db = new_test_db()?;
        let user = create_test_user(&mut db)?;
        let playlist = crate::Playlist {
            db_id: None,
            id: nanoid!(),
            name: "p".to_string(),
            description: None,
            is_public: Some(false),
            created_at_ms: None,
            updated_at_ms: None,
        };
        let playlist_db_id = crate::playlists::create(&mut db, &playlist, user)?;

        add(&mut db, user, playlist_db_id, FavoriteKind::Playlist, 10)?;
        assert!(has(&db, user, playlist_db_id)?);

        crate::playlists::delete(&mut db, playlist_db_id)?;

        assert!(
            !has(&db, user, playlist_db_id)?,
            "favorite edge must be removed when the playlist is deleted",
        );
        assert!(
            target_ids(&db, user, FavoriteKind::Playlist)?.is_empty(),
            "no edge may remain whose target has been deleted",
        );

        Ok(())
    }
    #[cfg(feature = "nightly")]
    mod benches {
        extern crate test;

        use test::Bencher;

        use super::*;
        use crate::test_db::new_test_db;
        use agdb::{
            DbAny,
            QueryId,
        };

        fn insert_user(db: &mut DbAny) -> DbId {
            let id = db
                .exec_mut(
                    QueryBuilder::insert()
                        .nodes()
                        .values([[("username", "bench").into()]])
                        .query(),
                )
                .unwrap()
                .ids()[0];
            db.exec_mut(QueryBuilder::insert().edges().from("users").to(id).query())
                .unwrap();
            id
        }

        fn insert_track(db: &mut DbAny) -> DbId {
            let id = db
                .exec_mut(QueryBuilder::insert().nodes().count(1).query())
                .unwrap()
                .ids()[0];
            db.exec_mut(QueryBuilder::insert().edges().from("tracks").to(id).query())
                .unwrap();
            id
        }

        fn seed_user_favorites(n: usize) -> (DbAny, DbId, Vec<DbId>) {
            let mut db = new_test_db().unwrap();
            let user = insert_user(&mut db);
            let mut tracks = Vec::with_capacity(n);
            for i in 0..n {
                let t = insert_track(&mut db);
                add(&mut db, user, t, FavoriteKind::Track, i as i64).unwrap();
                tracks.push(t);
            }
            (db, user, tracks)
        }

        #[bench]
        fn for_targets_user_with_100_favorites(b: &mut Bencher) {
            let (db, user, tracks) = seed_user_favorites(100);
            let probe: Vec<DbId> = tracks.iter().take(10).copied().collect();
            b.iter(|| for_targets(&db, user, &probe).unwrap());
        }

        #[bench]
        fn for_targets_user_with_1000_favorites(b: &mut Bencher) {
            let (db, user, tracks) = seed_user_favorites(1_000);
            let probe: Vec<DbId> = tracks.iter().take(10).copied().collect();
            b.iter(|| for_targets(&db, user, &probe).unwrap());
        }

        #[bench]
        fn list_ids_user_with_100_favorites(b: &mut Bencher) {
            let (db, user, _) = seed_user_favorites(100);
            b.iter(|| list_ids(&db, user, FavoriteKind::Track).unwrap());
        }

        #[bench]
        fn list_ids_user_with_1000_favorites(b: &mut Bencher) {
            let (db, user, _) = seed_user_favorites(1_000);
            b.iter(|| list_ids(&db, user, FavoriteKind::Track).unwrap());
        }

        #[bench]
        fn read_inbound_edges_100_users(b: &mut Bencher) {
            let mut db = new_test_db().unwrap();
            let track = insert_track(&mut db);
            for i in 0..100 {
                let user = insert_user(&mut db);
                add(&mut db, user, track, FavoriteKind::Track, i as i64).unwrap();
            }
            b.iter(|| read_inbound_favorite_edges(&db, track).unwrap());
        }

        #[bench]
        fn read_inbound_edges_1000_users(b: &mut Bencher) {
            let mut db = new_test_db().unwrap();
            let track = insert_track(&mut db);
            for i in 0..1_000 {
                let user = insert_user(&mut db);
                add(&mut db, user, track, FavoriteKind::Track, i as i64).unwrap();
            }
            b.iter(|| read_inbound_favorite_edges(&db, track).unwrap());
        }

        fn insert_listen(db: &mut DbAny, user: DbId, track: DbId) {
            let listen = db
                .exec_mut(QueryBuilder::insert().nodes().count(1).query())
                .unwrap()
                .ids()[0];
            for (from, to) in [
                (QueryId::from("listens"), listen),
                (listen.into(), track),
                (listen.into(), user),
            ] {
                db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())
                    .unwrap();
            }
        }

        /// Ten users favorite the probed track. Each user listens to the probed
        /// track `track_listens_per_user` times and to another track
        /// `other_listens_per_user` times.
        fn seed_favorites_with_listens(
            track_listens_per_user: usize,
            other_listens_per_user: usize,
        ) -> (DbAny, DbId) {
            let mut db = new_test_db().unwrap();
            let track = insert_track(&mut db);
            let other_track = insert_track(&mut db);
            for i in 0..10 {
                let user = insert_user(&mut db);
                add(&mut db, user, track, FavoriteKind::Track, i as i64).unwrap();
                for _ in 0..track_listens_per_user {
                    insert_listen(&mut db, user, track);
                }
                for _ in 0..other_listens_per_user {
                    insert_listen(&mut db, user, other_track);
                }
            }
            (db, track)
        }

        #[bench]
        fn read_inbound_edges_10_users_no_listens(b: &mut Bencher) {
            let (db, track) = seed_favorites_with_listens(0, 0);
            b.iter(|| read_inbound_favorite_edges(&db, track).unwrap());
        }

        #[bench]
        fn read_inbound_edges_10_users_1000_listens_on_track(b: &mut Bencher) {
            let (db, track) = seed_favorites_with_listens(100, 0);
            b.iter(|| read_inbound_favorite_edges(&db, track).unwrap());
        }

        #[bench]
        fn read_inbound_edges_10_users_10000_listens_elsewhere(b: &mut Bencher) {
            let (db, track) = seed_favorites_with_listens(0, 1_000);
            b.iter(|| read_inbound_favorite_edges(&db, track).unwrap());
        }
    }
}
