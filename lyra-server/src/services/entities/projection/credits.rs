// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use agdb::{
    DbAny,
    DbId,
};

use crate::{
    db::Release,
    services::entities::{
        CreditedArtistProjectionInfo,
        ResolvedCreditedArtist,
        TrackCreditedArtistContext,
        dedupe_db_ids,
        resolve_release_credited_artists,
        resolve_release_credited_artists_map,
        resolve_track_credited_artists,
        resolve_track_credited_artists_with_context,
    },
};

pub(super) fn project(artists: Vec<ResolvedCreditedArtist>) -> Vec<CreditedArtistProjectionInfo> {
    artists.into_iter().map(Into::into).collect()
}

pub(super) fn fetch_release(
    db: &DbAny,
    release_id: DbId,
) -> anyhow::Result<Vec<CreditedArtistProjectionInfo>> {
    Ok(project(resolve_release_credited_artists(db, release_id)?))
}

pub(super) fn fetch_track(
    db: &DbAny,
    track_id: DbId,
) -> anyhow::Result<Vec<CreditedArtistProjectionInfo>> {
    Ok(project(resolve_track_credited_artists(db, track_id)?))
}

pub(super) fn prefetch_by_owner(
    db: &DbAny,
    release_ids: &[DbId],
    track_ids: &[DbId],
    releases_by_track: Option<&HashMap<DbId, Vec<Release>>>,
) -> anyhow::Result<HashMap<DbId, Vec<CreditedArtistProjectionInfo>>> {
    let credit_release_ids = release_ids_for_prefetch(release_ids, releases_by_track);
    let credited_artists_by_release = if credit_release_ids.is_empty() {
        HashMap::new()
    } else {
        resolve_release_credited_artists_map(db, &credit_release_ids)?
    };

    let credited_artists_by_track = if track_ids.is_empty() {
        HashMap::new()
    } else {
        let ctx = TrackCreditedArtistContext {
            releases_by_track,
            credited_artists_by_release: Some(&credited_artists_by_release),
            scope_release_id: None,
        };
        resolve_track_credited_artists_with_context(db, track_ids, &ctx)?
    };

    let mut credits = HashMap::new();
    for (owner_id, artists) in credited_artists_by_release {
        credits.insert(owner_id, project(artists));
    }
    for (owner_id, artists) in credited_artists_by_track {
        credits.insert(owner_id, project(artists));
    }

    Ok(credits)
}

fn release_ids_for_prefetch(
    release_ids: &[DbId],
    releases_by_track: Option<&HashMap<DbId, Vec<Release>>>,
) -> Vec<DbId> {
    let mut ids = release_ids.to_vec();
    if let Some(releases_by_track) = releases_by_track {
        for releases in releases_by_track.values() {
            for release in releases {
                if let Some(release_id) = release.db_id.clone().map(DbId::from) {
                    ids.push(release_id);
                }
            }
        }
    }

    dedupe_db_ids(&ids)
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
        QueryId,
    };
    use nanoid::nanoid;

    use crate::{
        db::{
            self,
            test_db::{
                connect,
                insert_artist,
                insert_release,
                insert_track,
                new_test_db,
            },
        },
        services::entities::{
            ArtistCreditSource,
            EntityInclude,
            EntityProjectionInfo,
            project_entities,
            project_entity,
        },
    };

    fn connect_artist_credit(
        db: &mut DbAny,
        owner_id: DbId,
        artist_id: DbId,
        credit_type: db::CreditType,
        detail: Option<&str>,
    ) -> anyhow::Result<()> {
        let credit = db::Credit {
            db_id: None,
            id: nanoid!(),
            credit_type,
            detail: detail.map(str::to_string),
        };
        let credit_id = db
            .exec_mut(QueryBuilder::insert().element(&credit).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("credits")
                .to(credit_id)
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(owner_id)
                .to(credit_id)
                .values_uniform([
                    ("owned", 1).into(),
                    (db::credits::EDGE_ORDER_KEY, 0_u64).into(),
                ])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(credit_id)
                .to(artist_id)
                .query(),
        )?;
        Ok(())
    }

    #[test]
    fn project_release_includes_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Credit Release")?;
        let artist_id = insert_artist(&mut db, "Engineer")?;
        connect_artist_credit(
            &mut db,
            release_id,
            artist_id,
            db::CreditType::Engineer,
            Some("recording"),
        )?;

        let projection = project_entity(
            &db,
            QueryId::Id(release_id),
            &[EntityInclude::Credits],
            None,
        )?;
        let EntityProjectionInfo::Release(release) = projection else {
            panic!("expected release projection");
        };
        let credits = release.includes.credits.expect("credits included");

        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].artist.artist_name, "Engineer");
        assert_eq!(credits[0].credit.r#type, db::CreditType::Engineer);
        assert_eq!(credits[0].credit.detail.as_deref(), Some("recording"));
        assert_eq!(credits[0].source, ArtistCreditSource::Release);
        Ok(())
    }

    #[test]
    fn project_track_credits_fall_back_to_release_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Credit Release")?;
        let track_id = insert_track(&mut db, "Credit Track")?;
        let artist_id = insert_artist(&mut db, "Composer")?;
        connect(&mut db, release_id, track_id)?;
        connect_artist_credit(
            &mut db,
            release_id,
            artist_id,
            db::CreditType::Composer,
            None,
        )?;

        let projection =
            project_entity(&db, QueryId::Id(track_id), &[EntityInclude::Credits], None)?;
        let EntityProjectionInfo::Track(track) = projection else {
            panic!("expected track projection");
        };
        let credits = track.includes.credits.expect("credits included");

        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].artist.artist_name, "Composer");
        assert_eq!(credits[0].credit.r#type, db::CreditType::Composer);
        assert_eq!(credits[0].source, ArtistCreditSource::Release);
        Ok(())
    }

    #[test]
    fn project_entities_prefetches_track_credit_fallbacks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Batch Release")?;
        let track_id = insert_track(&mut db, "Batch Track")?;
        let artist_id = insert_artist(&mut db, "Batch Lyricist")?;
        connect(&mut db, release_id, track_id)?;
        connect_artist_credit(
            &mut db,
            release_id,
            artist_id,
            db::CreditType::Lyricist,
            Some("translation"),
        )?;

        let projections = project_entities(
            &db,
            vec![QueryId::Id(track_id)],
            &[EntityInclude::Credits],
            None,
        )?;
        let EntityProjectionInfo::Track(track) = &projections[0] else {
            panic!("expected track projection");
        };
        let credits = track.includes.credits.as_ref().expect("credits included");

        assert_eq!(credits.len(), 1);
        assert_eq!(credits[0].artist.artist_name, "Batch Lyricist");
        assert_eq!(credits[0].credit.r#type, db::CreditType::Lyricist);
        assert_eq!(credits[0].credit.detail.as_deref(), Some("translation"));
        assert_eq!(credits[0].source, ArtistCreditSource::Release);
        Ok(())
    }

    #[test]
    fn project_artist_rejects_credits_include() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Artist")?;

        let err = project_entity(&db, QueryId::Id(artist_id), &[EntityInclude::Credits], None)
            .expect_err("artist projections should reject credit includes");

        assert!(err.to_string().contains("not supported"));
        Ok(())
    }
}
