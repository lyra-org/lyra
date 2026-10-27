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

use crate::services::entities::relations;
use lyra_db::{
    Artist,
    ArtistRelationType,
    Release,
    Track,
};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ArtistIncludes {
    pub(crate) releases: bool,
    pub(crate) tracks: bool,
    pub(crate) relations: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RelationDirection {
    Incoming,
    Outgoing,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedRelation {
    pub(crate) relation_type: ArtistRelationType,
    pub(crate) attributes: Option<String>,
    pub(crate) direction: RelationDirection,
    pub(crate) artist: Artist,
}

pub(crate) struct ArtistDetails {
    pub(crate) artist: Artist,
    pub(crate) releases: Option<Vec<Release>>,
    pub(crate) tracks: Option<Vec<Track>>,
    pub(crate) relations: Option<Vec<ResolvedRelation>>,
}

pub(crate) fn get_relations(
    db: &DbAny,
    artist_db_id: DbId,
) -> anyhow::Result<Vec<ResolvedRelation>> {
    let mut resolved = Vec::new();

    let incoming = lyra_db::artists::relations::get_relations_to(db, artist_db_id, None)?;
    for (relation, peer_id) in incoming {
        if let Some(peer_artist) = lyra_db::artists::get_by_id(db, peer_id)? {
            resolved.push(ResolvedRelation {
                relation_type: relation.relation_type,
                attributes: relation.attributes,
                direction: RelationDirection::Incoming,
                artist: peer_artist,
            });
        }
    }

    let outgoing = lyra_db::artists::relations::get_relations_from(db, artist_db_id, None)?;
    for (relation, peer_id) in outgoing {
        if let Some(peer_artist) = lyra_db::artists::get_by_id(db, peer_id)? {
            resolved.push(ResolvedRelation {
                relation_type: relation.relation_type,
                attributes: relation.attributes,
                direction: RelationDirection::Outgoing,
                artist: peer_artist,
            });
        }
    }

    Ok(resolved)
}

pub(crate) fn get_relations_many(
    db: &DbAny,
    artist_db_ids: &[DbId],
) -> anyhow::Result<HashMap<DbId, Vec<ResolvedRelation>>> {
    let mut relations_by_artist_id = HashMap::new();
    let mut seen = HashSet::new();

    for artist_db_id in artist_db_ids.iter().copied() {
        if artist_db_id.0 <= 0 || !seen.insert(artist_db_id) {
            continue;
        }

        relations_by_artist_id.insert(artist_db_id, get_relations(db, artist_db_id)?);
    }

    Ok(relations_by_artist_id)
}

pub(crate) fn list_details_for_artists(
    db: &DbAny,
    includes: ArtistIncludes,
    artists: Vec<Artist>,
) -> anyhow::Result<Vec<ArtistDetails>> {
    let artist_ids = relations::db_ids_from_artists(&artists);
    let owned_entities = relations::artist_owned_entities_by_artist(
        db,
        &artist_ids,
        includes.releases,
        includes.tracks,
    )?;
    let mut releases_by_artist = owned_entities.releases_by_artist;
    let mut tracks_by_artist = owned_entities.tracks_by_artist;
    let mut relations_by_artist = if includes.relations {
        get_relations_many(db, &artist_ids)?
    } else {
        HashMap::new()
    };

    let mut details = Vec::with_capacity(artists.len());

    for artist in artists {
        let artist_db_id = artist
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("artist missing db id"))?;
        let releases = if includes.releases {
            Some(releases_by_artist.remove(&artist_db_id).unwrap_or_default())
        } else {
            None
        };
        let tracks = if includes.tracks {
            Some(tracks_by_artist.remove(&artist_db_id).unwrap_or_default())
        } else {
            None
        };
        let relations = if includes.relations {
            Some(
                relations_by_artist
                    .remove(&artist_db_id)
                    .unwrap_or_default(),
            )
        } else {
            None
        };

        details.push(ArtistDetails {
            artist,
            releases,
            tracks,
            relations,
        });
    }

    Ok(details)
}

pub(crate) fn get_details(
    db: &DbAny,
    artist_db_id: DbId,
    includes: ArtistIncludes,
) -> anyhow::Result<Option<ArtistDetails>> {
    let Some(artist) = lyra_db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(None);
    };

    Ok(list_details_for_artists(db, includes, vec![artist])?
        .into_iter()
        .next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::catalog::{
        self,
        artists::{
            ArtistFilter,
            Artists,
        },
        pipeline::Catalog,
    };
    use lyra_db::artists::relations::link as link_artist_relation;
    use lyra_db::fixtures::{
        connect,
        insert_artist,
        insert_release,
        insert_track,
    };
    use lyra_db::test_db::{
        connect_artist,
        insert_credited_artist,
        insert_library,
        new_test_db,
    };

    fn list_details(db: &DbAny, includes: ArtistIncludes) -> anyhow::Result<Vec<ArtistDetails>> {
        list_details_for_artists(db, includes, credited_names(db, ArtistFilter::default())?)
    }

    /// The artists a system query with `filter` returns.
    fn credited_names(db: &DbAny, filter: ArtistFilter) -> anyhow::Result<Vec<Artist>> {
        let query = catalog::Query::<Artists>::new(filter);
        let ids = catalog::order(db, &catalog::Viewer::System, &query)?;
        Artists::hydrate(db, &ids)
    }

    /// One page of a system browse with `filter`: its names and the total.
    fn browse(
        db: &DbAny,
        filter: ArtistFilter,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<(Vec<String>, u64)> {
        let query = catalog::Query::<Artists>::new(filter);
        let page = catalog::page(db, &catalog::Viewer::System, &query, offset, Some(limit))?;
        Ok((
            page.items
                .into_iter()
                .map(|artist| artist.artist_name)
                .collect(),
            page.total,
        ))
    }

    fn set_artist_type(
        db: &mut DbAny,
        artist_db_id: DbId,
        artist_type: lyra_db::ArtistType,
    ) -> anyhow::Result<()> {
        let mut artist = lyra_db::artists::get_by_id(db, artist_db_id)?
            .ok_or_else(|| anyhow::anyhow!("artist should exist"))?;
        artist.set_artist_type(artist_type);
        lyra_db::artists::update(db, &artist)
    }

    #[test]
    fn list_details_returns_artists_with_releases_and_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Coltrane")?;
        let release_id = insert_release(&mut db, "A Love Supreme")?;
        let track_id = insert_track(&mut db, "Acknowledgement")?;

        connect_artist(&mut db, release_id, artist_id)?;
        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = ArtistIncludes {
            releases: true,
            tracks: true,
            ..Default::default()
        };
        let details = list_details(&db, includes)?;

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].artist.artist_name, "Coltrane");
        assert_eq!(
            details[0].artist.db_id.clone().map(DbId::from),
            Some(artist_id)
        );

        let releases = details[0].releases.as_ref().expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "A Love Supreme");

        let tracks = details[0].tracks.as_ref().expect("tracks included");
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Acknowledgement");

        Ok(())
    }

    #[test]
    fn list_details_omits_includes_when_disabled() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        insert_credited_artist(&mut db, "Solo Artist")?;

        let includes = ArtistIncludes {
            releases: false,
            tracks: false,
            ..Default::default()
        };
        let details = list_details(&db, includes)?;

        assert_eq!(details.len(), 1);
        assert!(details[0].releases.is_none());
        assert!(details[0].tracks.is_none());
        Ok(())
    }

    #[test]
    fn get_details_returns_none_for_missing_artist() -> anyhow::Result<()> {
        let db = new_test_db()?;
        let result = get_details(&db, DbId(999_999), ArtistIncludes::default())?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_details_hydrates_releases_and_tracks() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Mingus")?;
        let release_id = insert_release(&mut db, "The Black Saint")?;
        let track_id = insert_track(&mut db, "Solo Dancer")?;

        connect_artist(&mut db, release_id, artist_id)?;
        connect(&mut db, release_id, track_id)?;
        connect_artist(&mut db, track_id, artist_id)?;

        let includes = ArtistIncludes {
            releases: true,
            tracks: true,
            ..Default::default()
        };
        let details = get_details(&db, artist_id, includes)?.expect("artist should exist");

        assert_eq!(details.artist.artist_name, "Mingus");

        let releases = details.releases.expect("releases included");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].release_title, "The Black Saint");

        let tracks = details.tracks.expect("tracks included");
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].track_title, "Solo Dancer");

        Ok(())
    }

    #[test]
    fn get_relations_returns_incoming_and_outgoing_artist_relations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let person_id = insert_artist(&mut db, "Voice Actor")?;
        let character_id = insert_artist(&mut db, "Character")?;

        link_artist_relation(
            &mut db,
            person_id,
            character_id,
            lyra_db::ArtistRelationType::VoiceActor,
            None,
        )?;

        let incoming = get_relations(&db, character_id)?;
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].artist.artist_name, "Voice Actor");
        assert_eq!(incoming[0].direction, RelationDirection::Incoming);
        assert_eq!(
            incoming[0].relation_type,
            lyra_db::ArtistRelationType::VoiceActor
        );

        let outgoing = get_relations(&db, person_id)?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].artist.artist_name, "Character");
        assert_eq!(outgoing[0].direction, RelationDirection::Outgoing);
        assert_eq!(
            outgoing[0].relation_type,
            lyra_db::ArtistRelationType::VoiceActor
        );

        Ok(())
    }

    #[test]
    fn get_relations_many_returns_entries_per_requested_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let person_id = insert_artist(&mut db, "Voice Actor")?;
        let character_id = insert_artist(&mut db, "Character")?;

        link_artist_relation(
            &mut db,
            person_id,
            character_id,
            lyra_db::ArtistRelationType::VoiceActor,
            None,
        )?;

        let related = get_relations_many(&db, &[person_id, character_id, person_id])?;
        assert_eq!(related.len(), 2);
        assert_eq!(related.get(&person_id).map(Vec::len), Some(1));
        assert_eq!(related.get(&character_id).map(Vec::len), Some(1));

        Ok(())
    }

    #[test]
    fn browsing_pages_and_counts_only_credited_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        for name in ["Alpha", "Delta", "Foxtrot"] {
            insert_credited_artist(&mut db, name)?;
        }
        for name in ["Bravo", "Charlie", "Echo"] {
            insert_artist(&mut db, name)?;
        }

        assert_eq!(
            browse(&db, ArtistFilter::default(), 0, 2)?,
            (vec!["Alpha".to_string(), "Delta".to_string()], 3)
        );
        assert_eq!(
            browse(&db, ArtistFilter::default(), 2, 2)?,
            (vec!["Foxtrot".to_string()], 3)
        );
        Ok(())
    }

    #[test]
    fn uncredited_artists_stay_reachable_outside_browsing() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let credited = insert_credited_artist(&mut db, "Credited")?;
        let uncredited = insert_artist(&mut db, "Uncredited")?;
        for artist in [credited, uncredited] {
            set_artist_type(&mut db, artist, lyra_db::ArtistType::Person)?;
        }
        let by_type = ArtistFilter {
            artist_type: Some(lyra_db::ArtistType::Person),
            ..ArtistFilter::default()
        };
        assert_eq!(browse(&db, by_type, 0, 10)?.0, ["Credited"]);

        let by_id = ArtistFilter {
            ids: Some(vec![uncredited]),
            ..ArtistFilter::default()
        };
        assert_eq!(browse(&db, by_id, 0, 10)?.0, ["Uncredited"]);
        let by_id_and_type = ArtistFilter {
            ids: Some(vec![credited, uncredited]),
            artist_type: Some(lyra_db::ArtistType::Person),
            ..ArtistFilter::default()
        };
        assert_eq!(
            browse(&db, by_id_and_type, 0, 10)?.0,
            ["Credited", "Uncredited"],
            "a type filter on a lookup by id ignores credits"
        );
        assert!(get_details(&db, uncredited, ArtistIncludes::default())?.is_some());

        let user = lyra_db::test_db::insert_user(&mut db, "admin")?;
        lyra_db::favorites::add(
            &mut db,
            user,
            uncredited,
            lyra_db::favorites::FavoriteKind::Artist,
            1,
        )?;
        let viewer = catalog::Viewer::user(
            &db,
            crate::services::auth::Principal::for_user(
                &db,
                user,
                vec![lyra_db::Permission::Admin],
                HashSet::new(),
            ),
        )?;
        let names = |favorite| -> anyhow::Result<Vec<String>> {
            let query = catalog::Query::<Artists>::new(ArtistFilter {
                favorite: Some(favorite),
                ..ArtistFilter::default()
            });
            let ids = catalog::order(&db, &viewer, &query)?;
            Ok(Artists::hydrate(&db, &ids)?
                .into_iter()
                .map(|artist| artist.artist_name)
                .collect())
        };
        assert_eq!(names(true)?, ["Uncredited"], "favorites are not browsing");
        assert_eq!(names(false)?, ["Credited"]);
        Ok(())
    }

    #[test]
    fn a_voice_actor_without_credits_is_reached_only_through_relations() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let character_id = insert_credited_artist(&mut db, "Character")?;
        let actor_id = insert_artist(&mut db, "Voice Actor")?;
        link_artist_relation(
            &mut db,
            actor_id,
            character_id,
            lyra_db::ArtistRelationType::VoiceActor,
            None,
        )?;

        // Intended: relations do not credit, so browsing leaves the actor out.
        assert_eq!(
            browse(&db, ArtistFilter::default(), 0, 10)?.0,
            ["Character"]
        );
        let relations = get_relations(&db, character_id)?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].artist.artist_name, "Voice Actor");
        Ok(())
    }

    #[test]
    fn catalog_filters_by_credit_type_and_artist_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Query Release")?;

        let release_artist_id = insert_artist(&mut db, "Release Artist")?;
        set_artist_type(&mut db, release_artist_id, lyra_db::ArtistType::Person)?;
        connect_artist(&mut db, release_id, release_artist_id)?;

        let composer_id = insert_artist(&mut db, "Composer Person")?;
        set_artist_type(&mut db, composer_id, lyra_db::ArtistType::Person)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_id,
            composer_id,
            lyra_db::CreditType::Composer,
            None,
            0,
        )?;

        let group_composer_id = insert_artist(&mut db, "Composer Group")?;
        set_artist_type(&mut db, group_composer_id, lyra_db::ArtistType::Group)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_id,
            group_composer_id,
            lyra_db::CreditType::Composer,
            None,
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                releases: Some(vec![release_id]),
                artist_type: Some(lyra_db::ArtistType::Person),
                credit_types: None,
                exclude_credit_types: vec![lyra_db::CreditType::Artist],
                ..ArtistFilter::default()
            },
        )?;

        let names: Vec<&str> = artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Composer Person"]);
        Ok(())
    }

    #[test]
    fn catalog_track_scope_falls_back_to_release_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Fallback Release")?;
        let track_id = insert_track(&mut db, "Fallback Track")?;
        connect(&mut db, release_id, track_id)?;

        let composer_id = insert_artist(&mut db, "Fallback Composer")?;
        set_artist_type(&mut db, composer_id, lyra_db::ArtistType::Person)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_id,
            composer_id,
            lyra_db::CreditType::Composer,
            None,
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                tracks: Some(vec![track_id]),
                artist_type: Some(lyra_db::ArtistType::Person),
                credit_types: Some(vec![lyra_db::CreditType::Composer]),
                exclude_credit_types: Vec::new(),
                ..ArtistFilter::default()
            },
        )?;

        let names: Vec<&str> = artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Fallback Composer"]);
        Ok(())
    }

    #[test]
    fn catalog_library_scope_dedupes_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_id = insert_library(&mut db, "Music", "/music")?;
        let release_id = insert_release(&mut db, "Library Release")?;
        let track_id = insert_track(&mut db, "Library Track")?;
        connect(&mut db, library_id, release_id)?;
        connect(&mut db, release_id, track_id)?;

        let composer_id = insert_artist(&mut db, "Shared Composer")?;
        set_artist_type(&mut db, composer_id, lyra_db::ArtistType::Person)?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            release_id,
            composer_id,
            lyra_db::CreditType::Composer,
            None,
            0,
        )?;
        lyra_db::fixtures::connect_credit(
            &mut db,
            track_id,
            composer_id,
            lyra_db::CreditType::Composer,
            Some("piano"),
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                library: Some(library_id),
                artist_type: Some(lyra_db::ArtistType::Person),
                credit_types: Some(vec![lyra_db::CreditType::Composer]),
                exclude_credit_types: Vec::new(),
                ..ArtistFilter::default()
            },
        )?;

        let names: Vec<&str> = artists
            .iter()
            .map(|artist| artist.artist_name.as_str())
            .collect();
        assert_eq!(names, vec!["Shared Composer"]);
        Ok(())
    }
}
