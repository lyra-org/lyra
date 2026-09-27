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

use crate::db::{
    self,
    Artist,
    ArtistRelationType,
    Release,
    Track,
};
use crate::services::entities::relations;

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

    let incoming = db::artists::relations::get_relations_to(db, artist_db_id, None)?;
    for (relation, peer_id) in incoming {
        if let Some(peer_artist) = db::artists::get_by_id(db, peer_id)? {
            resolved.push(ResolvedRelation {
                relation_type: relation.relation_type,
                attributes: relation.attributes,
                direction: RelationDirection::Incoming,
                artist: peer_artist,
            });
        }
    }

    let outgoing = db::artists::relations::get_relations_from(db, artist_db_id, None)?;
    for (relation, peer_id) in outgoing {
        if let Some(peer_artist) = db::artists::get_by_id(db, peer_id)? {
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
    let Some(artist) = db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(None);
    };

    Ok(list_details_for_artists(db, includes, vec![artist])?
        .into_iter()
        .next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::artists::relations::link as link_artist_relation;
    use crate::db::test_db::{
        connect,
        connect_artist,
        insert_artist,
        insert_library,
        insert_release,
        insert_track,
        new_test_db,
    };
    use crate::services::catalog::{
        self,
        artists::{
            ArtistFilter,
            Artists,
        },
        pipeline::Catalog,
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

    fn set_artist_type(
        db: &mut DbAny,
        artist_db_id: DbId,
        artist_type: db::ArtistType,
    ) -> anyhow::Result<()> {
        let mut artist = db::artists::get_by_id(db, artist_db_id)?
            .ok_or_else(|| anyhow::anyhow!("artist should exist"))?;
        artist.set_artist_type(artist_type);
        db::artists::update(db, &artist)
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
        insert_artist(&mut db, "Solo Artist")?;

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
            db::ArtistRelationType::VoiceActor,
            None,
        )?;

        let incoming = get_relations(&db, character_id)?;
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].artist.artist_name, "Voice Actor");
        assert_eq!(incoming[0].direction, RelationDirection::Incoming);
        assert_eq!(
            incoming[0].relation_type,
            db::ArtistRelationType::VoiceActor
        );

        let outgoing = get_relations(&db, person_id)?;
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].artist.artist_name, "Character");
        assert_eq!(outgoing[0].direction, RelationDirection::Outgoing);
        assert_eq!(
            outgoing[0].relation_type,
            db::ArtistRelationType::VoiceActor
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
            db::ArtistRelationType::VoiceActor,
            None,
        )?;

        let related = get_relations_many(&db, &[person_id, character_id, person_id])?;
        assert_eq!(related.len(), 2);
        assert_eq!(related.get(&person_id).map(Vec::len), Some(1));
        assert_eq!(related.get(&character_id).map(Vec::len), Some(1));

        Ok(())
    }

    #[test]
    fn catalog_filters_by_credit_type_and_artist_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Query Release")?;

        let release_artist_id = insert_artist(&mut db, "Release Artist")?;
        set_artist_type(&mut db, release_artist_id, db::ArtistType::Person)?;
        connect_artist(&mut db, release_id, release_artist_id)?;

        let composer_id = insert_artist(&mut db, "Composer Person")?;
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        db::test_db::connect_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
            0,
        )?;

        let group_composer_id = insert_artist(&mut db, "Composer Group")?;
        set_artist_type(&mut db, group_composer_id, db::ArtistType::Group)?;
        db::test_db::connect_credit(
            &mut db,
            release_id,
            group_composer_id,
            db::CreditType::Composer,
            None,
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                releases: Some(vec![release_id]),
                artist_type: Some(db::ArtistType::Person),
                credit_types: None,
                exclude_credit_types: vec![db::CreditType::Artist],
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
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        db::test_db::connect_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                tracks: Some(vec![track_id]),
                artist_type: Some(db::ArtistType::Person),
                credit_types: Some(vec![db::CreditType::Composer]),
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
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        db::test_db::connect_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
            0,
        )?;
        db::test_db::connect_credit(
            &mut db,
            track_id,
            composer_id,
            db::CreditType::Composer,
            Some("piano"),
            0,
        )?;

        let artists = credited_names(
            &db,
            ArtistFilter {
                library: Some(library_id),
                artist_type: Some(db::ArtistType::Person),
                credit_types: Some(vec![db::CreditType::Composer]),
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
