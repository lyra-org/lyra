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
    CreditType,
    ListOptions,
    PagedResult,
    Release,
    ResolveId,
    Track,
};
use crate::services::entities::{
    ResolvedCreditedArtist,
    TrackCreditedArtistContext,
    resolve_release_credited_artists_map,
    resolve_track_credited_artists_with_context,
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

#[derive(Clone, Debug, Default)]
pub(crate) struct CreditedArtistFilters {
    pub(crate) artist_type: Option<db::ArtistType>,
    pub(crate) credit_types: Option<Vec<CreditType>>,
    pub(crate) exclude_credit_types: Option<Vec<CreditType>>,
}

fn collect_scoped_credit_owner_ids(
    db: &DbAny,
    scope: Option<&ResolveId>,
) -> anyhow::Result<(Vec<DbId>, Vec<DbId>)> {
    let global_release_ids = || -> anyhow::Result<Vec<DbId>> {
        Ok(db::releases::get(db, "releases")?
            .into_iter()
            .filter_map(|release| release.db_id.map(Into::into))
            .collect())
    };
    let global_track_ids = || -> anyhow::Result<Vec<DbId>> {
        Ok(db::tracks::get(db, "tracks")?
            .into_iter()
            .filter_map(|track| track.db_id.map(Into::into))
            .collect())
    };

    let (release_ids, track_ids) = match scope {
        None => (global_release_ids()?, global_track_ids()?),
        Some(ResolveId::Alias(alias)) if matches!(alias.as_str(), "artists" | "libraries") => {
            (global_release_ids()?, global_track_ids()?)
        }
        Some(ResolveId::Alias(alias)) if alias == "releases" => (global_release_ids()?, Vec::new()),
        Some(ResolveId::Alias(alias)) if alias == "tracks" => (Vec::new(), global_track_ids()?),
        Some(scope) => {
            let Some(scope_db_id) = scope.to_db_id(db)? else {
                return Ok((Vec::new(), Vec::new()));
            };

            if db::libraries::get_by_id(db, scope_db_id)?.is_some() {
                (
                    db::releases::get_direct(db, scope_db_id)?
                        .into_iter()
                        .filter_map(|release| release.db_id.map(Into::into))
                        .collect(),
                    db::tracks::get_by_library(db, scope_db_id)?
                        .into_iter()
                        .filter_map(|track| track.db_id.map(Into::into))
                        .collect(),
                )
            } else if db::releases::get_by_id(db, scope_db_id)?.is_some() {
                (vec![scope_db_id], Vec::new())
            } else if db::tracks::get_by_id(db, scope_db_id)?.is_some() {
                (Vec::new(), vec![scope_db_id])
            } else {
                (Vec::new(), Vec::new())
            }
        }
    };

    Ok((
        db::dedup_positive_ids(&release_ids),
        db::dedup_positive_ids(&track_ids),
    ))
}

fn collect_scoped_credited_artists(
    db: &DbAny,
    scope: Option<&ResolveId>,
) -> anyhow::Result<Vec<ResolvedCreditedArtist>> {
    let (release_ids, track_ids) = collect_scoped_credit_owner_ids(db, scope)?;
    if release_ids.is_empty() && track_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut release_credits_by_owner = if release_ids.is_empty() {
        HashMap::new()
    } else {
        resolve_release_credited_artists_map(db, &release_ids)?
    };

    let mut track_credits_by_owner = if track_ids.is_empty() {
        HashMap::new()
    } else {
        let ctx = TrackCreditedArtistContext {
            releases_by_track: None,
            credited_artists_by_release: Some(&release_credits_by_owner),
            scope_release_id: None,
        };
        resolve_track_credited_artists_with_context(db, &track_ids, &ctx)?
    };

    let mut credited = Vec::new();
    for release_id in release_ids {
        if let Some(artists) = release_credits_by_owner.remove(&release_id) {
            credited.extend(artists);
        }
    }
    for track_id in track_ids {
        if let Some(artists) = track_credits_by_owner.remove(&track_id) {
            credited.extend(artists);
        }
    }

    Ok(credited)
}

pub(crate) fn query_credited(
    db: &DbAny,
    scope: Option<&ResolveId>,
    filters: &CreditedArtistFilters,
    options: &ListOptions,
) -> anyhow::Result<PagedResult<Artist>> {
    let include_credit_types: Option<HashSet<CreditType>> = filters
        .credit_types
        .as_ref()
        .map(|values| values.iter().copied().collect());
    let exclude_credit_types: Option<HashSet<CreditType>> = filters
        .exclude_credit_types
        .as_ref()
        .map(|values| values.iter().copied().collect());

    let credited = collect_scoped_credited_artists(db, scope)?;
    let mut seen_artist_ids = HashSet::new();
    let mut artists = Vec::new();

    for credited_artist in credited {
        if let Some(ref include_credit_types) = include_credit_types
            && !include_credit_types.contains(&credited_artist.credit.credit_type)
        {
            continue;
        }
        if let Some(ref exclude_credit_types) = exclude_credit_types
            && exclude_credit_types.contains(&credited_artist.credit.credit_type)
        {
            continue;
        }
        if let Some(artist_type) = filters.artist_type
            && credited_artist.artist.artist_type != Some(artist_type)
        {
            continue;
        }

        let Some(artist_db_id) = credited_artist.artist.db_id.clone().map(DbId::from) else {
            continue;
        };
        if seen_artist_ids.insert(artist_db_id) {
            artists.push(credited_artist.artist);
        }
    }

    Ok(db::artists::query_items(artists, options))
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

pub(crate) fn list_details(
    db: &DbAny,
    includes: ArtistIncludes,
) -> anyhow::Result<Vec<ArtistDetails>> {
    let artists = db::artists::get(db, "artists")?;
    let mut details = Vec::with_capacity(artists.len());

    for artist in artists {
        let artist_db_id = artist
            .db_id
            .clone()
            .map(DbId::from)
            .ok_or_else(|| anyhow::anyhow!("artist missing db id"))?;
        let releases = if includes.releases {
            Some(db::releases::get_by_artist(db, artist_db_id)?)
        } else {
            None
        };
        let tracks = if includes.tracks {
            Some(db::tracks::get_by_artist(db, artist_db_id)?)
        } else {
            None
        };
        let relations = if includes.relations {
            Some(get_relations(db, artist_db_id)?)
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

    let releases = if includes.releases {
        Some(db::releases::get_by_artist(db, artist_db_id)?)
    } else {
        None
    };
    let tracks = if includes.tracks {
        Some(db::tracks::get_by_artist(db, artist_db_id)?)
    } else {
        None
    };
    let relations = if includes.relations {
        Some(get_relations(db, artist_db_id)?)
    } else {
        None
    };

    Ok(Some(ArtistDetails {
        artist,
        releases,
        tracks,
        relations,
    }))
}

pub(crate) fn update(
    db: &mut DbAny,
    artist_db_id: DbId,
    update_name: Option<String>,
    update_sort_name: Option<Option<String>>,
    update_description: Option<Option<String>>,
) -> anyhow::Result<Option<Artist>> {
    let Some(artist_entity) = db::artists::get_by_id(db, artist_db_id)? else {
        return Ok(None);
    };

    let Artist {
        id: entity_id,
        artist_name,
        scan_name,
        sort_name: artist_sort_name,
        description: artist_description,
        verified,
        created_at,
        ..
    } = artist_entity;
    let updated_name = update_name.unwrap_or(artist_name);
    let mut updated_sort_name = artist_sort_name;
    let mut updated_description = artist_description;

    let mut clear_sort_name = false;
    if let Some(sort_name) = update_sort_name {
        match sort_name {
            Some(value) => updated_sort_name = Some(value),
            None => {
                updated_sort_name = None;
                clear_sort_name = true;
            }
        }
    }

    let mut clear_description = false;
    if let Some(description) = update_description {
        match description {
            Some(value) => updated_description = Some(value),
            None => {
                updated_description = None;
                clear_description = true;
            }
        }
    }

    let updated = Artist {
        db_id: Some(artist_db_id.into()),
        id: entity_id,
        artist_name: updated_name,
        scan_name,
        sort_name: updated_sort_name,
        artist_type: None,
        description: updated_description,
        verified,
        locked: None,
        created_at,
    };

    db::artists::update_with_clears(db, &updated, clear_sort_name, clear_description)?;
    Ok(Some(updated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::ResolveId;
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
    use agdb::QueryBuilder;
    use nanoid::nanoid;

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
    fn query_credited_filters_by_credit_type_and_artist_type() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Query Release")?;

        let release_artist_id = insert_artist(&mut db, "Release Artist")?;
        set_artist_type(&mut db, release_artist_id, db::ArtistType::Person)?;
        connect_artist(&mut db, release_id, release_artist_id)?;

        let composer_id = insert_artist(&mut db, "Composer Person")?;
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        connect_artist_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
        )?;

        let group_composer_id = insert_artist(&mut db, "Composer Group")?;
        set_artist_type(&mut db, group_composer_id, db::ArtistType::Group)?;
        connect_artist_credit(
            &mut db,
            release_id,
            group_composer_id,
            db::CreditType::Composer,
            None,
        )?;

        let scope = ResolveId::DbId(release_id);
        let result = query_credited(
            &db,
            Some(&scope),
            &CreditedArtistFilters {
                artist_type: Some(db::ArtistType::Person),
                credit_types: None,
                exclude_credit_types: Some(vec![db::CreditType::Artist]),
            },
            &ListOptions {
                sort: vec![],
                offset: None,
                limit: None,
                search_term: None,
            },
        )?;

        assert_eq!(result.total_count, 1);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].artist_name, "Composer Person");
        Ok(())
    }

    #[test]
    fn query_credited_track_scope_falls_back_to_release_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Fallback Release")?;
        let track_id = insert_track(&mut db, "Fallback Track")?;
        connect(&mut db, release_id, track_id)?;

        let composer_id = insert_artist(&mut db, "Fallback Composer")?;
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        connect_artist_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
        )?;

        let scope = ResolveId::DbId(track_id);
        let result = query_credited(
            &db,
            Some(&scope),
            &CreditedArtistFilters {
                artist_type: Some(db::ArtistType::Person),
                credit_types: Some(vec![db::CreditType::Composer]),
                exclude_credit_types: None,
            },
            &ListOptions {
                sort: vec![],
                offset: None,
                limit: None,
                search_term: None,
            },
        )?;

        assert_eq!(result.total_count, 1);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].artist_name, "Fallback Composer");
        Ok(())
    }

    #[test]
    fn query_credited_library_scope_dedupes_artists() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let library_id = insert_library(&mut db, "Music", "/music")?;
        let release_id = insert_release(&mut db, "Library Release")?;
        let track_id = insert_track(&mut db, "Library Track")?;
        connect(&mut db, library_id, release_id)?;
        connect(&mut db, release_id, track_id)?;

        let composer_id = insert_artist(&mut db, "Shared Composer")?;
        set_artist_type(&mut db, composer_id, db::ArtistType::Person)?;
        connect_artist_credit(
            &mut db,
            release_id,
            composer_id,
            db::CreditType::Composer,
            None,
        )?;
        connect_artist_credit(
            &mut db,
            track_id,
            composer_id,
            db::CreditType::Composer,
            Some("piano"),
        )?;

        let scope = ResolveId::DbId(library_id);
        let result = query_credited(
            &db,
            Some(&scope),
            &CreditedArtistFilters {
                artist_type: Some(db::ArtistType::Person),
                credit_types: Some(vec![db::CreditType::Composer]),
                exclude_credit_types: None,
            },
            &ListOptions {
                sort: vec![],
                offset: None,
                limit: None,
                search_term: None,
            },
        )?;

        assert_eq!(result.total_count, 1);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].artist_name, "Shared Composer");
        Ok(())
    }
}
