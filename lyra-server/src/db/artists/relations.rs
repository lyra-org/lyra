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
    DbAny,
    DbElement,
    DbError,
    DbId,
    DbType,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryResult,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::super::{
    DbAccess,
    NodeId,
};

#[harmony_macros::userdata(name = "ArtistRelationType")]
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, DbTypeMarker,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtistRelationType {
    #[default]
    VoiceActor,
    MemberOf,
}

impl ArtistRelationType {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::VoiceActor => "voice_actor",
            Self::MemberOf => "member_of",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "voice_actor" => Ok(Self::VoiceActor),
            "member_of" => Ok(Self::MemberOf),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid ArtistRelationType value '{value}'"),
            )),
        }
    }
}

impl std::fmt::Display for ArtistRelationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<ArtistRelationType> for DbValue {
    fn from(value: ArtistRelationType) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&ArtistRelationType> for DbValue {
    fn from(value: &ArtistRelationType) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for ArtistRelationType {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ArtistRelation {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) relation_type: ArtistRelationType,
    pub(crate) attributes: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ArtistRelationLinkInput {
    pub(crate) target_artist_id: DbId,
    pub(crate) relation_type: ArtistRelationType,
    pub(crate) attributes: Option<String>,
}

impl_luau_record_userdata!(
    ArtistRelation,
    "ArtistRelation",
    fields {
        db_id: Option<NodeId> as "db_id",
        relation_type: ArtistRelationType as "relation_type",
        attributes: Option<String> as "attributes",
    },
    methods {}
);

pub(crate) fn link(
    db: &mut impl DbAccess,
    from_artist_id: DbId,
    to_artist_id: DbId,
    relation_type: ArtistRelationType,
    attributes: Option<String>,
) -> anyhow::Result<DbId> {
    let edge_ids = super::super::graph::direct_edge_ids(db, from_artist_id, to_artist_id)?;
    let mut existing_edge_id = None;
    let mut duplicate_edge_ids = Vec::new();

    if !edge_ids.is_empty() {
        let result = db.exec(QueryBuilder::select().ids(&edge_ids).query())?;
        for element in result.elements {
            let Ok(existing) = ArtistRelation::from_db_element(&element) else {
                continue;
            };
            if existing.relation_type != relation_type {
                continue;
            }
            if existing_edge_id.is_some() {
                duplicate_edge_ids.push(element.id);
            } else {
                existing_edge_id = Some(element.id);
            }
        }
    }

    if let Some(edge_id) = existing_edge_id {
        if !duplicate_edge_ids.is_empty() {
            db.exec_mut(QueryBuilder::remove().ids(&duplicate_edge_ids).query())?;
        }
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .ids(edge_id)
                .from(from_artist_id)
                .to(to_artist_id)
                .values_uniform(ArtistRelation {
                    db_id: None,
                    relation_type,
                    attributes: attributes.clone(),
                })
                .query(),
        )?;
        if attributes.is_none() {
            db.exec_mut(
                QueryBuilder::remove()
                    .values(["attributes".to_string()])
                    .ids(edge_id)
                    .query(),
            )?;
        }
        return Ok(edge_id);
    }

    let result = db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(from_artist_id)
            .to(to_artist_id)
            .values_uniform(ArtistRelation {
                db_id: None,
                relation_type,
                attributes,
            })
            .query(),
    )?;
    let edge_id = result.elements.first().map(|e| e.id).unwrap_or(DbId(0));
    Ok(edge_id)
}

pub(crate) fn exists(
    db: &impl DbAccess,
    from_artist_id: DbId,
    to_artist_id: DbId,
    relation_type: ArtistRelationType,
) -> anyhow::Result<bool> {
    let edge_ids = super::super::graph::direct_edge_ids(db, from_artist_id, to_artist_id)?;
    if edge_ids.is_empty() {
        return Ok(false);
    }
    let result = db.exec(QueryBuilder::select().ids(&edge_ids).query())?;
    Ok(result.elements.iter().any(|element| {
        ArtistRelation::from_db_element(element)
            .is_ok_and(|relation| relation.relation_type == relation_type)
    }))
}

pub(crate) fn replace_from(
    db: &mut impl DbAccess,
    from_artist_id: DbId,
    desired: &[ArtistRelationLinkInput],
) -> anyhow::Result<()> {
    let existing = db.exec(
        QueryBuilder::select()
            .elements::<ArtistRelation>()
            .search()
            .from(from_artist_id)
            .where_()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    let edge_ids: Vec<DbId> = existing
        .elements
        .into_iter()
        .filter_map(|element| (element.id.0 < 0).then_some(element.id))
        .collect();
    if !edge_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(edge_ids).query())?;
    }

    for input in desired {
        link(
            db,
            from_artist_id,
            input.target_artist_id,
            input.relation_type,
            input.attributes.clone(),
        )?;
    }
    Ok(())
}

fn collect_relations<F>(
    result: QueryResult,
    relation_type: Option<ArtistRelationType>,
    extract_peer: F,
) -> anyhow::Result<Vec<(ArtistRelation, DbId)>>
where
    F: Fn(&DbElement) -> Option<DbId>,
{
    let mut relations = Vec::new();
    for element in &result.elements {
        if element.id.0 >= 0 {
            continue;
        }
        if let Ok(relation) = ArtistRelation::from_db_element(element) {
            if let Some(filter_type) = relation_type
                && relation.relation_type != filter_type
            {
                continue;
            }
            if let Some(peer_id) = extract_peer(element) {
                relations.push((relation, peer_id));
            }
        }
    }
    Ok(relations)
}

pub(crate) fn get_relations_to(
    db: &impl DbAccess,
    artist_id: DbId,
    relation_type: Option<ArtistRelationType>,
) -> anyhow::Result<Vec<(ArtistRelation, DbId)>> {
    let result = db.exec(
        QueryBuilder::select()
            .elements::<ArtistRelation>()
            .search()
            .to(artist_id)
            .where_()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    collect_relations(result, relation_type, |e| (e.from.0 != 0).then_some(e.from))
}

pub(crate) fn get_relations_from(
    db: &impl DbAccess,
    artist_id: DbId,
    relation_type: Option<ArtistRelationType>,
) -> anyhow::Result<Vec<(ArtistRelation, DbId)>> {
    let result = db.exec(
        QueryBuilder::select()
            .elements::<ArtistRelation>()
            .search()
            .from(artist_id)
            .where_()
            .distance(CountComparison::Equal(1))
            .end_where()
            .query(),
    )?;
    collect_relations(result, relation_type, |e| (e.to.0 != 0).then_some(e.to))
}

pub(crate) fn get_related_targets_from_many(
    db: &DbAny,
    from_artist_ids: &[DbId],
    candidate_target_ids: &[DbId],
    relation_type: ArtistRelationType,
) -> anyhow::Result<HashMap<DbId, HashSet<DbId>>> {
    let unique_from_artist_ids = super::super::dedup_positive_ids(from_artist_ids);
    let candidate_target_ids: HashSet<DbId> =
        super::super::dedup_positive_ids(candidate_target_ids)
            .into_iter()
            .collect();

    let mut related_targets: HashMap<DbId, HashSet<DbId>> = unique_from_artist_ids
        .iter()
        .copied()
        .map(|artist_id| (artist_id, HashSet::new()))
        .collect();
    if unique_from_artist_ids.is_empty() || candidate_target_ids.is_empty() {
        return Ok(related_targets);
    }

    for from_artist_id in unique_from_artist_ids {
        let targets = related_targets
            .get_mut(&from_artist_id)
            .expect("batch relation map initialized for every source artist");
        for (_, target_id) in get_relations_from(db, from_artist_id, Some(relation_type))? {
            if candidate_target_ids.contains(&target_id) {
                targets.insert(target_id);
            }
        }
    }

    Ok(related_targets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        insert_artist,
        new_test_db,
    };

    #[test]
    fn link_reuses_existing_relation_edge() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let voice_actor_id = insert_artist(&mut db, "Voice Actor")?;
        let character_id = insert_artist(&mut db, "Character")?;

        let first_edge_id = link(
            &mut db,
            voice_actor_id,
            character_id,
            ArtistRelationType::VoiceActor,
            Some("lead".to_string()),
        )?;
        let second_edge_id = link(
            &mut db,
            voice_actor_id,
            character_id,
            ArtistRelationType::VoiceActor,
            Some("main".to_string()),
        )?;

        assert_eq!(second_edge_id, first_edge_id);
        let relations =
            get_relations_from(&db, voice_actor_id, Some(ArtistRelationType::VoiceActor))?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].0.attributes.as_deref(), Some("main"));

        let third_edge_id = link(
            &mut db,
            voice_actor_id,
            character_id,
            ArtistRelationType::VoiceActor,
            None,
        )?;

        assert_eq!(third_edge_id, first_edge_id);
        let relations =
            get_relations_from(&db, voice_actor_id, Some(ArtistRelationType::VoiceActor))?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].0.attributes, None);

        Ok(())
    }

    #[test]
    fn link_removes_duplicate_relation_edges() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let voice_actor_id = insert_artist(&mut db, "Voice Actor")?;
        let character_id = insert_artist(&mut db, "Character")?;

        for attributes in [Some("first".to_string()), Some("second".to_string())] {
            db.exec_mut(
                QueryBuilder::insert()
                    .edges()
                    .from(voice_actor_id)
                    .to(character_id)
                    .values_uniform(ArtistRelation {
                        db_id: None,
                        relation_type: ArtistRelationType::VoiceActor,
                        attributes,
                    })
                    .query(),
            )?;
        }

        link(
            &mut db,
            voice_actor_id,
            character_id,
            ArtistRelationType::VoiceActor,
            Some("canonical".to_string()),
        )?;

        let relations =
            get_relations_from(&db, voice_actor_id, Some(ArtistRelationType::VoiceActor))?;
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].0.attributes.as_deref(), Some("canonical"));

        Ok(())
    }

    #[test]
    fn get_related_targets_from_many_filters_by_relation_and_target() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let voice_actor_id = insert_artist(&mut db, "Voice Actor")?;
        let unrelated_actor_id = insert_artist(&mut db, "Unrelated")?;
        let character_id = insert_artist(&mut db, "Character")?;
        let other_character_id = insert_artist(&mut db, "Other Character")?;

        link(
            &mut db,
            voice_actor_id,
            character_id,
            ArtistRelationType::VoiceActor,
            None,
        )?;
        link(
            &mut db,
            voice_actor_id,
            other_character_id,
            ArtistRelationType::MemberOf,
            None,
        )?;

        let related = get_related_targets_from_many(
            &db,
            &[voice_actor_id, unrelated_actor_id],
            &[character_id],
            ArtistRelationType::VoiceActor,
        )?;

        assert_eq!(
            related
                .get(&voice_actor_id)
                .expect("voice actor batch result should exist"),
            &HashSet::from([character_id])
        );
        assert!(
            related
                .get(&unrelated_actor_id)
                .expect("unrelated actor batch result should exist")
                .is_empty()
        );

        Ok(())
    }
}
