// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    HashMap,
    HashSet,
};

use agdb::{
    DbElement,
    DbError,
    DbId,
    DbType,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
    QueryResult,
};
use schemars::JsonSchema;
use serde::{
    Deserialize,
    Serialize,
};

use super::super::{
    DbAny,
    NodeId,
};

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
    DbTypeMarker,
)]
#[serde(rename_all = "snake_case")]
#[harmony_macros::enumeration]
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
            _ => Err(DbError::from(format!(
                "invalid ArtistRelationType value '{value}'"
            ))),
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

harmony_macros::compile!(type_path = ArtistRelationType, variants = true);

#[derive(DbElement, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[harmony_macros::structure]
pub(crate) struct ArtistRelation {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) relation_type: ArtistRelationType,
    pub(crate) attributes: Option<String>,
}

pub(crate) fn link(
    db: &mut DbAny,
    from_artist_id: DbId,
    to_artist_id: DbId,
    relation_type: ArtistRelationType,
    attributes: Option<String>,
) -> anyhow::Result<DbId> {
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
            if let Some(filter_type) = relation_type {
                if relation.relation_type != filter_type {
                    continue;
                }
            }
            if let Some(peer_id) = extract_peer(element) {
                relations.push((relation, peer_id));
            }
        }
    }
    Ok(relations)
}

pub(crate) fn get_relations_to(
    db: &DbAny,
    artist_id: DbId,
    relation_type: Option<ArtistRelationType>,
) -> anyhow::Result<Vec<(ArtistRelation, DbId)>> {
    let result = db.exec(
        QueryBuilder::select()
            .elements::<ArtistRelation>()
            .search()
            .to(artist_id)
            .limit(100)
            .query(),
    )?;
    collect_relations(result, relation_type, |e| e.from)
}

pub(crate) fn get_relations_from(
    db: &DbAny,
    artist_id: DbId,
    relation_type: Option<ArtistRelationType>,
) -> anyhow::Result<Vec<(ArtistRelation, DbId)>> {
    let result = db.exec(
        QueryBuilder::select()
            .elements::<ArtistRelation>()
            .search()
            .from(artist_id)
            .limit(100)
            .query(),
    )?;
    collect_relations(result, relation_type, |e| e.to)
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
