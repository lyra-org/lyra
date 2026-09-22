// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    CountComparison,
    DbElement,
    DbError,
    DbId,
    DbType,
    DbTypeMarker,
    DbValue,
    QueryBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};

use super::NodeId;

pub(crate) const EDGE_ORDER_KEY: &str = "artist_order";

#[derive(Clone, Debug)]
pub(crate) struct CreditLinkInput {
    pub(crate) artist_id: DbId,
    pub(crate) credit_type: CreditType,
    pub(crate) detail: Option<String>,
}

#[harmony_macros::userdata(name = "CreditType")]
#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CreditType {
    #[default]
    Artist,
    Vocalist,
    Instrumentalist,
    Composer,
    Lyricist,
    Arranger,
    Writer,
    Producer,
    Conductor,
    Engineer,
    Mixer,
    Remixer,
}

impl CreditType {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Artist => "artist",
            Self::Vocalist => "vocalist",
            Self::Instrumentalist => "instrumentalist",
            Self::Composer => "composer",
            Self::Lyricist => "lyricist",
            Self::Arranger => "arranger",
            Self::Writer => "writer",
            Self::Producer => "producer",
            Self::Conductor => "conductor",
            Self::Engineer => "engineer",
            Self::Mixer => "mixer",
            Self::Remixer => "remixer",
        }
    }

    pub(crate) fn from_db_str(value: &str) -> Result<Self, DbError> {
        match value {
            "artist" => Ok(Self::Artist),
            "vocalist" => Ok(Self::Vocalist),
            "instrumentalist" => Ok(Self::Instrumentalist),
            "composer" => Ok(Self::Composer),
            "lyricist" => Ok(Self::Lyricist),
            "arranger" => Ok(Self::Arranger),
            "writer" => Ok(Self::Writer),
            "producer" => Ok(Self::Producer),
            "conductor" => Ok(Self::Conductor),
            "engineer" => Ok(Self::Engineer),
            "mixer" => Ok(Self::Mixer),
            "remixer" => Ok(Self::Remixer),
            _ => Err(DbError::serialization(
                agdb::DbErrorType::TypeError,
                format!("invalid CreditType value '{value}'"),
            )),
        }
    }
}

impl std::fmt::Display for CreditType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_db_str())
    }
}

impl From<CreditType> for DbValue {
    fn from(value: CreditType) -> Self {
        Self::from(value.as_db_str())
    }
}

impl From<&CreditType> for DbValue {
    fn from(value: &CreditType) -> Self {
        (*value).into()
    }
}

impl TryFrom<DbValue> for CreditType {
    type Error = DbError;

    fn try_from(value: DbValue) -> Result<Self, Self::Error> {
        Self::from_db_str(value.string()?)
    }
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Credit {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) credit_type: CreditType,
    pub(crate) detail: Option<String>,
}

/// Walks `Artist ← Credit ← Owner` without hydrating intermediate credits.
/// Depth-first traversal lets a bounded query stop after `offset + limit` owners.
pub(crate) fn owner_ids_by_artist<Owner: DbType>(
    db: &impl super::DbAccess,
    artist_db_id: DbId,
    offset: usize,
    limit: usize,
) -> anyhow::Result<Vec<DbId>> {
    Ok(db
        .exec(
            QueryBuilder::search()
                .depth_first()
                .to(artist_db_id)
                .offset(offset as u64)
                .limit(limit as u64)
                .where_()
                .node()
                .and()
                .distance(CountComparison::Equal(4))
                .and()
                .element::<Owner>()
                .and()
                .not_beyond()
                .distance(CountComparison::Equal(4))
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect())
}

pub(crate) fn replace_for_owner(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    desired: &[CreditLinkInput],
) -> anyhow::Result<()> {
    let existing: Vec<Credit> = db
        .exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .from(owner_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    let existing_ids: Vec<DbId> = existing
        .into_iter()
        .filter_map(|credit| credit.db_id.map(Into::into))
        .collect();
    if !existing_ids.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(existing_ids).query())?;
    }

    for (order, input) in desired.iter().enumerate() {
        let credit = Credit {
            db_id: None,
            id: nanoid::nanoid!(),
            credit_type: input.credit_type,
            detail: input.detail.clone(),
        };
        let credit_id = db
            .exec_mut(QueryBuilder::insert().element(&credit).query())?
            .ids()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("credit insert returned no id"))?;
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
                    ("owned", 1_u64).into(),
                    (EDGE_ORDER_KEY, order as u64).into(),
                ])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(credit_id)
                .to(input.artist_id)
                .query(),
        )?;
    }

    Ok(())
}

impl_luau_record_userdata!(
    Credit,
    "Credit",
    fields {
        db_id: Option<NodeId> as "db_id",
        id: String as "id",
        credit_type: CreditType as "credit_type",
        detail: Option<String> as "detail",
    },
    methods {}
);

#[derive(Clone, Debug)]
pub(crate) struct ArtistCreditInput {
    pub(crate) artist_id: DbId,
    pub(crate) name: String,
    pub(crate) join_phrase: String,
}

/// Call inside a transaction: validation, additions, and removal form one update.
pub(crate) fn reconcile_artists(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    provider_id: &str,
    desired: &[ArtistCreditInput],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        super::releases::get_by_id(db, owner_id)?.is_some()
            || super::tracks::get_by_id(db, owner_id)?.is_some(),
        "owner_id must reference a release or track"
    );
    anyhow::ensure!(!desired.is_empty(), "credits must not be empty");
    let mut target_ids = std::collections::HashSet::new();
    for input in desired {
        anyhow::ensure!(
            !input.name.trim().is_empty(),
            "credit name must not be empty"
        );
        anyhow::ensure!(
            super::artists::get_by_id(db, input.artist_id)?.is_some(),
            "artist_id must reference an artist"
        );
        anyhow::ensure!(
            super::external_ids::get_for_entity_inside_tx(db, input.artist_id)?
                .iter()
                .any(|id| id.provider_id == provider_id && !id.id_value.is_empty()),
            "artist_id must have an identity from this provider"
        );
        target_ids.insert(input.artist_id);
    }
    if super::metadata::manual_overrides::owns_field(
        db,
        owner_id,
        super::metadata::manual_overrides::ManualMetadataField::Credits,
    )? {
        return Ok(());
    }
    let existing: Vec<Credit> = db
        .exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .from(owner_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;
    let names = desired
        .iter()
        .map(|input| lyra_metadata::ArtistCreditName {
            name: input.name.clone(),
            join_phrase: input.join_phrase.clone(),
        })
        .collect::<Vec<_>>();
    let mut linked = std::collections::HashSet::new();
    let mut remove = Vec::new();
    for credit in existing {
        if credit.credit_type != CreditType::Artist || credit.detail.is_some() {
            continue;
        }
        let Some(credit_id) = credit.db_id.map(DbId::from) else {
            continue;
        };
        let edges = super::graph::direct_edges_from(db, credit_id)?;
        let Some(artist_id) = edges
            .iter()
            .find_map(|edge| (edge.to.0 > 0).then_some(edge.to))
        else {
            continue;
        };
        linked.insert(artist_id);
        if target_ids.len() < 2 || target_ids.contains(&artist_id) {
            continue;
        }
        let Some(artist) = super::artists::get_by_id(db, artist_id)? else {
            continue;
        };
        if artist.locked.unwrap_or(false)
            || super::metadata::manual_overrides::owns_field(
                db,
                artist_id,
                super::metadata::manual_overrides::ManualMetadataField::ArtistName,
            )?
            || !super::external_ids::get_for_entity_inside_tx(db, artist_id)?
                .iter()
                .all(|id| id.id_value.is_empty())
        {
            continue;
        }
        if lyra_metadata::matches_artist_credit(&artist.scan_name, &names) {
            remove.push(credit_id);
        }
    }
    for input in desired {
        if !linked.insert(input.artist_id) {
            continue;
        }
        let credit = Credit {
            db_id: None,
            id: nanoid::nanoid!(),
            credit_type: CreditType::Artist,
            detail: None,
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
                .values_uniform([("owned", 1).into()])
                .query(),
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(credit_id)
                .to(input.artist_id)
                .query(),
        )?;
    }
    if !remove.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(remove).query())?;
    }
    Ok(())
}

#[cfg(test)]
mod reconciliation_tests {
    use super::*;
    use crate::db::{
        self,
        test_db::*,
    };
    use agdb::DbAny;

    fn destination(db: &mut DbAny, name: &str, join: &str) -> anyhow::Result<ArtistCreditInput> {
        let artist_id = insert_artist(db, name)?;
        db::external_ids::upsert(
            db,
            artist_id,
            "test",
            "artist_id",
            name,
            db::IdSource::Plugin,
        )?;
        Ok(ArtistCreditInput {
            artist_id,
            name: name.into(),
            join_phrase: join.into(),
        })
    }

    fn links(
        db: &DbAny,
        owner: DbId,
    ) -> anyhow::Result<Vec<(DbId, CreditType, Option<String>, DbId)>> {
        let credits: Vec<Credit> = db
            .exec(
                QueryBuilder::select()
                    .elements::<Credit>()
                    .search()
                    .from(owner)
                    .where_()
                    .neighbor()
                    .end_where()
                    .query(),
            )?
            .try_into()?;
        let mut result = Vec::new();
        for credit in credits {
            let id: DbId = credit.db_id.unwrap().into();
            let artist = db::graph::direct_edges_from(db, id)?[0].to;
            result.push((artist, credit.credit_type, credit.detail, id));
        }
        result.sort_by_key(|entry| entry.3.0);
        Ok(result)
    }

    #[test]
    fn reconcile_preserves_shared_artist_other_roles_and_is_idempotent() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_release(&mut db, "release")?;
        let other_owner = insert_track(&mut db, "other")?;
        let combined = insert_artist(&mut db, "imase and なとり")?;
        let unrelated = insert_artist(&mut db, "Other")?;
        let local = CreditLinkInput {
            artist_id: combined,
            credit_type: CreditType::Artist,
            detail: None,
        };
        replace_for_owner(
            &mut db,
            owner,
            &[
                local.clone(),
                CreditLinkInput {
                    artist_id: combined,
                    credit_type: CreditType::Vocalist,
                    detail: None,
                },
                CreditLinkInput {
                    artist_id: combined,
                    credit_type: CreditType::Artist,
                    detail: Some("guest".into()),
                },
                CreditLinkInput {
                    artist_id: unrelated,
                    credit_type: CreditType::Artist,
                    detail: None,
                },
            ],
        )?;
        replace_for_owner(&mut db, other_owner, &[local])?;
        let desired = vec![
            destination(&mut db, "imase", " & ")?,
            destination(&mut db, "なとり", "")?,
        ];
        db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))?;
        let after = links(&db, owner)?;
        assert_eq!(after.len(), 5);
        assert!(
            !after
                .iter()
                .any(|(artist, role, detail, _)| *artist == combined
                    && *role == CreditType::Artist
                    && detail.is_none())
        );
        assert_eq!(links(&db, other_owner)?.len(), 1);
        assert_eq!(
            db::artists::get_by_id(&db, combined)?.unwrap().artist_name,
            "imase and なとり"
        );
        db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))?;
        assert_eq!(links(&db, owner)?, after);
        Ok(())
    }

    #[test]
    fn reconcile_preserves_manual_and_provider_identified_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_track(&mut db, "track")?;
        let combined = insert_artist(&mut db, "A & B")?;
        replace_for_owner(
            &mut db,
            owner,
            &[CreditLinkInput {
                artist_id: combined,
                credit_type: CreditType::Artist,
                detail: None,
            }],
        )?;
        let desired = vec![
            destination(&mut db, "A", " & ")?,
            destination(&mut db, "B", "")?,
        ];
        use db::metadata::manual_overrides::{
            self,
            ManualMetadataField,
        };
        manual_overrides::upsert(
            &mut db,
            owner,
            &[(ManualMetadataField::Credits, serde_json::json!(true))].into(),
        )?;
        let before = links(&db, owner)?;
        db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))?;
        assert_eq!(links(&db, owner)?, before);
        manual_overrides::replace(&mut db, owner, &Default::default())?;
        manual_overrides::upsert(
            &mut db,
            combined,
            &[(
                ManualMetadataField::ArtistName,
                serde_json::json!("Collaboration Band"),
            )]
            .into(),
        )?;
        db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))?;
        assert_eq!(links(&db, owner)?.len(), 3);
        manual_overrides::replace(&mut db, combined, &Default::default())?;
        db::external_ids::upsert(
            &mut db,
            combined,
            "another",
            "artist_id",
            "band",
            db::IdSource::Plugin,
        )?;
        db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))?;
        assert_eq!(links(&db, owner)?.len(), 3);
        Ok(())
    }

    #[test]
    fn invalid_destination_rolls_back_all_changes() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let owner = insert_release(&mut db, "release")?;
        let combined = insert_artist(&mut db, "A & B")?;
        replace_for_owner(
            &mut db,
            owner,
            &[CreditLinkInput {
                artist_id: combined,
                credit_type: CreditType::Artist,
                detail: None,
            }],
        )?;
        let mut desired = vec![
            destination(&mut db, "A", " & ")?,
            destination(&mut db, "B", "")?,
        ];
        let before = links(&db, owner)?;
        desired[1].artist_id = owner;
        assert!(
            db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &desired))
                .is_err()
        );
        assert_eq!(links(&db, owner)?, before);
        assert!(
            db.transaction_mut(|tx| reconcile_artists(tx, owner, "test", &[]))
                .is_err()
        );
        assert_eq!(links(&db, owner)?, before);
        Ok(())
    }
}
