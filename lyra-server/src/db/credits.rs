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
