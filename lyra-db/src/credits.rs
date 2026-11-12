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

#[derive(Clone, Debug)]
pub struct CreditLinkInput {
    pub artist_id: DbId,
    pub credit_type: CreditType,
    pub detail: Option<String>,
}

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    DbTypeMarker,
)]
#[serde(rename_all = "lowercase")]
pub enum CreditType {
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
    pub const ALL: [Self; 12] = [
        Self::Artist,
        Self::Vocalist,
        Self::Instrumentalist,
        Self::Composer,
        Self::Lyricist,
        Self::Arranger,
        Self::Writer,
        Self::Producer,
        Self::Conductor,
        Self::Engineer,
        Self::Mixer,
        Self::Remixer,
    ];

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

/// Values on an `owner → artist` credit edge. An owner may credit one artist
/// several times, one edge per credit.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub struct Credit {
    pub credit_type: CreditType,
    pub detail: Option<String>,
    pub artist_order: u64,
}

impl Credit {
    /// What makes two credits of one owner for one artist the same credit.
    pub fn role(&self) -> (CreditType, Option<&str>) {
        (self.credit_type, self.detail.as_deref())
    }
}

/// A stored credit edge with its endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreditLink {
    pub edge_id: DbId,
    pub owner_id: DbId,
    pub artist_id: DbId,
    pub credit: Credit,
}

/// The credits a set of links holds, for asking whether another link would repeat one.
pub struct CreditSet(std::collections::HashSet<(DbId, DbId, CreditType, Option<String>)>);

impl CreditSet {
    pub fn new(links: &[CreditLink]) -> Self {
        Self(
            links
                .iter()
                .map(|link| {
                    let (credit_type, detail) = link.credit.role();
                    (
                        link.owner_id,
                        link.artist_id,
                        credit_type,
                        detail.map(str::to_string),
                    )
                })
                .collect(),
        )
    }

    /// Whether `owner_id` already credits `artist_id` in `role`.
    pub fn contains(
        &self,
        owner_id: DbId,
        artist_id: DbId,
        (credit_type, detail): (CreditType, Option<&str>),
    ) -> bool {
        self.0
            .contains(&(owner_id, artist_id, credit_type, detail.map(str::to_string)))
    }
}

fn links_from(result: agdb::QueryResult) -> anyhow::Result<Vec<CreditLink>> {
    result
        .elements
        .iter()
        .filter(|element| element.id.0 < 0)
        .map(|element| {
            Ok(CreditLink {
                edge_id: element.id,
                owner_id: element.from,
                artist_id: element.to,
                credit: Credit::from_db_element(element)?,
            })
        })
        .collect()
}

/// The artist key holding whether any release or track credits the artist.
pub(crate) const CREDITED: &str = "credited";

/// Inserts a credit edge. Every credit insert goes through here.
pub fn link(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    artist_id: DbId,
    credit: &Credit,
) -> anyhow::Result<DbId> {
    let edge_id = db
        .exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(owner_id)
                .to(artist_id)
                .values_uniform(credit.to_db_values())
                .query(),
        )?
        .ids()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("credit insert returned no id"))?;
    set_credited(db, artist_id, true)?;
    Ok(edge_id)
}

fn set_credited(
    db: &mut impl super::DbAccess,
    artist_id: DbId,
    credited: bool,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([(CREDITED, credited).into()])
            .ids(artist_id)
            .query(),
    )?;
    Ok(())
}

/// Recomputes the credited flag of each artist in `artist_ids` that still exists.
pub(crate) fn recompute_credited(
    db: &mut impl super::DbAccess,
    artist_ids: &[DbId],
) -> anyhow::Result<()> {
    for artist_id in
        super::graph::existing_ids(db, &super::dedup_positive_ids(artist_ids), "Artist")?
    {
        let credited = db
            .exec(
                QueryBuilder::search()
                    .to(artist_id)
                    .limit(1)
                    .where_()
                    .distance(CountComparison::Equal(1))
                    .and()
                    .element::<Credit>()
                    .query(),
            )?
            .result
            > 0;
        set_credited(db, artist_id, credited)?;
    }
    Ok(())
}

/// The artists the owners in `owner_ids` credit, each once.
pub fn credited_artist_ids(
    db: &impl super::DbAccess,
    owner_ids: &[DbId],
) -> anyhow::Result<Vec<DbId>> {
    let mut artist_ids = Vec::new();
    for &owner_id in owner_ids {
        artist_ids.extend(
            db.exec(
                QueryBuilder::search()
                    .from(owner_id)
                    .where_()
                    .distance(CountComparison::Equal(1))
                    .and()
                    .element::<Credit>()
                    .query(),
            )?
            .elements
            .into_iter()
            .map(|edge| edge.to),
        );
    }
    Ok(super::dedup_positive_ids(&artist_ids))
}

pub fn links_for_owner(
    db: &impl super::DbAccess,
    owner_id: DbId,
) -> anyhow::Result<Vec<CreditLink>> {
    links_from(
        db.exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .from(owner_id)
                .where_()
                .distance(CountComparison::Equal(1))
                .end_where()
                .query(),
        )?,
    )
}

pub fn links_to_artist(
    db: &impl super::DbAccess,
    artist_id: DbId,
) -> anyhow::Result<Vec<CreditLink>> {
    links_from(
        db.exec(
            QueryBuilder::select()
                .elements::<Credit>()
                .search()
                .to(artist_id)
                .where_()
                .distance(CountComparison::Equal(1))
                .end_where()
                .query(),
        )?,
    )
}

/// Owners crediting `artist_id`, each once.
pub fn crediting_owner_ids(
    db: &impl super::DbAccess,
    artist_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let mut seen = std::collections::HashSet::new();
    Ok(db
        .exec(
            QueryBuilder::search()
                .to(artist_id)
                .where_()
                .distance(CountComparison::Equal(1))
                .and()
                .element::<Credit>()
                .query(),
        )?
        .elements
        .into_iter()
        .map(|edge| edge.from)
        .filter(|owner_id| seen.insert(*owner_id))
        .collect())
}

/// Walks `Artist ← credit edge ← Owner` depth-first, so a bounded query stops
/// after `offset + limit` owners.
pub fn owner_ids_by_artist<Owner: DbType>(
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
                .distance(CountComparison::Equal(2))
                .and()
                .element::<Owner>()
                .and()
                .beyond()
                .where_()
                .node()
                .or()
                .element::<Credit>()
                .end_where()
                .query(),
        )?
        .ids()
        .into_iter()
        .filter(|id| id.0 > 0)
        .collect())
}

pub fn replace_for_owner(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    desired: &[CreditLinkInput],
) -> anyhow::Result<()> {
    let existing = links_for_owner(db, owner_id)?;
    if !existing.is_empty() {
        db.exec_mut(
            QueryBuilder::remove()
                .ids(existing.iter().map(|link| link.edge_id).collect::<Vec<_>>())
                .query(),
        )?;
    }

    for (order, input) in desired.iter().enumerate() {
        link(
            db,
            owner_id,
            input.artist_id,
            &Credit {
                credit_type: input.credit_type,
                detail: input.detail.clone(),
                artist_order: order as u64,
            },
        )?;
    }

    let uncredited: Vec<DbId> = existing
        .into_iter()
        .map(|link| link.artist_id)
        .filter(|artist_id| !desired.iter().any(|input| input.artist_id == *artist_id))
        .collect();
    crate::orphans::mark_artists(db, &uncredited)?;
    recompute_credited(db, &uncredited)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::*;
    use crate::test_db::*;
    use agdb::DbAny;

    fn links(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<CreditLink>> {
        let mut links = links_for_owner(db, owner)?;
        links.sort_unstable_by_key(|link| link.edge_id.0);
        Ok(links)
    }

    #[test]
    fn links_only_walk_the_owner_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "release")?;
        let track = insert_track(&mut db, "track")?;
        crate::fixtures::link_track(&mut db, release, track)?;
        let release_artist = insert_artist(&mut db, "Release Artist")?;
        let track_artist = insert_artist(&mut db, "Track Artist")?;
        connect_artist(&mut db, release, release_artist)?;
        connect_artist(&mut db, track, track_artist)?;

        let release_links = links(&db, release)?;
        assert_eq!(release_links.len(), 1);
        assert_eq!(release_links[0].artist_id, release_artist);
        assert_eq!(release_links[0].owner_id, release);
        Ok(())
    }

    #[test]
    fn one_owner_can_credit_an_artist_in_several_roles() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track = insert_track(&mut db, "track")?;
        let artist = insert_artist(&mut db, "Artist")?;
        let inputs = [
            (CreditType::Artist, None),
            (CreditType::Composer, None),
            (CreditType::Artist, Some("feat.".to_string())),
        ]
        .map(|(credit_type, detail)| CreditLinkInput {
            artist_id: artist,
            credit_type,
            detail,
        });
        replace_for_owner(&mut db, track, &inputs)?;

        let mut stored = links_for_owner(&db, track)?;
        stored.sort_by_key(|link| link.credit.artist_order);
        let stored = stored
            .into_iter()
            .map(|link| (link.artist_id, link.credit))
            .collect::<Vec<_>>();
        let expected = inputs
            .iter()
            .enumerate()
            .map(|(order, input)| {
                (
                    artist,
                    Credit {
                        credit_type: input.credit_type,
                        detail: input.detail.clone(),
                        artist_order: order as u64,
                    },
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(stored, expected);
        assert_eq!(
            owner_ids_by_artist::<crate::Track>(&db, artist, 0, 0)?,
            vec![track]
        );
        Ok(())
    }

    #[test]
    fn deleting_an_artist_removes_its_credits() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "release")?;
        let kept = insert_artist(&mut db, "Kept")?;
        let removed = insert_artist(&mut db, "Removed")?;
        connect_credit(&mut db, release, removed, CreditType::Artist, None, 0)?;
        connect_credit(&mut db, release, kept, CreditType::Artist, None, 1)?;

        db.transaction_mut(|tx| crate::metadata::cascade_remove_entities_in_txn(tx, &[removed]))?;

        let remaining = links_for_owner(&db, release)?;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].artist_id, kept);
        Ok(())
    }

    fn credited(db: &DbAny, artist: DbId) -> anyhow::Result<bool> {
        Ok(crate::artists::get_by_id(db, artist)?.is_some_and(|artist| artist.credited))
    }

    fn artist_credit(artist_id: DbId) -> CreditLinkInput {
        CreditLinkInput {
            artist_id,
            credit_type: CreditType::Artist,
            detail: None,
        }
    }

    #[test]
    fn replacing_credits_moves_the_credited_flag() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track = insert_track(&mut db, "track")?;
        let first = insert_artist(&mut db, "First")?;
        let second = insert_artist(&mut db, "Second")?;
        assert!(!credited(&db, first)?, "a new artist starts uncredited");

        replace_for_owner(&mut db, track, &[artist_credit(first)])?;
        assert!(credited(&db, first)?);

        replace_for_owner(&mut db, track, &[artist_credit(second)])?;
        assert!(!credited(&db, first)?);
        assert!(credited(&db, second)?);

        replace_for_owner(&mut db, track, &[])?;
        assert!(!credited(&db, second)?);
        Ok(())
    }

    #[test]
    fn deleting_the_only_crediting_track_uncredits_the_artist() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release = insert_release(&mut db, "release")?;
        let track = insert_track(&mut db, "track")?;
        crate::fixtures::link_track(&mut db, release, track)?;
        let other = insert_track(&mut db, "other")?;
        crate::fixtures::link_track(&mut db, release, other)?;
        let track_only = insert_artist(&mut db, "Track Only")?;
        let both = insert_artist(&mut db, "Both")?;
        connect_artist(&mut db, track, track_only)?;
        connect_artist(&mut db, track, both)?;
        connect_artist(&mut db, release, both)?;

        crate::metadata::cascade_remove_entities(&mut db, &[track])?;

        assert!(!credited(&db, track_only)?);
        assert!(credited(&db, both)?, "the release still credits it");
        crate::metadata::cascade_remove_entities(&mut db, &[release])?;
        assert!(!credited(&db, both)?);
        Ok(())
    }
}
