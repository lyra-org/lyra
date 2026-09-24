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

/// Values on an `owner → artist` credit edge. An owner may credit one artist
/// several times, one edge per credit.
#[derive(DbElement, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Credit {
    pub(crate) credit_type: CreditType,
    pub(crate) detail: Option<String>,
    pub(crate) artist_order: u64,
}

impl Credit {
    /// What makes two credits of one owner for one artist the same credit.
    pub(crate) fn role(&self) -> (CreditType, Option<&str>) {
        (self.credit_type, self.detail.as_deref())
    }

    fn is_primary(&self) -> bool {
        self.role() == (CreditType::Artist, None)
    }
}

/// A stored credit edge with its endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CreditLink {
    pub(crate) edge_id: DbId,
    pub(crate) owner_id: DbId,
    pub(crate) artist_id: DbId,
    pub(crate) credit: Credit,
}

/// The credits a set of links holds, for asking whether another link would repeat one.
pub(crate) struct CreditSet(std::collections::HashSet<(DbId, DbId, CreditType, Option<String>)>);

impl CreditSet {
    pub(crate) fn new(links: &[CreditLink]) -> Self {
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
    pub(crate) fn contains(
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

/// Inserts a credit edge. Every credit insert goes through here.
pub(crate) fn link(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    artist_id: DbId,
    credit: &Credit,
) -> anyhow::Result<DbId> {
    db.exec_mut(
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
    .ok_or_else(|| anyhow::anyhow!("credit insert returned no id"))
}

pub(crate) fn links_for_owner(
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

pub(crate) fn links_to_artist(
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
pub(crate) fn crediting_owner_ids(
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

/// The order that places a new credit after every existing one.
pub(crate) fn next_order(links: &[CreditLink]) -> u64 {
    links
        .iter()
        .map(|link| link.credit.artist_order + 1)
        .max()
        .unwrap_or(0)
}

fn set_order(
    db: &mut impl super::DbAccess,
    edge_id: DbId,
    artist_order: u64,
) -> anyhow::Result<()> {
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform([("artist_order", artist_order).into()])
            .ids(edge_id)
            .query(),
    )?;
    Ok(())
}

/// Walks `Artist ← credit edge ← Owner` depth-first, so a bounded query stops
/// after `offset + limit` owners.
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

pub(crate) fn replace_for_owner(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    desired: &[CreditLinkInput],
) -> anyhow::Result<()> {
    let existing: Vec<DbId> = links_for_owner(db, owner_id)?
        .into_iter()
        .map(|link| link.edge_id)
        .collect();
    if !existing.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(existing).query())?;
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

    Ok(())
}

fn primary_links(db: &impl super::DbAccess, owner_id: DbId) -> anyhow::Result<Vec<CreditLink>> {
    let mut links: Vec<CreditLink> = links_for_owner(db, owner_id)?
        .into_iter()
        .filter(|link| link.credit.is_primary())
        .collect();
    links.sort_by_key(|link| link.credit.artist_order);
    Ok(links)
}

/// Artists holding the owner's primary credits (`Artist`, no detail), in credit order.
pub(crate) fn primary_artist_ids(
    db: &impl super::DbAccess,
    owner_id: DbId,
) -> anyhow::Result<Vec<DbId>> {
    let mut seen = std::collections::HashSet::new();
    Ok(primary_links(db, owner_id)?
        .into_iter()
        .map(|link| link.artist_id)
        .filter(|artist_id| seen.insert(*artist_id))
        .collect())
}

/// Aligns the owner's primary credits with `artist_ids`, ordered as given.
/// Credits in other roles or with a detail are left alone.
/// Tags only yield primary credits; the rest belong to providers and manual edits.
pub(crate) fn replace_primary_for_owner(
    db: &mut impl super::DbAccess,
    owner_id: DbId,
    artist_ids: &[DbId],
) -> anyhow::Result<()> {
    let mut desired = Vec::new();
    let mut desired_set = std::collections::HashSet::new();
    for artist_id in artist_ids {
        if desired_set.insert(*artist_id) {
            desired.push(*artist_id);
        }
    }

    let mut kept = std::collections::HashMap::new();
    let mut remove = Vec::new();
    for link in primary_links(db, owner_id)? {
        if desired_set.contains(&link.artist_id) && !kept.contains_key(&link.artist_id) {
            kept.insert(link.artist_id, link.edge_id);
        } else {
            remove.push(link.edge_id);
        }
    }
    if !remove.is_empty() {
        db.exec_mut(QueryBuilder::remove().ids(remove).query())?;
    }

    for (order, artist_id) in desired.into_iter().enumerate() {
        let artist_order = order as u64;
        if let Some(edge_id) = kept.get(&artist_id) {
            set_order(db, *edge_id, artist_order)?;
        } else {
            link(
                db,
                owner_id,
                artist_id,
                &Credit {
                    credit_type: CreditType::Artist,
                    detail: None,
                    artist_order,
                },
            )?;
        }
    }
    Ok(())
}

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
    let existing = links_for_owner(db, owner_id)?;
    let mut artist_order = next_order(&existing);
    let names = desired
        .iter()
        .map(|input| lyra_metadata::ArtistCreditName {
            name: input.name.clone(),
            join_phrase: input.join_phrase.clone(),
        })
        .collect::<Vec<_>>();
    let mut linked = std::collections::HashSet::new();
    let mut remove = Vec::new();
    for CreditLink {
        edge_id,
        artist_id,
        credit,
        ..
    } in existing
    {
        if !credit.is_primary() {
            continue;
        }
        linked.insert(artist_id);
        // Only a split credit can replace a combined artist, and only one the scanner created:
        // unlocked, unnamed by the user, without identities, and named exactly like the credit.
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
            remove.push(edge_id);
        }
    }
    for input in desired {
        if !linked.insert(input.artist_id) {
            continue;
        }
        link(
            db,
            owner_id,
            input.artist_id,
            &Credit {
                credit_type: CreditType::Artist,
                detail: None,
                artist_order,
            },
        )?;
        artist_order += 1;
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

    /// Inserts an artist with the `test` identity that `reconcile_artists` requires.
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
        connect(&mut db, release, track)?;
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
            owner_ids_by_artist::<crate::db::Track>(&db, artist, 0, 0)?,
            vec![track]
        );
        Ok(())
    }

    #[test]
    fn primary_credit_sync_leaves_other_credits_alone() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track = insert_track(&mut db, "track")?;
        let primary = insert_artist(&mut db, "Primary")?;
        let composer = insert_artist(&mut db, "Composer")?;
        let guest = insert_artist(&mut db, "Guest")?;
        connect_credit(&mut db, track, primary, CreditType::Artist, None, 0)?;
        connect_credit(&mut db, track, composer, CreditType::Composer, None, 1)?;
        connect_credit(&mut db, track, guest, CreditType::Artist, None, 2)?;
        connect_credit(&mut db, track, guest, CreditType::Artist, Some("feat."), 3)?;

        // A rescan re-applies the primary artists it read back.
        let primary_ids = primary_artist_ids(&db, track)?;
        assert_eq!(primary_ids, [primary, guest]);
        replace_primary_for_owner(&mut db, track, &primary_ids)?;
        let roles = |db: &DbAny| -> anyhow::Result<Vec<(DbId, CreditType, Option<String>)>> {
            let mut links = links(db, track)?
                .into_iter()
                .map(|link| (link.artist_id, link.credit.credit_type, link.credit.detail))
                .collect::<Vec<_>>();
            links.sort_by_key(|(artist_id, credit_type, detail)| {
                (artist_id.0, credit_type.to_string(), detail.clone())
            });
            Ok(links)
        };
        let mut expected = vec![
            (primary, CreditType::Artist, None),
            (composer, CreditType::Composer, None),
            (guest, CreditType::Artist, None),
            (guest, CreditType::Artist, Some("feat.".to_string())),
        ];
        expected.sort_by_key(|(artist_id, credit_type, detail)| {
            (artist_id.0, credit_type.to_string(), detail.clone())
        });
        assert_eq!(roles(&db)?, expected);

        // Dropping the guest removes only its primary credit.
        replace_primary_for_owner(&mut db, track, &[primary])?;
        expected.retain(|(artist_id, _, detail)| *artist_id != guest || detail.is_some());
        assert_eq!(roles(&db)?, expected);
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

        db.transaction_mut(|tx| db::metadata::cascade_remove_entities_in_txn(tx, &[removed]))?;

        let remaining = links_for_owner(&db, release)?;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].artist_id, kept);
        Ok(())
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
        assert!(!after.iter().any(|link| link.artist_id == combined
            && link.credit.credit_type == CreditType::Artist
            && link.credit.detail.is_none()));
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
