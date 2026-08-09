// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::BTreeMap;

use agdb::{
    DbElement,
    DbId,
    QueryBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;

use crate::db::{
    DbAccess,
    NodeId,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManualMetadataField {
    ReleaseTitle,
    TrackTitle,
    ArtistName,
    SortTitle,
    SortName,
    ReleaseType,
    ReleaseDate,
    Genres,
    Labels,
    Credits,
    Year,
    Disc,
    DiscTotal,
    Track,
    TrackTotal,
    ArtistType,
    Description,
    Relations,
}

impl ManualMetadataField {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReleaseTitle => "release_title",
            Self::TrackTitle => "track_title",
            Self::ArtistName => "artist_name",
            Self::SortTitle => "sort_title",
            Self::SortName => "sort_name",
            Self::ReleaseType => "release_type",
            Self::ReleaseDate => "release_date",
            Self::Genres => "genres",
            Self::Labels => "labels",
            Self::Credits => "credits",
            Self::Year => "year",
            Self::Disc => "disc",
            Self::DiscTotal => "disc_total",
            Self::Track => "track",
            Self::TrackTotal => "track_total",
            Self::ArtistType => "artist_type",
            Self::Description => "description",
            Self::Relations => "relations",
        }
    }

    pub(crate) const fn is_graph(self) -> bool {
        matches!(
            self,
            Self::Genres | Self::Labels | Self::Credits | Self::Relations
        )
    }
}

#[derive(DbElement, Clone, Debug)]
pub(crate) struct ManualMetadataOverride {
    pub(crate) db_id: Option<NodeId>,
    pub(crate) fields: String,
}

impl ManualMetadataOverride {
    pub(crate) fn parsed_fields(&self) -> anyhow::Result<BTreeMap<ManualMetadataField, Value>> {
        let fields: BTreeMap<ManualMetadataField, Value> = serde_json::from_str(&self.fields)?;
        validate_fields(&fields)?;
        Ok(fields)
    }
}

fn validate_fields(fields: &BTreeMap<ManualMetadataField, Value>) -> anyhow::Result<()> {
    if fields.is_empty() {
        anyhow::bail!("manual metadata override cannot own zero fields");
    }
    for (field, value) in fields {
        if field.is_graph() && value != &Value::Bool(true) {
            anyhow::bail!(
                "manual graph field '{}' must be stored as an ownership marker",
                field.as_str(),
            );
        }
    }
    Ok(())
}

pub(crate) fn get(
    db: &impl DbAccess,
    entity_id: DbId,
) -> anyhow::Result<Option<ManualMetadataOverride>> {
    let rows: Vec<ManualMetadataOverride> = db
        .exec(
            QueryBuilder::select()
                .elements::<ManualMetadataOverride>()
                .search()
                .from(entity_id)
                .where_()
                .neighbor()
                .end_where()
                .query(),
        )?
        .try_into()?;

    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.into_iter().next()),
        count => anyhow::bail!(
            "entity {} has {count} manual metadata override rows; expected exactly one",
            entity_id.0,
        ),
    }
}

pub(crate) fn field_names(
    db: &impl DbAccess,
    entity_id: DbId,
) -> anyhow::Result<Vec<ManualMetadataField>> {
    let Some(row) = get(db, entity_id)? else {
        return Ok(Vec::new());
    };
    Ok(row.parsed_fields()?.into_keys().collect())
}

pub(crate) fn owns_field(
    db: &impl DbAccess,
    entity_id: DbId,
    field: ManualMetadataField,
) -> anyhow::Result<bool> {
    let Some(row) = get(db, entity_id)? else {
        return Ok(false);
    };
    Ok(row.parsed_fields()?.contains_key(&field))
}

pub(crate) fn upsert(
    db: &mut impl DbAccess,
    entity_id: DbId,
    fields: &BTreeMap<ManualMetadataField, Value>,
) -> anyhow::Result<DbId> {
    validate_fields(fields)?;
    let existing = get(db, entity_id)?;
    let row = ManualMetadataOverride {
        db_id: existing.as_ref().and_then(|row| row.db_id.clone()),
        fields: serde_json::to_string(fields)?,
    };
    let result = db.exec_mut(QueryBuilder::insert().element(&row).query())?;
    let row_id = existing
        .as_ref()
        .and_then(|row| row.db_id.clone())
        .map(DbId::from)
        .or_else(|| result.ids().first().copied())
        .ok_or_else(|| anyhow::anyhow!("manual metadata override upsert returned no id"))?;

    if existing.is_none() {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(entity_id)
                .to(row_id)
                .query(),
        )?;
    }

    Ok(row_id)
}

pub(crate) fn replace(
    db: &mut impl DbAccess,
    entity_id: DbId,
    fields: &BTreeMap<ManualMetadataField, Value>,
) -> anyhow::Result<Option<DbId>> {
    if !fields.is_empty() {
        return upsert(db, entity_id, fields).map(Some);
    }

    let Some(existing) = get(db, entity_id)? else {
        return Ok(None);
    };
    let row_id = existing
        .db_id
        .map(DbId::from)
        .ok_or_else(|| anyhow::anyhow!("manual metadata override row has no database id"))?;
    db.exec_mut(QueryBuilder::remove().ids(row_id).query())?;
    Ok(None)
}

pub(crate) fn merge_into(db: &mut impl DbAccess, winner: DbId, loser: DbId) -> anyhow::Result<()> {
    let Some(loser_row) = get(db, loser)? else {
        return Ok(());
    };
    let loser_fields = loser_row.parsed_fields()?;
    let winner_row = get(db, winner)?;
    let mut winner_fields = winner_row
        .as_ref()
        .map(ManualMetadataOverride::parsed_fields)
        .transpose()?
        .unwrap_or_default();

    for (field, value) in loser_fields {
        winner_fields.entry(field).or_insert(value);
    }

    upsert(db, winner, &winner_fields)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_db::{
        TestDb,
        insert_track,
        new_test_db,
    };

    #[test]
    fn upsert_reuses_single_row_and_preserves_explicit_null() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        let entity_id = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0];
        let first =
            BTreeMap::from([(ManualMetadataField::TrackTitle, Value::String("One".into()))]);
        let second = BTreeMap::from([
            (ManualMetadataField::SortTitle, Value::Null),
            (ManualMetadataField::TrackTitle, Value::String("Two".into())),
        ]);

        let first_id = upsert(&mut db, entity_id, &first)?;
        let second_id = upsert(&mut db, entity_id, &second)?;

        assert_eq!(first_id, second_id);
        let loaded = get(&db, entity_id)?.expect("override exists");
        assert_eq!(loaded.parsed_fields()?, second);
        Ok(())
    }

    #[test]
    fn entity_cascade_removes_manual_override_row() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let track_id = insert_track(&mut db, "Track")?;
        let fields = BTreeMap::from([(
            ManualMetadataField::TrackTitle,
            Value::String("Manual".into()),
        )]);
        let row_id = upsert(&mut db, track_id, &fields)?;

        crate::db::metadata::cascade_remove_entities(&mut db, &[track_id])?;

        assert!(
            db.exec(QueryBuilder::select().ids(row_id).query()).is_err(),
            "manual override row should no longer exist"
        );
        Ok(())
    }

    #[test]
    fn replacing_with_no_owned_fields_removes_the_row() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        let entity_id = db
            .exec_mut(QueryBuilder::insert().nodes().count(1).query())?
            .ids()[0];
        upsert(
            &mut db,
            entity_id,
            &BTreeMap::from([(ManualMetadataField::Credits, Value::Bool(true))]),
        )?;

        replace(&mut db, entity_id, &BTreeMap::new())?;

        assert!(get(&db, entity_id)?.is_none());
        Ok(())
    }
}
