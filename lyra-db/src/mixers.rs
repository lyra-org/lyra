// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
};
use serde::Serialize;

use super::NodeId;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(DbElement, Serialize, Clone, Debug)]
pub struct MixerConfig {
    #[serde(skip)]
    pub db_id: Option<NodeId>,
    pub mixer_id: String,
    pub display_name: String,
    /// Zero-based rank; lower positions take precedence.
    pub position: u32,
    pub enabled: bool,
}

pub fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<MixerConfig>> {
    let mut mixers: Vec<MixerConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<MixerConfig>()
                .search()
                .from("mixers")
                .where_()
                .neighbor()
                .query(),
        )?
        .try_into()?;
    mixers.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.mixer_id.cmp(&b.mixer_id))
    });

    Ok(mixers)
}

pub fn get_by_mixer_id(db: &DbAny, mixer_id: &str) -> anyhow::Result<Option<MixerConfig>> {
    let mixers: Vec<MixerConfig> = db
        .exec(
            QueryBuilder::select()
                .elements::<MixerConfig>()
                .search()
                .from("mixers")
                .where_()
                .key("mixer_id")
                .value(mixer_id)
                .query(),
        )?
        .try_into()?;

    Ok(mixers.into_iter().next())
}

pub fn upsert(db: &mut DbAny, mixer: &MixerConfig) -> anyhow::Result<DbId> {
    let existing = get_by_mixer_id(db, &mixer.mixer_id)?;
    let mut to_save = mixer.clone();
    if let Some(ref e) = existing {
        to_save.db_id = e.db_id.clone();
    }

    let result = db.exec_mut(QueryBuilder::insert().element(&to_save).query())?;
    let id = existing
        .as_ref()
        .and_then(|e| e.db_id.clone())
        .map(DbId::from)
        .or_else(|| result.elements.first().map(|e| e.id))
        .ok_or_else(|| anyhow::anyhow!("upsert mixer returned no id"))?;

    if existing.is_none() {
        db.exec_mut(QueryBuilder::insert().edges().from("mixers").to(id).query())?;
    }

    Ok(id)
}

/// Registers a mixer at the end of the order unless it is already stored, and names it
/// `display_name`, or its id without one. A stored mixer keeps its place and is renamed.
pub fn register(db: &mut DbAny, mixer_id: &str, display_name: Option<&str>) -> anyhow::Result<()> {
    let display_name = display_name.unwrap_or(mixer_id);
    if let Some(mut existing) = get_by_mixer_id(db, mixer_id)? {
        if existing.display_name != display_name {
            existing.display_name = display_name.to_string();
            db.exec_mut(QueryBuilder::insert().element(&existing).query())?;
        }
        return Ok(());
    }
    let position = u32::try_from(get(db)?.len())?;
    upsert(
        db,
        &MixerConfig {
            db_id: None,
            mixer_id: mixer_id.to_string(),
            display_name: display_name.to_string(),
            position,
            enabled: true,
        },
    )?;
    Ok(())
}

/// Moves a mixer to `position`, clamped to the end, and renumbers the rest
/// without gaps. Returns `false` when the mixer does not exist.
pub fn move_to(
    db: &mut impl super::DbAccess,
    mixer_id: &str,
    position: u32,
) -> anyhow::Result<bool> {
    let mut mixers = get(db)?;
    let Some(current) = mixers.iter().position(|mixer| mixer.mixer_id == mixer_id) else {
        return Ok(false);
    };
    let mixer = mixers.remove(current);
    let insert_at = usize::try_from(position)
        .unwrap_or(usize::MAX)
        .min(mixers.len());
    mixers.insert(insert_at, mixer);

    for (index, mut mixer) in mixers.into_iter().enumerate() {
        let index = u32::try_from(index)?;
        if mixer.position != index {
            mixer.position = index;
            db.exec_mut(QueryBuilder::insert().element(&mixer).query())?;
        }
    }
    Ok(true)
}

/// Returns `false` when the mixer does not exist.
pub fn set_enabled(
    db: &mut impl super::DbAccess,
    mixer_id: &str,
    enabled: bool,
) -> anyhow::Result<bool> {
    let Some(mut mixer) = get(db)?
        .into_iter()
        .find(|mixer| mixer.mixer_id == mixer_id)
    else {
        return Ok(false);
    };
    if mixer.enabled != enabled {
        mixer.enabled = enabled;
        db.exec_mut(QueryBuilder::insert().element(&mixer).query())?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_db::TestDb;

    fn order(db: &DbAny) -> anyhow::Result<Vec<(String, u32)>> {
        Ok(get(db)?
            .into_iter()
            .map(|mixer| (mixer.mixer_id, mixer.position))
            .collect())
    }

    #[test]
    fn mixers_register_at_the_end_and_move_without_gaps() -> anyhow::Result<()> {
        let mut db = TestDb::with_root_aliases(&["mixers"])?.into_inner();
        for mixer_id in ["a", "b", "c"] {
            register(&mut db, mixer_id, None)?;
        }

        assert!(move_to(&mut db, "c", 0)?);
        assert_eq!(
            order(&db)?,
            [("c".into(), 0), ("a".into(), 1), ("b".into(), 2)]
        );
        assert!(set_enabled(&mut db, "a", false)?);
        assert!(!move_to(&mut db, "missing", 0)?);
        assert!(!set_enabled(&mut db, "missing", true)?);
        Ok(())
    }

    #[test]
    fn registering_again_renames_a_mixer_in_place() -> anyhow::Result<()> {
        let mut db = TestDb::with_root_aliases(&["mixers"])?.into_inner();
        register(&mut db, "a", None)?;
        register(&mut db, "b", Some("Bee"))?;
        assert!(move_to(&mut db, "b", 0)?);
        register(&mut db, "b", Some("Bees"))?;
        register(&mut db, "a", Some("Ay"))?;

        let names = get(&db)?
            .into_iter()
            .map(|mixer| (mixer.mixer_id, mixer.display_name, mixer.position))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [("b".into(), "Bees".into(), 0), ("a".into(), "Ay".into(), 1)]
        );
        Ok(())
    }
}
