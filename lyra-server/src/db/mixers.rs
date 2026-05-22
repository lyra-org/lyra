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
use schemars::JsonSchema;
use serde::Serialize;

use super::NodeId;

#[derive(DbElement, Serialize, Clone, Debug, JsonSchema)]
pub(crate) struct MixerConfig {
    #[serde(skip)]
    pub(crate) db_id: Option<NodeId>,
    pub(crate) id: String,
    pub(crate) mixer_id: String,
    pub(crate) display_name: String,
    pub(crate) priority: u32,
    pub(crate) enabled: bool,
}

pub(crate) fn get(db: &impl super::DbAccess) -> anyhow::Result<Vec<MixerConfig>> {
    let mixers: Vec<MixerConfig> = db
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

    Ok(mixers)
}

pub(crate) fn get_by_mixer_id(db: &DbAny, mixer_id: &str) -> anyhow::Result<Option<MixerConfig>> {
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

pub(crate) fn upsert(db: &mut DbAny, mixer: &MixerConfig) -> anyhow::Result<DbId> {
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
