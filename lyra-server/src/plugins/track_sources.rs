// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use agdb::DbId;
use harmony_luau as luau;

use crate::plugins::db::ViewerDb;

pub(crate) struct TrackSourcesModule;

#[harmony_macros::module(id = "lyra/track_sources", docs_feature = "docgen")]
impl TrackSourcesModule {
    async fn get_primary_source_key(
        track_id: i64,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<Option<String>> {
        if track_id <= 0 {
            return Ok(None);
        }
        let (db, viewer) = db.read().await?;
        if !viewer.sees_entity(&*db, DbId(track_id))? {
            return Ok(None);
        }
        let source = lyra_db::track_sources::get_primary_by_track(&db, DbId(track_id))?;
        Ok(source
            .map(|source| source.source_key)
            .filter(|value| !value.trim().is_empty()))
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &TrackSourcesModule::module_descriptor(),
        &[],
        &[],
        &[],
    )
}
