// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
use harmony_luau::FromCallFrame;

use crate::plugins::{
    db::ViewerDb,
    ids,
};

pub(crate) struct TrackSourcesModule;

#[harmony_macros::module(id = "lyra/track_sources", docs_feature = "docgen")]
impl TrackSourcesModule {
    async fn get_primary_source_key(
        track_id: String,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<Option<String>> {
        let db = ViewerDb::from_call_frame(&vm, &context)?;
        let (db, viewer) = db.read().await?;
        let Some(track_id) = ids::visible_db_id_of(&db, &viewer, &track_id, "Track")? else {
            return Ok(None);
        };
        let source = lyra_db::track_sources::get_primary_by_track(&db, track_id)?;
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
