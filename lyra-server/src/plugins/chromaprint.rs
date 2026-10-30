// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use agdb::DbId;
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};

use crate::plugins::db::ViewerDb;

const DECODE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct ChromaprintModule;

#[cfg(feature = "docgen")]
struct ChromaprintResult;

#[harmony_macros::module(id = "lyra/chromaprint", docs_feature = "docgen")]
impl ChromaprintModule {
    #[harmony(returns(ChromaprintResult))]
    async fn compute(
        entry_id: i64,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        if entry_id <= 0 {
            return Err(crate::plugins::runtime_error("entry not found"));
        }
        let path = {
            let (db, viewer) = db.read().await?;
            let entry = lyra_db::entries::get_by_id(&db, DbId(entry_id))?
                .filter(|_| viewer.sees_entity(&*db, DbId(entry_id)).unwrap_or(false))
                .ok_or_else(|| crate::plugins::runtime_error("entry not found"))?;
            entry.full_path
        };
        let fingerprint =
            lyra_chromaprint::compute_fingerprint_from_file(&path, None, Some(DECODE_TIMEOUT))
                .map_err(crate::plugins::runtime_error)?;
        let mut table = luau::OwnedTable::with_capacity(0, 1);
        table.set_field("fingerprint", fingerprint);
        Ok(table)
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for ChromaprintResult {
    fn luau_type() -> LuauType {
        LuauType::named("ChromaprintResult")
    }
}

#[cfg(feature = "docgen")]
impl DescribeInterface for ChromaprintResult {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("ChromaprintResult", None);
        descriptor.fields.push(FieldDescriptor {
            name: "fingerprint",
            ty: String::luau_type(),
            description: None,
        });
        descriptor
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &ChromaprintModule::module_descriptor(),
        &[],
        &[ChromaprintResult::interface_descriptor()],
        &[],
    )
}
