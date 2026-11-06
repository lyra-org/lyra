// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::time::Duration;

use harmony_luau as luau;
use harmony_luau::FromCallFrame;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};

use crate::{
    plugins::{
        db::ViewerDb,
        ids,
    },
    services::libraries::MediaRoots,
};

const DECODE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct ChromaprintModule;

#[cfg(feature = "docgen")]
struct ChromaprintResult;

#[harmony_macros::module(id = "lyra/chromaprint", docs_feature = "docgen")]
impl ChromaprintModule {
    #[harmony(returns(ChromaprintResult))]
    async fn compute(
        entry_id: String,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = ViewerDb::from_call_frame(&vm, &context)?;
        let (path, roots) = {
            let (db, viewer) = db.read().await?;
            let not_found = || crate::plugins::runtime_error("entry not found");
            let entry_id =
                ids::visible_db_id_of(&db, &viewer, &entry_id, "Entry")?.ok_or_else(not_found)?;
            let entry = lyra_db::entries::get_by_id(&db, entry_id)?.ok_or_else(not_found)?;
            (entry.full_path, MediaRoots::libraries(&*db, &viewer)?)
        };
        let path = roots
            .resolve_off_thread(path)
            .await
            .map_err(crate::plugins::runtime_error)?;
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
