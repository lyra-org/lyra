// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
mod descriptors;
mod layer;
mod parsing;
mod provider;
mod registry;

use harmony_core::{
    ModuleExport,
    ModuleSpec,
};

use crate::plugins::db::{
    self as server_db,
    DbAsync,
};
use crate::services::EntityType;

use self::layer::MetadataLayer;
use self::provider::{
    MetadataProvider,
    ids_for_provider_spec,
    provider_new_spec,
};

#[cfg(feature = "docgen")]
pub(crate) use self::descriptors::render_luau_definition;
pub(crate) use self::registry::{
    MetadataCallback,
    MetadataCallbackRegistry,
};

#[derive(Clone, Default)]
pub(crate) struct MetadataModuleStore {
    db: Option<DbAsync>,
}

impl MetadataModuleStore {
    pub(crate) fn empty() -> Self {
        Self { db: None }
    }

    pub(crate) fn with_db(db: DbAsync) -> Self {
        Self { db: Some(db) }
    }
}

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/metadata")
        .capability("lyra.metadata")
        .function(provider_new_spec())
        .function(ids_for_provider_spec())
        .userdata(EntityType::_harmony_userdata_spec())
        .userdata(server_db::ArtistType::_harmony_userdata_spec())
        .userdata(server_db::CreditType::_harmony_userdata_spec())
        .userdata(server_db::ArtistRelationType::_harmony_userdata_spec())
        .userdata(MetadataProvider::_harmony_userdata_spec())
        .userdata(MetadataLayer::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(MetadataModule)))
}

struct MetadataModule;
