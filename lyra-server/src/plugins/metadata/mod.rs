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

use self::layer::MetadataLayer;
use self::provider::{
    MetadataProvider,
    provider_new_spec,
};

#[cfg(feature = "docgen")]
pub(crate) use self::descriptors::render_luau_definition;
pub(crate) use self::registry::{
    MetadataCallback,
    MetadataCallbackRegistry,
};

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/metadata")
        .function(provider_new_spec())
        .userdata(crate::plugins::types::LuauEntityType::_harmony_userdata_spec())
        .userdata(crate::plugins::types::LuauArtistType::_harmony_userdata_spec())
        .userdata(crate::plugins::types::LuauCreditType::_harmony_userdata_spec())
        .userdata(crate::plugins::types::LuauArtistRelationType::_harmony_userdata_spec())
        .userdata(MetadataProvider::_harmony_userdata_spec())
        .userdata(MetadataLayer::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(MetadataModule)))
}

struct MetadataModule;
