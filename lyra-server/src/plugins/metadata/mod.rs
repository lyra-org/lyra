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

use harmony_luau as luau;

use self::layer::MetadataLayer;
#[cfg(feature = "docgen")]
use self::provider::ProviderOptions;
use self::provider::{
    MetadataProvider,
    ProviderHost,
    provider_new,
};

#[cfg(feature = "docgen")]
pub(crate) use self::descriptors::render_luau_definition;
pub(crate) use self::registry::{
    MetadataCallback,
    MetadataCallbackRegistry,
};

pub(crate) struct MetadataModule;

#[harmony_macros::module(
    id = "lyra/metadata",
    docs_feature = "docgen",
    userdata(
        crate::plugins::types::LuauEntityType,
        crate::plugins::types::LuauArtistType,
        crate::plugins::types::LuauCreditType,
        crate::plugins::types::LuauArtistRelationType,
        MetadataProvider,
        MetadataLayer
    )
)]
impl MetadataModule {
    #[harmony(
        name = "Provider.new",
        description = "Creates a metadata provider registration object.",
        args(id: String, options: Option<ProviderOptions>),
        returns(MetadataProvider)
    )]
    async fn provider_new(
        id: String,
        options: Option<luau::Table>,
        #[context] host: ProviderHost,
    ) -> luau::runtime::Result<luau::Value> {
        provider_new(id, options, host).await
    }
}
