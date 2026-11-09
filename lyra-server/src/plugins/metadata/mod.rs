// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(test)]
mod cover_pass_tests;
#[cfg(feature = "docgen")]
mod descriptors;
mod layer;
mod parsing;
mod provider;
mod registry;

use std::collections::HashMap;

use crate::services::providers::EntityRefreshOptions;

use harmony_luau as luau;
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use self::layer::MetadataLayer;
#[cfg(feature = "docgen")]
use self::provider::ProviderOptions;
use self::provider::{
    MetadataProvider,
    provider_new,
};

#[cfg(feature = "docgen")]
pub(crate) use self::descriptors::render_luau_definition;
pub(crate) use self::registry::{
    MetadataCallback,
    MetadataCallbackRegistry,
};

pub(crate) struct MetadataModule;

/// The `options` argument of `refresh`.
struct RefreshOptions(EntityRefreshOptions);

impl<'vm> luau::FromLuau<'vm> for RefreshOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        use crate::plugins::args;

        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(
            &options,
            "options",
            &["covers", "bypass_cover_cache", "provider_options"],
        )?;
        let provider_options = match options.get("provider_options")? {
            luau::Value::Nil => HashMap::new(),
            luau::Value::Table(table) => {
                let mut provider_options = HashMap::new();
                for (key, value) in table.pairs()? {
                    let (luau::Value::String(key), luau::Value::String(value)) = (key, value)
                    else {
                        return Err(crate::plugins::runtime_error(
                            "options.provider_options must map strings to strings",
                        ));
                    };
                    provider_options.insert(
                        String::from_utf8_lossy(&key).into_owned(),
                        String::from_utf8_lossy(&value).into_owned(),
                    );
                }
                provider_options
            }
            other => {
                return Err(crate::plugins::runtime_error(format!(
                    "options.provider_options must be a table, got {}",
                    other.type_name()
                )));
            }
        };
        Ok(Self(EntityRefreshOptions {
            covers: args::cover_sync_options(&options, "options")?,
            provider_options,
        }))
    }
}

impl LuauTypeInfo for RefreshOptions {
    fn luau_type() -> LuauType {
        LuauType::named("EntityRefreshOptions")
    }
}

#[cfg(feature = "docgen")]
struct EntityRefreshRecord;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntityRefreshRecord {
    fn luau_type() -> LuauType {
        LuauType::named("EntityRefresh")
    }
}

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
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        provider_new(id, options, vm, context).await
    }

    #[harmony(
        description = "Refreshes the metadata of a release, artist or track from every enabled provider and, for a release or artist, its cover as `options.covers` asks, returning once every provider has answered, which may take a while. Returns the entity's type and the providers called, in the order they ran. Returns nil when the entity is missing, hidden from the caller, or of another type. Raises without a calling user with the sync_metadata permission, as the database has them when the call runs. Needs the `lyra.metadata.refresh` scope too.",
        returns(Option<EntityRefreshRecord>)
    )]
    async fn refresh(
        entity_id: String,
        options: Option<RefreshOptions>,
        #[context] principal: crate::services::auth::Principal,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::Value> {
        use crate::services::providers::{
            ProviderAdminError,
            refresh_entity,
        };

        let options = options.map(|options| options.0).unwrap_or_default();
        let calls = crate::plugins::executor::VmCalls::of(&vm, &context)?;
        match refresh_entity(&calls, &principal, &entity_id, options).await {
            Ok(result) => {
                let mut refreshed = luau::OwnedTable::with_capacity(0, 2);
                refreshed.set_field("entity_type", result.entity_type.as_str());
                refreshed.set_field(
                    "provider_ids",
                    luau::OwnedTable::from_array(result.providers_called),
                );
                Ok(refreshed.into())
            }
            Err(ProviderAdminError::EntityNotFound(_)) => Ok(luau::Value::Nil),
            Err(error) => Err(crate::plugins::runtime_error(error)),
        }
    }
}
