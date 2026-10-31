// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::{
    plugins::{
        args::UniqueIds,
        db::{
            SystemDb,
            ViewerDb,
        },
        ids::Resolved,
        types::CoverInfo,
    },
    services::covers::{
        display::Listens,
        lookup,
    },
};

pub(crate) struct CoversModule;

/// The `options` argument.
struct CoverOptions {
    listens: Listens,
}

impl<'vm> luau::FromLuau<'vm> for CoverOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        let listens = match options.get("listens")? {
            luau::Value::Nil => Listens::Viewer,
            luau::Value::String(value) if value == b"caller" => Listens::Viewer,
            luau::Value::String(value) if value == b"instance" => Listens::Instance,
            other => {
                return Err(crate::plugins::runtime_error(format!(
                    "listens must be \"caller\" or \"instance\", got {}",
                    other.type_name()
                )));
            }
        };
        Ok(Self { listens })
    }
}

impl LuauTypeInfo for CoverOptions {
    fn luau_type() -> LuauType {
        LuauType::named("CoverOptions")
    }
}

#[harmony_macros::module(
    id = "lyra/covers",
    description = "Item covers, read from disk by plugins that serve the image themselves. Releases and artists have their own cover, a track shows its release's, and a playlist or genre shows the cover of one release in it: a playlist's is picked deterministically from its tracks' releases, a genre's from listens when they are enough, else deterministically. A cover whose file is gone is left out and forgotten.",
    docs_feature = "docgen"
)]
impl CoversModule {
    #[harmony(
        description = "The covers of the items the caller can see, keyed by id. Items without a cover, unknown ids and items of other types are absent. A playlist or genre shows a release the caller can see. Without a resolved caller, a client dispatch is refused.",
        returns(HashMap<String, CoverInfo>)
    )]
    async fn get(
        ids: UniqueIds,
        options: Option<CoverOptions>,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let listens = options.map_or(Listens::Viewer, |options| options.listens);
        covers(&db, &ids, listens).await
    }

    #[harmony(
        description = "`get` read as the system, with genre covers picked from the instance's listens whatever `options` says. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
        returns(HashMap<String, CoverInfo>)
    )]
    async fn artwork(
        ids: UniqueIds,
        _options: Option<CoverOptions>,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        covers(&db, &ids, Listens::Instance).await
    }
}

async fn covers(
    db: &ViewerDb,
    ids: &UniqueIds,
    listens: Listens,
) -> luau::runtime::Result<luau::OwnedTable> {
    let (table, stale_owners) = {
        let (db, viewer) = db.read().await?;
        let ids = Resolved::of(&*db, ids.0.clone())?;
        let found = lookup::resolve(&db, &viewer, ids.db_ids(), listens)?;
        let owner_ids = found
            .covers
            .values()
            .map(|cover| cover.owner_db_id)
            .collect::<Vec<_>>();
        let owners = crate::plugins::ids::public_ids(&*db, &owner_ids)?;
        let mut covers = Vec::with_capacity(found.covers.len());
        for (id, cover) in found.covers {
            covers.push((
                id,
                harmony_luau::serializable_to_luau_owned(CoverInfo::new(cover, &owners)?)?,
            ));
        }
        (ids.keyed(covers), found.stale_owners)
    };
    if !stale_owners.is_empty() {
        let handle = db.handle();
        let mut db = handle.write().await;
        lookup::remove_stale(&mut db, &stale_owners)?;
    }
    Ok(table)
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    let mut options = InterfaceDescriptor::new("CoverOptions", None);
    options.fields.push(FieldDescriptor {
        name: "listens",
        ty: LuauType::optional(LuauType::union(vec![
            LuauType::string_literal("caller"),
            LuauType::string_literal("instance"),
        ])),
        description: Some(
            "Whose listens pick a genre's cover: the caller's first, then the instance's (`caller`, the default), or only the instance's (`instance`), so every caller sees the same cover.",
        ),
    });
    harmony_luau::render_definition_file_with_support(
        &CoversModule::module_descriptor(),
        &[],
        &[options, CoverInfo::interface_descriptor()],
        &[],
    )
}
