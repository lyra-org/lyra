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
        display::{
            CoverFilter,
            Listens,
        },
        lookup,
        stale,
    },
};

pub(crate) struct CoversModule;

/// The `options` argument.
#[derive(Default)]
struct CoverOptions {
    listens: Option<Listens>,
    require_file: bool,
}

impl<'vm> luau::FromLuau<'vm> for CoverOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        let listens = match options.get("listens")? {
            luau::Value::Nil => None,
            luau::Value::String(value) if value == b"caller" => Some(Listens::Viewer),
            luau::Value::String(value) if value == b"instance" => Some(Listens::Instance),
            other => {
                return Err(crate::plugins::runtime_error(format!(
                    "listens must be \"caller\" or \"instance\", got {}",
                    other.type_name()
                )));
            }
        };
        let require_file =
            crate::plugins::args::optional_bool(&options, "require_file", "options")?
                .unwrap_or(false);
        Ok(Self {
            listens,
            require_file,
        })
    }
}

impl LuauTypeInfo for CoverOptions {
    fn luau_type() -> LuauType {
        LuauType::named("CoverOptions")
    }
}

#[harmony_macros::module(
    id = "lyra/covers",
    description = "Item covers, read from disk by plugins that serve the image themselves. Releases and artists have their own cover; a track its own, else that of the first of its releases the caller can see, by release id; a playlist its uploaded cover, else, like a genre, the cover of one release in it: a playlist's is picked deterministically from its tracks' releases, a genre's from listens when they are enough, else deterministically. An owner's own cover is its pinned source's, else the first of manual, provider and local. Covers are read from their stored records without looking at the files unless `require_file` is set.",
    docs_feature = "docgen"
)]
impl CoversModule {
    #[harmony(
        description = "The covers of the items the caller can see, keyed by id. Items without a cover, unknown ids and items of other types are absent. A track, playlist or genre shows a release the caller can see. Without a resolved caller, a client dispatch is refused.",
        returns(HashMap<String, CoverInfo>)
    )]
    async fn get(
        ids: UniqueIds,
        options: Option<CoverOptions>,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let options = options.unwrap_or_default();
        let listens = options.listens.unwrap_or(Listens::Viewer);
        covers(&db, &ids, listens, options.require_file).await
    }

    #[harmony(
        description = "`get` read as the system, with genre covers picked from the instance's listens whatever `options.listens` says. Ignores library and playlist visibility, including private playlists, so it is only for public artwork routes, which must serve nothing from it but the image bytes.",
        returns(HashMap<String, CoverInfo>)
    )]
    async fn artwork(
        ids: UniqueIds,
        options: Option<CoverOptions>,
        #[context] db: SystemDb,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let require_file = options.is_some_and(|options| options.require_file);
        covers(&db, &ids, Listens::Instance, require_file).await
    }
}

async fn covers(
    db: &ViewerDb,
    ids: &UniqueIds,
    listens: Listens,
    require_file: bool,
) -> luau::runtime::Result<luau::OwnedTable> {
    let (table, stale_covers) = {
        let (db, viewer) = db.read().await?;
        let ids = Resolved::of(&*db, ids.0.clone())?;
        let filter = CoverFilter::new(&viewer).listening(listens);
        let mut found = lookup::resolve(&db, &filter, ids.db_ids())?;
        let stale_covers = if require_file {
            stale::keep_present(&*db, &mut found)?
        } else {
            Vec::new()
        };
        let owner_ids = found
            .values()
            .map(|cover| cover.owner_db_id)
            .collect::<Vec<_>>();
        let owners = crate::plugins::ids::public_ids(&*db, &owner_ids)?;
        let mut covers = Vec::with_capacity(found.len());
        for (id, cover) in found {
            covers.push((
                id,
                harmony_luau::serializable_to_luau_owned(CoverInfo::new(cover, &owners)?)?,
            ));
        }
        (ids.keyed(covers), stale_covers)
    };
    if !stale_covers.is_empty() {
        let handle = db.handle();
        let mut db = handle.write().await;
        stale::remove_stale(&mut db, &stale_covers)?;
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
    options.fields.push(FieldDescriptor {
        name: "require_file",
        ty: LuauType::optional(bool::luau_type()),
        description: Some(
            "Leave out covers whose image file is missing, and remove the record of one whose file is confirmed gone, so later reads show the owner's next cover. Default `false`. For serving the image; listings leave it off, so they never touch the disk.",
        ),
    });
    harmony_luau::render_definition_file_with_support(
        &CoversModule::module_descriptor(),
        &[],
        &[options, CoverInfo::interface_descriptor()],
        &[],
    )
}
