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
    DescribeTypeAlias,
    FieldDescriptor,
    InterfaceDescriptor,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};

use crate::{
    plugins::{
        args::{
            self,
            UniqueIds,
        },
        db::{
            SystemDb,
            ViewerDb,
        },
        ids::{
            self,
            Resolved,
        },
        types::CoverInfo,
    },
    services::{
        auth::Principal,
        covers::{
            self as cover_service,
            display::{
                CoverFilter,
                Listens,
            },
            lookup,
            selection::{
                self,
                CoverOwnerKind,
                CoverSelectionError,
            },
            stale,
        },
    },
};

pub(crate) struct CoversModule;

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_IMAGE_BYTES",
    value: cover_service::MAX_COVER_BYTES,
    description: "The most bytes an image `set_manual` takes may have.",
}]);

/// The `image` argument: raw image bytes, as a string or a buffer.
struct ImageBytes(Vec<u8>);

impl<'vm> luau::FromLuau<'vm> for ImageBytes {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        match reader.read::<luau::Value>()? {
            luau::Value::String(bytes) | luau::Value::Buffer(bytes) => Ok(Self(bytes)),
            other => Err(crate::plugins::runtime_error(format!(
                "image must be a string or buffer, got {}",
                other.type_name()
            ))),
        }
    }
}

impl LuauTypeInfo for ImageBytes {
    fn luau_type() -> LuauType {
        LuauType::union(vec![String::luau_type(), luau::Buffer::luau_type()])
    }
}

/// The `options` argument.
#[derive(Default)]
struct CoverOptions {
    listens: Option<Listens>,
    require_file: bool,
}

#[harmony_macros::string_enum(
    name = "CoverListens",
    description = "Whose listens pick a genre's cover: the caller's first, then the instance's (`caller`), or only the instance's (`instance`), so every caller sees the same cover."
)]
#[derive(Clone, Copy)]
enum CoverListens {
    Caller,
    Instance,
}

impl From<CoverListens> for Listens {
    fn from(listens: CoverListens) -> Self {
        match listens {
            CoverListens::Caller => Self::Viewer,
            CoverListens::Instance => Self::Instance,
        }
    }
}

impl<'vm> luau::FromLuau<'vm> for CoverOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["listens", "require_file"])?;
        let listens = args::optional_string_enum::<CoverListens>(&options, "listens", "options")?
            .map(Listens::from);
        let require_file =
            args::optional_bool(&options, "require_file", "options")?.unwrap_or(false);
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
    description = "Item covers, read from disk by plugins that serve the image themselves, and the manual covers users upload. Releases and artists have their own cover; a track its own, else that of the first of its releases the caller can see, by release id; a playlist its uploaded cover, else, like a genre, the cover of one release in it: a playlist's is picked deterministically from its tracks' releases, a genre's from listens when they are enough, else deterministically. An owner's own cover is its pinned source's, else the first of manual, provider and local. Covers are read from their stored records without looking at the files unless `require_file` is set.",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
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

    #[harmony(
        description = "Stores `image`, the raw bytes of a JPEG, PNG or WebP image, as the manual cover of a release, artist or playlist, replacing any earlier one. A release or artist pins it as the cover it shows; a playlist shows it in place of the cover picked from its releases. Returns false when the item is missing, hidden from the caller, or not a release, artist or playlist. Raises when the image is over `MAX_IMAGE_BYTES` or does not decode, when the caller lacks the manage_metadata permission for a release or artist, and when the caller does not own the playlist."
    )]
    async fn set_manual(
        id: String,
        image: ImageBytes,
        #[context] db: ViewerDb,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let Some(kind) = owner_kind(&db, &id).await? else {
            return Ok(false);
        };
        applied(selection::set_manual(&db.handle(), &principal, kind, &id, image.0).await)
    }

    #[harmony(
        description = "Removes the manual cover of a release, artist or playlist, which then shows its next cover, and unpins a release or artist pinned to it. Returns false when the item is missing, hidden from the caller, not a release, artist or playlist, or has no manual cover. Raises when the caller lacks the manage_metadata permission for a release or artist, and when the caller does not own the playlist."
    )]
    async fn remove_manual(
        id: String,
        #[context] db: ViewerDb,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let Some(kind) = owner_kind(&db, &id).await? else {
            return Ok(false);
        };
        applied(selection::remove_manual(&db.handle(), &principal, kind, &id).await)
    }
}

/// The kind of the item `id` names, if the caller can see it and users choose its cover.
async fn owner_kind(db: &ViewerDb, id: &str) -> luau::runtime::Result<Option<CoverOwnerKind>> {
    let (db, viewer) = db.read().await?;
    let Some(db_id) = ids::visible_db_id(&db, &viewer, id)? else {
        return Ok(None);
    };
    Ok(match ids::kind(&db, db_id)?.as_deref() {
        Some("Release") => Some(CoverOwnerKind::Release),
        Some("Artist") => Some(CoverOwnerKind::Artist),
        Some("Playlist") => Some(CoverOwnerKind::Playlist),
        _ => None,
    })
}

/// A missing or hidden owner, or a missing manual cover, as false; other failures raise.
fn applied<T>(result: Result<T, CoverSelectionError>) -> luau::runtime::Result<bool> {
    match result {
        Ok(_) => Ok(true),
        Err(CoverSelectionError::NotFound(_)) => Ok(false),
        Err(error) => Err(crate::plugins::runtime_error(error)),
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
        ty: Option::<CoverListens>::luau_type(),
        description: Some("Defaults to `caller`."),
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
        &[CoverListens::type_alias_descriptor()],
        &[options, CoverInfo::interface_descriptor()],
        &[],
    )
}
