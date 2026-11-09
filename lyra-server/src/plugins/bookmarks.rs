// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
use std::collections::HashMap;

use harmony_luau::{
    self as luau,
    LuauType,
    LuauTypeInfo,
};
use serde::Serialize;

use crate::{
    plugins::{
        args::{
            self,
            UniqueIds,
        },
        db::Db,
        ids::{
            self,
            Resolved,
        },
    },
    services::{
        auth::Principal,
        bookmarks::{
            self as bookmark_service,
            BookmarkInput,
            BookmarkUpdatePayload,
        },
    },
};

pub(crate) struct BookmarksModule;

crate::plugins::update_callbacks::update_handler!(
    BookmarkUpdateHandler,
    "BookmarkUpdatePayload",
    "Bookmark update callback."
);

/// [`BookmarkUpdatePayload`] as described to Luau.
#[cfg(feature = "docgen")]
struct LuauBookmarkUpdatePayload;

#[cfg(feature = "docgen")]
impl harmony_luau::DescribeInterface for LuauBookmarkUpdatePayload {
    fn interface_descriptor() -> harmony_luau::InterfaceDescriptor {
        use harmony_luau::{
            FieldDescriptor,
            LuauType,
            LuauTypeInfo,
        };

        harmony_luau::InterfaceDescriptor {
            name: "BookmarkUpdatePayload",
            description: Some("One change to one user's bookmarks."),
            fields: vec![
                FieldDescriptor {
                    name: "event",
                    ty: LuauType::literal("\"set\" | \"removed\""),
                    description: Some(
                        "Whether the user set the track's bookmark, anew or again, or cleared it.",
                    ),
                },
                FieldDescriptor {
                    name: "user_id",
                    ty: String::luau_type(),
                    description: Some("The user's public id."),
                },
                FieldDescriptor {
                    name: "track_id",
                    ty: String::luau_type(),
                    description: Some("The track's public id."),
                },
                FieldDescriptor {
                    name: "position_ms",
                    ty: Option::<u64>::luau_type(),
                    description: Some("The position a `set` left. Absent for `removed`."),
                },
            ],
        }
    }
}

const _: fn(&BookmarkUpdatePayload) = |value| {
    let BookmarkUpdatePayload {
        event: _,
        user_id: _,
        track_id: _,
        position_ms: _,
    } = value;
};

crate::plugins::limits::module_limits!(&[crate::plugins::limits::Limit {
    name: "MAX_COMMENT_CHARS",
    value: lyra_db::bookmarks::MAX_COMMENT_CHARS,
    description: "The most characters a bookmark's `comment` holds.",
}]);

/// The `options` argument of `set`.
#[derive(Default)]
struct SetOptions {
    comment: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for SetOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["comment"])?;
        Ok(Self {
            comment: options.optional_string_field("comment")?,
        })
    }
}

impl LuauTypeInfo for SetOptions {
    fn luau_type() -> LuauType {
        LuauType::named("BookmarkSetOptions")
    }
}

/// A bookmark as plugins see it.
#[derive(Serialize)]
struct BookmarkRecord {
    position_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for BookmarkRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Bookmark")
    }
}

#[harmony_macros::module(
    id = "lyra/bookmarks",
    description = "The dispatch principal's saved positions in tracks, and every user's bookmarks as they change: `on_update` delivers all users' changes, not only the caller's, and needs the `lyra.bookmarks.all` scope too.",
    docs_feature = "docgen",
    extend = install_limits,
    describe = describe_limits
)]
impl BookmarksModule {
    #[harmony(
        description = "Sets the caller's bookmark of the track, replacing its position and comment and keeping when it was first created. A position past the track's known duration is stored as the duration. Returns false when the target is not a track the caller can see. Raises on a position below 0 or past 2^53 - 1, or on a comment past `MAX_COMMENT_CHARS` characters."
    )]
    async fn set(
        track_id: String,
        position_ms: i64,
        options: Option<SetOptions>,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let options = options.unwrap_or_default();
        let input = BookmarkInput::new(position_ms, options.comment)
            .map_err(|err| crate::plugins::runtime_error(err.to_string()))?;
        let mut db = db.write().await;
        let viewer = ids::viewer_of(&*db, &principal)?;
        let Some(track) = ids::visible_db_id(&db, &viewer, &track_id)? else {
            return Ok(false);
        };
        let outcome = bookmark_service::set_for_principal(&mut db, &principal, track, &input)?;
        Ok(outcome == bookmark_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "Clears the caller's bookmark of the track. Returns false when it is not a track the caller can see; a bookmark of one the caller can no longer see is still cleared, and still answers false."
    )]
    async fn remove(
        track_id: String,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<bool> {
        let mut db = db.write().await;
        crate::plugins::auth::require_user_db_id(&principal, &db)?;
        let Some(track) = ids::db_id(&*db, &track_id)? else {
            return Ok(false);
        };
        let outcome = bookmark_service::remove_for_principal(&mut db, &principal, track)?;
        Ok(outcome == bookmark_service::MutationOutcome::Applied)
    }

    #[harmony(
        description = "The caller's bookmarks of the tracks, keyed by id. Tracks without a bookmark, other targets and tracks the caller cannot see are absent.",
        returns(HashMap<String, BookmarkRecord>)
    )]
    async fn get(
        track_ids: UniqueIds,
        #[context] db: Db,
        #[context] principal: Principal,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let db = db.read().await;
        let tracks = Resolved::of(&*db, track_ids.0)?;
        let values = bookmark_service::for_principal(&db, &principal, tracks.db_ids())?
            .into_iter()
            .map(|(id, bookmark)| {
                let record = BookmarkRecord {
                    position_ms: bookmark.position_ms,
                    comment: bookmark.comment,
                    created_at_ms: bookmark.created_at_ms,
                    updated_at_ms: bookmark.updated_at_ms,
                };
                Ok((id, harmony_luau::serializable_to_luau_owned(record)?))
            })
            .collect::<luau::runtime::Result<Vec<_>>>()?;
        Ok(tracks.keyed(values))
    }

    #[harmony(
        description = "Registers a callback for every user's bookmark changes made through `set` and `remove`, by any plugin or the REST API. An update follows the commit, also when `set` leaves the same position; a refused call, or a `remove` of what had no bookmark, sends none. Bookmarks that move when tracks merge, or go when their track or user is deleted, send none. An update carries the position but not the comment. The track may be one the plugin's own callers cannot see. Delivery is best effort: updates are dropped while the plugin runtime is saturated. Needs the `lyra.bookmarks.all` scope too.",
        args(handler: BookmarkUpdateHandler)
    )]
    fn on_update(
        vm: &luau::Vm,
        context: &luau::CallContext,
        handler: luau::Function,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        crate::plugins::update_callbacks::register::<BookmarkUpdatePayload>(
            vm,
            context,
            handler,
            "bookmarks.on_update",
        )
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::{
        DescribeInterface,
        DescribeModule,
        DescribeTypeAlias,
        FieldDescriptor,
        InterfaceDescriptor,
    };

    let field = |name, ty, description| FieldDescriptor {
        name,
        ty,
        description,
    };
    let mut bookmark = InterfaceDescriptor::new("Bookmark", None);
    bookmark.fields.extend([
        field(
            "position_ms",
            u64::luau_type(),
            Some("Position in the track."),
        ),
        field(
            "comment",
            Option::<String>::luau_type(),
            Some("Absent when the bookmark has none."),
        ),
        field("created_at_ms", i64::luau_type(), None),
        field(
            "updated_at_ms",
            i64::luau_type(),
            Some("When the bookmark was last set."),
        ),
    ]);
    let mut set_options = InterfaceDescriptor::new("BookmarkSetOptions", None);
    set_options.fields.push(field(
        "comment",
        Option::<String>::luau_type(),
        Some("A note on the position. Default none; empty stores none."),
    ));
    harmony_luau::render_definition_file_with_support(
        &BookmarksModule::module_descriptor(),
        &[BookmarkUpdateHandler::type_alias_descriptor()],
        &[
            bookmark,
            set_options,
            LuauBookmarkUpdatePayload::interface_descriptor(),
        ],
        &[],
    )
}
