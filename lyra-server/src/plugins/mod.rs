// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashSet,
    sync::Arc,
};

use agdb::DbId;
use harmony_luau::{
    DescribeTypeAlias,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use mlua::{
    DeserializeOptions,
    Function,
    IntoLua,
    Lua,
    LuaSerdeExt,
    SerializeOptions,
    Table,
    Value,
};
use serde::de::DeserializeOwned;

use crate::db::{
    ListOptions,
    PagedResult,
    SortDirection,
    parse_sort_specs_tokens,
};

pub(crate) mod api;
pub(crate) mod artists;
pub(crate) mod auth;
pub(crate) mod bootstrap;
pub(crate) mod chromaprint;
pub(crate) mod covers;
pub(crate) mod datastore;
pub(crate) mod docs;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod favorites;
pub(crate) mod genres;
pub(crate) mod globals;
pub(crate) mod ids;
pub(crate) mod images;
pub(crate) mod labels;
pub(crate) mod libraries;
pub(crate) mod lifecycle;
pub(crate) mod listens;
pub(crate) mod lyrics;
pub(crate) mod metadata;
pub(crate) mod mix;
pub(crate) mod playback_sessions;
pub(crate) mod playback_sources;
pub(crate) mod playlists;
pub(crate) mod releases;
pub(crate) mod runtime;
pub(crate) mod server;
mod surfaces;
pub(crate) mod tags;
pub(crate) mod track_sources;
pub(crate) mod tracks;
pub(crate) mod users;
pub(crate) use surfaces::{
    lyra_doc_source_ids,
    lyra_modules,
    render_lyra_doc_source,
};

pub(crate) const LUA_SERIALIZE_OPTIONS: SerializeOptions = SerializeOptions::new()
    .serialize_none_to_null(false)
    .serialize_unit_to_null(false)
    .set_array_metatable(false);

#[harmony_macros::interface]
pub(crate) struct OptionConfig {
    name: String,
    label: String,
    r#type: String,
    requires_settings: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PluginSortOrder {
    Ascending,
    Descending,
}

impl PluginSortOrder {
    fn parse(raw: Option<String>) -> mlua::Result<Option<Self>> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Ok(None);
        }

        match normalized.as_str() {
            "ascending" => Ok(Some(Self::Ascending)),
            "descending" => Ok(Some(Self::Descending)),
            _ => Err(mlua::Error::runtime(format!(
                "unsupported sort_order value: {raw}"
            ))),
        }
    }
}

impl From<PluginSortOrder> for SortDirection {
    fn from(order: PluginSortOrder) -> Self {
        match order {
            PluginSortOrder::Ascending => SortDirection::Ascending,
            PluginSortOrder::Descending => SortDirection::Descending,
        }
    }
}

impl LuauTypeInfo for PluginSortOrder {
    fn luau_type() -> LuauType {
        LuauType::union(vec![
            LuauType::literal("\"ascending\""),
            LuauType::literal("\"descending\""),
        ])
    }
}

impl DescribeTypeAlias for PluginSortOrder {
    fn type_alias_descriptor() -> TypeAliasDescriptor {
        TypeAliasDescriptor::new(
            "PluginSortOrder",
            Self::luau_type(),
            Some("Plugin query sort order (ascending or descending)."),
        )
    }
}

pub(crate) fn from_lua_json_value<T>(lua: &Lua, value: Value) -> mlua::Result<T>
where
    T: DeserializeOwned,
{
    lua.from_value_with(
        value,
        DeserializeOptions::new().encode_empty_tables_as_array(true),
    )
}

pub(crate) fn require_positive_id(value: i64, field_name: &str) -> mlua::Result<DbId> {
    if value <= 0 {
        return Err(mlua::Error::runtime(format!(
            "{field_name} must be a positive id"
        )));
    }

    Ok(DbId(value))
}

pub(crate) fn require_non_empty_string(value: String, field_name: &str) -> mlua::Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(mlua::Error::runtime(format!(
            "{field_name} must be a non-empty string"
        )));
    }

    Ok(value)
}

pub(crate) fn id_from_function(handler: &Function) -> mlua::Result<Arc<str>> {
    let info = handler.info();
    let source = info.source.as_deref().or(info.short_src.as_deref());
    source
        .and_then(harmony_core::parse_plugin_id)
        .map(Arc::from)
        .ok_or_else(|| {
            mlua::Error::runtime(
                "could not determine plugin id from handler source (expected plugins/<id>/...)",
            )
        })
}

#[cfg(test)]
mod parse_plugin_id_tests {
    use harmony_core::parse_plugin_id;

    #[test]
    fn accepts_anchored_plain_source() {
        assert_eq!(
            parse_plugin_id("plugins/musicbrainz/init.luau"),
            Some("musicbrainz")
        );
    }

    #[test]
    fn accepts_anchored_at_prefixed_source() {
        assert_eq!(
            parse_plugin_id("@plugins/jellyfin/lib/util.luau"),
            Some("jellyfin")
        );
    }

    #[test]
    fn rejects_substring_match_in_path() {
        assert_eq!(parse_plugin_id("not_plugins/plugins/victim/x"), None);
        assert_eq!(parse_plugin_id("@a/b/plugins/victim/x"), None);
    }

    #[test]
    fn rejects_empty_id_or_missing_prefix() {
        assert_eq!(parse_plugin_id("plugins//init.luau"), None);
        assert_eq!(parse_plugin_id("scratch"), None);
        assert_eq!(parse_plugin_id(""), None);
    }
}

/// Wraps the `#[harmony_macros::module]`-generated `module()` to
/// attach the capability scope. The macro itself only knows the
/// Rust-side module shape; the scope id/description/danger live
/// here so they sit next to the rest of the Lua surface file.
macro_rules! plugin_surface_exports {
    ($module_ty:ty, $scope_id:literal, $description:literal, $danger:ident) => {
        pub(crate) fn get_module() -> ::harmony_core::Module {
            let mut m = <$module_ty>::module();
            m.scope = ::harmony_core::Scope {
                id: $scope_id.into(),
                description: $description,
                danger: ::harmony_core::Danger::$danger,
            };
            m
        }

        pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
            <$module_ty>::render_luau_definition()
        }
    };
}

pub(crate) use plugin_surface_exports;

/// Parse a `ListOptions` from a Lua table.
pub(crate) fn parse_list_options(table: &mlua::Table) -> mlua::Result<ListOptions> {
    let sort_by = match table.get::<Option<mlua::Table>>("sort_by")? {
        Some(sort_table) => {
            let mut tokens = Vec::new();
            for pair in sort_table.sequence_values::<String>() {
                tokens.push(pair?);
            }
            Some(tokens)
        }
        None => None,
    };
    let direction = PluginSortOrder::parse(table.get::<Option<String>>("sort_order")?)?
        .map(SortDirection::from)
        .unwrap_or(SortDirection::Ascending);
    let sort = parse_sort_specs_tokens(sort_by, direction, |_| true, false)
        .map_err(|err| mlua::Error::RuntimeError(err.to_string()))?;

    let offset = table.get::<Option<u64>>("offset")?;
    let limit = table.get::<Option<u64>>("limit")?;
    let search_term = table.get::<Option<String>>("search_term")?;

    Ok(ListOptions {
        sort,
        offset,
        limit,
        search_term,
    })
}

pub(crate) fn entry_to_table(lua: &Lua, entry: crate::db::Entry) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    if let Some(db_id) = entry.db_id {
        table.set("db_id", db_id.0)?;
    } else {
        table.set("db_id", Value::Nil)?;
    }
    table.set("full_path", entry.full_path.to_string_lossy().to_string())?;
    table.set("kind", entry.kind.to_string())?;
    table.set("name", entry.name)?;
    if let Some(hash) = entry.hash {
        table.set("hash", hash)?;
    } else {
        table.set("hash", Value::Nil)?;
    }
    table.set("size", entry.size)?;
    table.set("mtime", entry.mtime)?;
    Ok(table)
}

/// Convert a `PagedResult<T>` into a Lua table with shape:
/// `{ entities = [...], total_count = N, offset = N }`
pub(crate) fn paged_result_to_table<T: IntoLua>(
    lua: &mlua::Lua,
    result: PagedResult<T>,
) -> mlua::Result<mlua::Table> {
    let entities_table = lua.create_table()?;
    for (i, entity) in result.entries.into_iter().enumerate() {
        entities_table.set(i + 1, entity.into_lua(lua)?)?;
    }

    let table = lua.create_table()?;
    table.set("entities", entities_table)?;
    table.set("total_count", result.total_count)?;
    table.set("offset", result.offset)?;

    Ok(table)
}

/// Parse a Lua table of numeric IDs into a deduplicated `Vec<DbId>`,
/// skipping non-positive values.
pub(crate) fn parse_ids(ids: Table) -> mlua::Result<Vec<agdb::DbId>> {
    let mut parsed = Vec::new();
    let mut seen = HashSet::new();
    for value in ids.sequence_values::<i64>() {
        let raw_id = value?;
        if raw_id <= 0 {
            continue;
        }
        let id = agdb::DbId(raw_id);
        if seen.insert(id) {
            parsed.push(id);
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{
        PluginSortOrder,
        SortDirection,
        parse_ids,
        parse_list_options,
    };

    #[test]
    fn plugin_sort_order_rejects_unknown_values() {
        let lua = mlua::Lua::new();
        let table = lua.create_table().unwrap();
        table.set("sort_order", "sideways").unwrap();

        assert!(parse_list_options(&table).is_err());
    }

    #[test]
    fn plugin_sort_order_normalizes_known_values() {
        assert!(matches!(
            PluginSortOrder::parse(Some("ascending".to_string()))
                .unwrap()
                .map(SortDirection::from),
            Some(SortDirection::Ascending)
        ));
        assert!(matches!(
            PluginSortOrder::parse(Some("descending".to_string()))
                .unwrap()
                .map(SortDirection::from),
            Some(SortDirection::Descending)
        ));
    }

    #[test]
    fn parse_ids_deduplicates_and_skips_non_positive_values() {
        let lua = mlua::Lua::new();
        let table = lua.create_table().unwrap();
        table.set(1, 4).unwrap();
        table.set(2, 0).unwrap();
        table.set(3, 4).unwrap();
        table.set(4, -1).unwrap();
        table.set(5, 7).unwrap();

        let ids = parse_ids(table).unwrap();
        assert_eq!(ids, vec![agdb::DbId(4), agdb::DbId(7)]);
    }
}
