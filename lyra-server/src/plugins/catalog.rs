// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The Luau side of catalog queries, shared by the catalog modules.

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    FieldDescriptor,
    LuauType,
    LuauTypeInfo,
    TypeAliasDescriptor,
};
use serde::Serialize;

use crate::{
    plugins::{
        args,
        db::DbAccess,
    },
    services::{
        auth::Principal,
        catalog::{
            ArtistCredit,
            CatalogError,
            CreditRole,
            Direction,
            NameRange,
            Page,
            Query,
            Viewer,
            pipeline::{
                Catalog,
                SortKey,
            },
        },
    },
};

/// The viewer a query runs for, verified under the caller's guard.
pub(crate) fn viewer(
    db: &impl DbAccess,
    principal: Option<Principal>,
) -> luau::runtime::Result<Viewer> {
    principal.map_or(Ok(Viewer::System), |principal| {
        Viewer::user(db, principal).map_err(crate::plugins::runtime_error)
    })
}

pub(crate) fn error(error: CatalogError) -> luau::Error {
    crate::plugins::runtime_error(error)
}

/// A query read from the fields every catalog shares, with the page bounds.
pub(crate) struct PagedQuery<C: Catalog> {
    pub(crate) query: Query<C>,
    pub(crate) offset: u64,
    pub(crate) limit: Option<u64>,
}

pub(crate) fn read_query<C: Catalog>(
    vm: &luau::Vm,
    table: &luau::Table,
    filter: C::Filter,
) -> luau::runtime::Result<PagedQuery<C>> {
    let mut query = Query::new(filter);
    query.names = NameRange::new(
        luau::table::optional_string_field(vm, table, "sort_name_prefix")?,
        luau::table::optional_string_field(vm, table, "sort_name_at_least")?,
        luau::table::optional_string_field(vm, table, "sort_name_below")?,
    );
    query.search = luau::table::optional_string_field(vm, table, "search")?;
    query.sort = read_sort(vm, table)?;
    query.seed = args::optional_u64(vm, table, "seed")?.unwrap_or(0);
    Ok(PagedQuery {
        query,
        offset: args::optional_u64(vm, table, "offset")?.unwrap_or(0),
        limit: args::optional_u64(vm, table, "limit")?,
    })
}

fn read_sort<K: SortKey>(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Vec<(K, Direction)>> {
    let Some(terms) = luau::table::optional_table_field(vm, table, "sort")? else {
        return Ok(Vec::new());
    };
    args::array_values(vm, &terms)?
        .into_iter()
        .map(|(index, term)| {
            let luau::Value::Table(term) = term else {
                return Err(crate::plugins::runtime_error(format!(
                    "sort entry {index} must be a table with a key, got {}",
                    term.type_name()
                )));
            };
            let key =
                K::parse(&luau::table::required_string_field(vm, &term, "key")?).map_err(error)?;
            let direction = luau::table::optional_string_field(vm, &term, "order")?
                .as_deref()
                .map(Direction::parse)
                .transpose()
                .map_err(error)?
                .unwrap_or(Direction::Ascending);
            Ok((key, direction))
        })
        .collect()
}

/// The `artist_ids` filter with its `credit_role` and `exclude_credit_role`.
pub(crate) fn read_artist_credit(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<Option<ArtistCredit>> {
    let role = |key| {
        luau::table::optional_string_field(vm, table, key)?
            .as_deref()
            .map(CreditRole::parse)
            .transpose()
            .map_err(error)
    };
    let role_filter = role("credit_role")?;
    let excluding = role("exclude_credit_role")?;
    let Some(artists) = luau::table::optional_table_field(vm, table, "artist_ids")? else {
        if role_filter.is_some() || excluding.is_some() {
            return Err(crate::plugins::runtime_error(
                "credit_role and exclude_credit_role need artist_ids",
            ));
        }
        return Ok(None);
    };
    Ok(Some(ArtistCredit {
        artists: args::unique_ids(vm, &artists)?,
        role: role_filter.unwrap_or(CreditRole::Any),
        excluding,
    }))
}

pub(crate) fn page_table<T: Serialize>(page: Page<T>) -> luau::runtime::Result<luau::OwnedTable> {
    let mut table = luau::OwnedTable::with_capacity(0, 3);
    table.set_field(
        "items",
        harmony_luau::serializable_to_luau_owned(page.items)?,
    );
    table.set_field("total", luau::Value::from(page.total as i64));
    table.set_field("offset", luau::Value::from(page.offset as i64));
    Ok(table)
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

/// The query fields every catalog shares, for its query interface.
#[cfg(feature = "docgen")]
pub(crate) fn query_fields(sort_key: &'static str) -> Vec<FieldDescriptor> {
    vec![
        field("sort_name_prefix", Option::<String>::luau_type()),
        field("sort_name_at_least", Option::<String>::luau_type()),
        field("sort_name_below", Option::<String>::luau_type()),
        field("search", Option::<String>::luau_type()),
        field(
            "sort",
            LuauType::optional(LuauType::array(LuauType::object(vec![
                field("key", LuauType::named(sort_key)),
                field("order", LuauType::optional(args::sort_order_type())),
            ]))),
        ),
        field("seed", Option::<u64>::luau_type()),
        field("offset", Option::<u64>::luau_type()),
        field("limit", Option::<u64>::luau_type()),
    ]
}

#[cfg(feature = "docgen")]
pub(crate) fn credit_fields() -> Vec<FieldDescriptor> {
    let role = LuauType::optional(LuauType::named("CreditRole"));
    vec![
        field("artist_ids", Option::<Vec<u64>>::luau_type()),
        field("credit_role", role.clone()),
        field("exclude_credit_role", role),
    ]
}

/// The `CreditRole` type alias, for a catalog module with credit filters.
#[cfg(feature = "docgen")]
pub(crate) fn credit_role_alias() -> TypeAliasDescriptor {
    TypeAliasDescriptor::new(
        "CreditRole",
        LuauType::union(
            CreditRole::KEYS
                .iter()
                .map(|(token, _)| LuauType::string_literal(token))
                .collect(),
        ),
        None,
    )
}

/// The `<Sort>Key` and `<Page>` type aliases for a catalog module.
#[cfg(feature = "docgen")]
pub(crate) fn type_aliases<K: SortKey>(
    sort_key: &'static str,
    page: &'static str,
    item: &'static str,
) -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            sort_key,
            LuauType::union(
                K::KEYS
                    .iter()
                    .map(|(token, _)| LuauType::string_literal(token))
                    .collect(),
            ),
            None,
        ),
        TypeAliasDescriptor::new(
            page,
            LuauType::object(vec![
                field("items", LuauType::array(LuauType::named(item))),
                field("total", u64::luau_type()),
                field("offset", u64::luau_type()),
            ]),
            None,
        ),
    ]
}
