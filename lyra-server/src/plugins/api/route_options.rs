// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! The options a plugin registers its routes with.

use harmony_luau as luau;

use super::RouteAuthMode;
use crate::plugins::args;

/// The largest request body a route may declare, in bytes.
pub(crate) const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

const ROUTE_OWNER: &str = "route options";
const WEBSOCKET_OWNER: &str = "websocket options";

/// An HTTP route's options. Without `max_body_bytes` the route takes the server's request body
/// limit.
#[derive(Default)]
pub(crate) struct RouteOptions {
    pub(super) auth: Option<RouteAuthMode>,
    pub(super) case_insensitive: bool,
    pub(super) max_body_bytes: Option<usize>,
}

impl<'vm> luau::FromLuau<'vm> for RouteOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        args::reject_unknown_fields(
            &table,
            ROUTE_OWNER,
            &["auth", "case_insensitive", "max_body_bytes"],
        )?;
        Ok(Self {
            auth: args::optional_string_enum(&table, "auth", ROUTE_OWNER)?,
            case_insensitive: args::optional_bool(&table, "case_insensitive", ROUTE_OWNER)?
                .unwrap_or(false),
            max_body_bytes: read_max_body_bytes(&table)?,
        })
    }
}

fn read_max_body_bytes(table: &luau::Table) -> luau::runtime::Result<Option<usize>> {
    let Some(value) = args::optional_i64(table, "max_body_bytes", ROUTE_OWNER)? else {
        return Ok(None);
    };
    match usize::try_from(value) {
        Ok(bytes) if (1..=MAX_BODY_BYTES).contains(&bytes) => Ok(Some(bytes)),
        _ => Err(crate::plugins::runtime_error(format!(
            "{ROUTE_OWNER}.max_body_bytes must be between 1 and MAX_BODY_BYTES ({MAX_BODY_BYTES}), got {value}"
        ))),
    }
}

/// A WebSocket route's options.
#[derive(Default)]
pub(crate) struct WebSocketRouteOptions {
    pub(super) auth: Option<RouteAuthMode>,
}

impl<'vm> luau::FromLuau<'vm> for WebSocketRouteOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let table: luau::Table = reader.read()?;
        args::reject_unknown_fields(&table, WEBSOCKET_OWNER, &["auth"])?;
        Ok(Self {
            auth: args::optional_string_enum(&table, "auth", WEBSOCKET_OWNER)?,
        })
    }
}
