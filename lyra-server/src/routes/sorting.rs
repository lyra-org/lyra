// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::{
    db,
    routes::AppError,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RouteSortSpec<K> {
    pub(crate) key: K,
    pub(crate) direction: db::SortDirection,
}

pub(crate) fn parse_route_sort_specs<K>(
    sort_by: Option<Vec<String>>,
    sort_order: Option<String>,
    map_token: impl Fn(&str) -> Option<K>,
    supported_values: &'static str,
) -> Result<Vec<RouteSortSpec<K>>, AppError> {
    let direction = db::parse_sort_direction(sort_order, true).map_err(|err| match err {
        db::SortSpecParseError::UnsupportedSortOrder(raw) => AppError::bad_request(format!(
            "Unsupported sort_order value: {}. Supported values: ascending, descending",
            raw
        )),
        other => AppError::bad_request(other.to_string()),
    })?;

    let mut sort = Vec::new();
    let mut unknown = Vec::new();
    if let Some(values) = sort_by {
        for value in values {
            for entry in value.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                let token = entry.to_ascii_lowercase();
                match map_token(&token) {
                    Some(key) => sort.push(RouteSortSpec { key, direction }),
                    None => unknown.push(token),
                }
            }
        }
    }

    if !unknown.is_empty() {
        return Err(AppError::bad_request(format!(
            "Unsupported sort_by value(s): {}. Supported values: {}",
            unknown.join(", "),
            supported_values
        )));
    }

    Ok(sort)
}
