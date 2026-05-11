// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod app;
mod artists;
mod covers;
mod entries;
mod error;
mod favorites;
mod genres;
mod labels;
mod libraries;
mod listens;
mod metadata;
mod mix;
mod playback_sessions;
mod playlists;
mod plugins;
mod providers;
pub(crate) mod registry;
mod releases;
pub(crate) mod responses;
mod roles;
mod search;
mod serve;
mod server;
mod tags;
mod tracks;
mod users;
mod websocket;

use schemars::JsonSchema;
use serde::{
    Deserialize,
    Deserializer,
    de,
};

use base64::{
    Engine as _,
    engine::general_purpose::URL_SAFE_NO_PAD,
};

use crate::{
    db,
    services::auth::Principal,
};

pub(crate) use app::build_core_api;
pub use artists::artist_routes;
pub use covers::cover_routes;
pub use entries::entry_routes;
pub(crate) use error::AppError;
pub use favorites::favorite_routes;
pub use genres::genre_routes;
pub use labels::label_routes;
pub use libraries::library_routes;
pub use listens::listen_routes;
pub use metadata::metadata_routes;
pub use mix::mix_routes;
pub use playback_sessions::playback_session_routes;
pub use playlists::playlist_routes;
pub use plugins::plugin_routes;
pub use providers::{
    entity_routes,
    provider_routes,
};
pub use releases::release_routes;
pub use roles::role_routes;
pub use search::search_routes;
pub(crate) use serve::{
    build_ranged_file_body,
    download_track_response,
    serve_hls_playlist_for_track,
    stream_track_response,
};
pub use serve::{
    download_routes,
    stream_routes,
};
pub use server::server_routes;
pub use tags::tag_routes;
pub use tracks::track_routes;
pub use users::{
    me_routes,
    user_routes,
};
pub(crate) use websocket::install as install_websocket;

const DEFAULT_PAGE_LIMIT: u64 = 100;
pub(crate) const PAGE_HARD_LIMIT: u64 = 500;
const PAGE_CURSOR_VERSION: u8 = 1;
const PAGE_CURSOR_NONCE_LEN: usize = 8;
const PAGE_CURSOR_BYTES_LEN: usize = 1 + PAGE_CURSOR_NONCE_LEN + size_of::<u64>();
const PAGE_CURSOR_MAX_LEN: usize = 32;
const PAGE_CURSOR_MASK_SEED: u64 = 0;

#[derive(Deserialize, JsonSchema, Default)]
pub(crate) struct PageQuery {
    #[schemars(description = "Page size. Default 100, cap 500.")]
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    limit: Option<u64>,
    #[schemars(description = "Opaque cursor from the previous page's `next_cursor`.")]
    cursor: Option<String>,
}

pub(crate) struct PageOptions {
    pub(crate) limit: u64,
    pub(crate) offset: u64,
}

impl PageQuery {
    pub(crate) fn resolve(self) -> Result<PageOptions, AppError> {
        let limit = self
            .limit
            .unwrap_or(DEFAULT_PAGE_LIMIT)
            .clamp(1, PAGE_HARD_LIMIT);
        let offset = self
            .cursor
            .as_deref()
            .map(decode_page_cursor)
            .transpose()?
            .unwrap_or(0);
        Ok(PageOptions { limit, offset })
    }
}

pub(crate) fn next_page_cursor(offset: u64, item_count: usize, total_count: u64) -> Option<String> {
    let next_offset = offset.saturating_add(item_count as u64);
    (next_offset < total_count).then(|| encode_page_cursor(next_offset))
}

fn encode_page_cursor(offset: u64) -> String {
    let nonce = rand::random::<[u8; PAGE_CURSOR_NONCE_LEN]>();
    let mut bytes = [0_u8; PAGE_CURSOR_BYTES_LEN];
    bytes[0] = PAGE_CURSOR_VERSION;
    bytes[1..1 + PAGE_CURSOR_NONCE_LEN].copy_from_slice(&nonce);

    let mask = xxh3::hash64_with_seed(&nonce, PAGE_CURSOR_MASK_SEED);
    let masked_offset = offset ^ mask;
    bytes[1 + PAGE_CURSOR_NONCE_LEN..].copy_from_slice(&masked_offset.to_be_bytes());
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_page_cursor(raw: &str) -> Result<u64, AppError> {
    if raw.is_empty() || raw.len() > PAGE_CURSOR_MAX_LEN {
        return Err(AppError::bad_request("malformed cursor"));
    }

    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| AppError::bad_request("malformed cursor"))?;
    if bytes.len() != PAGE_CURSOR_BYTES_LEN || bytes[0] != PAGE_CURSOR_VERSION {
        return Err(AppError::bad_request("malformed cursor"));
    }

    let nonce: [u8; PAGE_CURSOR_NONCE_LEN] = bytes[1..1 + PAGE_CURSOR_NONCE_LEN]
        .try_into()
        .expect("cursor nonce length is fixed");
    let masked_offset = u64::from_be_bytes(
        bytes[1 + PAGE_CURSOR_NONCE_LEN..]
            .try_into()
            .expect("cursor offset length is fixed"),
    );
    let mask = xxh3::hash64_with_seed(&nonce, PAGE_CURSOR_MASK_SEED);
    Ok(masked_offset ^ mask)
}

fn deserialize_optional_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Number(u64),
        String(String),
    }

    let Some(value) = Option::<Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    match value {
        Value::Number(value) => Ok(Some(value)),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed.parse::<u64>().map(Some).map_err(de::Error::custom)
        }
    }
}

pub(crate) fn parse_inc_values(
    inc: Option<Vec<String>>,
    supported: &[&str],
) -> Result<Vec<String>, AppError> {
    let Some(values) = inc else {
        return Ok(Vec::new());
    };

    let mut result = Vec::new();
    let mut unknown = Vec::new();
    for value in values {
        for entry in value.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let lowered = entry.to_ascii_lowercase();
            if supported.contains(&lowered.as_str()) {
                if !result.contains(&lowered) {
                    result.push(lowered);
                }
            } else {
                unknown.push(entry.to_string());
            }
        }
    }

    if !unknown.is_empty() {
        return Err(AppError::bad_request(format!(
            "Unsupported inc value(s): {}. Supported values: {}",
            unknown.join(", "),
            supported.join(", ")
        )));
    }

    Ok(result)
}

pub(crate) fn deserialize_inc<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IncValue {
        Single(String),
        Multiple(Vec<String>),
    }

    let value = Option::<IncValue>::deserialize(deserializer)?;
    Ok(value.map(|value| match value {
        IncValue::Single(entry) => vec![entry],
        IncValue::Multiple(entries) => entries,
    }))
}

pub(crate) fn parse_text_query(query: Option<String>) -> Option<String> {
    query.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

pub(crate) fn entity_accessible_to_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_db_id: agdb::DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&db::Permission::Admin) {
        return Ok(true);
    }
    Ok(db::libraries::get_for_entity(db, entity_db_id)?
        .into_iter()
        .any(|library| principal.accessible_library_ids.contains(&library.id)))
}

pub(crate) fn require_entity_accessible(
    db: &impl db::DbAccess,
    principal: &Principal,
    entity_db_id: agdb::DbId,
    not_found: impl FnOnce() -> AppError,
) -> Result<(), AppError> {
    if entity_accessible_to_principal(db, principal, entity_db_id)? {
        Ok(())
    } else {
        Err(not_found())
    }
}

pub(crate) fn playlist_accessible_to_principal(
    db: &impl db::DbAccess,
    principal: &Principal,
    playlist_db_id: agdb::DbId,
) -> anyhow::Result<bool> {
    if principal.permissions.contains(&db::Permission::Admin) {
        return Ok(true);
    }
    let Some(playlist) = db::playlists::get_by_id(db, playlist_db_id)? else {
        return Ok(false);
    };
    if playlist.is_public.unwrap_or(false) {
        return Ok(true);
    }
    Ok(db::playlists::get_owner(db, playlist_db_id)? == Some(principal.user_db_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_query_trims_and_ignores_empty_values() {
        assert_eq!(
            parse_text_query(Some("  blue  ".to_string())),
            Some("blue".to_string())
        );
        assert!(parse_text_query(Some("   ".to_string())).is_none());
        assert!(parse_text_query(None).is_none());
    }
}
