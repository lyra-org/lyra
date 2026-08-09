// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

mod app;
mod artists;
mod covers;
mod entity_metadata;
mod entries;
mod error;
mod favorites;
mod genres;
mod labels;
mod libraries;
mod listens;
mod metadata;
mod mix;
mod pagination;
mod playbacks;
mod playlists;
mod plugins;
mod providers;
mod ratings;
pub(crate) mod registry;
mod releases;
pub(crate) mod responses;
mod roles;
mod search;
mod serve;
mod server;
mod sorting;
mod sync;
mod tags;
mod tracks;
mod users;
mod websocket;

use serde::{
    Deserialize,
    Deserializer,
    de,
};
use std::collections::HashMap;
use time::{
    OffsetDateTime,
    format_description::well_known::Rfc3339,
};

use crate::{
    db,
    services::entities::ResolvedCreditedArtist,
};
use agdb::DbId;

pub(crate) use app::build_core_api;
pub use artists::artist_routes;
pub use covers::cover_routes;
pub(crate) use entity_metadata::entity_metadata_routes;
pub use entries::entry_routes;
pub(crate) use error::AppError;
pub use favorites::favorite_routes;
pub use genres::genre_routes;
pub use labels::label_routes;
pub use libraries::library_routes;
pub use listens::listen_routes;
pub use metadata::metadata_routes;
pub(crate) use pagination::{
    SnapshotPageRequest,
    load_snapshot_items,
};
pub use playbacks::playback_routes;
pub use playlists::playlist_routes;
pub use plugins::plugin_routes;
pub use providers::{
    entity_routes,
    provider_routes,
};
pub use ratings::rating_routes;
pub use releases::release_routes;
pub use roles::role_routes;
pub use search::search_routes;
pub(crate) use serve::{
    DownloadTrackRequest,
    ServeTrackOptions,
    build_ranged_file_body,
    download_track_response,
    serve_hls_playlist_for_track,
    stream_track_response,
};
pub use sync::sync_routes;

pub use serve::{
    download_routes,
    stream_routes,
};
pub use server::server_routes;
pub(crate) use sorting::{
    RouteSortSpec,
    parse_route_sort_specs,
};
pub use tags::tag_routes;
pub use tracks::track_routes;
pub use users::{
    me_routes,
    user_routes,
};
pub(crate) use websocket::install as install_websocket;

#[cfg(feature = "docgen")]
pub(crate) use app::build_openapi_spec;

pub(crate) fn db_ids_from_credited_artists<'a>(
    artists: impl IntoIterator<Item = &'a ResolvedCreditedArtist>,
) -> Vec<DbId> {
    artists
        .into_iter()
        .filter_map(|artist| artist.artist.db_id.clone().map(DbId::from))
        .collect()
}

pub(crate) fn credited_artist_responses(
    artists: Vec<ResolvedCreditedArtist>,
    artist_covers: Option<&HashMap<DbId, db::Cover>>,
) -> Vec<responses::ArtistResponse> {
    artists
        .into_iter()
        .map(|artist| {
            let artist_db_id = artist.artist.db_id.clone().map(DbId::from);
            let mut response = responses::ArtistResponse::from(artist);
            if let Some(artist_covers) = artist_covers {
                response.cover = Some(
                    artist_db_id
                        .and_then(|id| artist_covers.get(&id).cloned())
                        .map(covers::cover_to_response),
                );
            }
            response
        })
        .collect()
}

const DEFAULT_PAGE_LIMIT: u64 = 100;
pub(crate) const PAGE_HARD_LIMIT: u64 = 500;

#[cfg_attr(feature = "docgen", derive(schemars::JsonSchema))]
#[derive(Clone, Deserialize, Default)]
pub(crate) struct PageQuery {
    #[cfg_attr(
        feature = "docgen",
        schemars(description = "Page size. Default 100, cap 500.")
    )]
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    limit: Option<u64>,
    #[cfg_attr(
        feature = "docgen",
        schemars(
            description = "Opaque cursor from the previous page's `next_cursor`. Repeat the same collection-shaping query parameters on continuation requests. Cursors are bound to the authenticated user and collection snapshot; expired, evicted, or mismatched cursors return 409 and pagination must restart."
        )
    )]
    cursor: Option<String>,
}

pub(crate) fn unix_secs_to_rfc3339_u64(seconds: u64) -> String {
    let seconds = i64::try_from(seconds).expect("Unix timestamp seconds should fit in i64");
    unix_secs_to_rfc3339_i64(seconds)
}

pub(crate) fn unix_secs_to_rfc3339_i64(seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(seconds)
        .expect("Unix timestamp seconds should be RFC3339-representable")
        .format(&Rfc3339)
        .expect("RFC3339 formatting should succeed")
}

pub(crate) fn unix_ms_to_rfc3339_u64(milliseconds: u64) -> String {
    unix_ms_to_rfc3339_i128(i128::from(milliseconds))
}

pub(crate) fn unix_ms_to_rfc3339_i64(milliseconds: i64) -> String {
    unix_ms_to_rfc3339_i128(i128::from(milliseconds))
}

fn unix_ms_to_rfc3339_i128(milliseconds: i128) -> String {
    let nanoseconds = milliseconds
        .checked_mul(1_000_000)
        .expect("Unix timestamp milliseconds should fit in nanoseconds");
    OffsetDateTime::from_unix_timestamp_nanos(nanoseconds)
        .expect("Unix timestamp milliseconds should be RFC3339-representable")
        .format(&Rfc3339)
        .expect("RFC3339 formatting should succeed")
}

/// Distinguishes an absent JSON field from an explicit `null` in a PATCH body.
///
/// The field must also carry `#[serde(default)]`: this runs only when the key
/// is present, so absence falls through to `Default` (`None`) while a present
/// `null` yields `Some(None)`. Without it, serde collapses both to `None` and
/// the clear is unreachable from the wire.
pub(crate) fn double_option<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
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

pub(crate) fn deserialize_optional_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_optional_u64(deserializer)?
        .map(usize::try_from)
        .transpose()
        .map_err(|_| de::Error::custom("number is too large"))
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
