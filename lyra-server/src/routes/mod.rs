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
mod ws;

use serde::{
    Deserialize,
    Deserializer,
};

pub(crate) use app::build_core_api;
pub use artists::artist_routes;
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
pub(crate) use ws::install as install_ws;

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
