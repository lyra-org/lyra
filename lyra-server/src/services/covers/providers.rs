// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    time::{
        Duration,
        Instant,
    },
};

use agdb::{
    DbAny,
    DbId,
};
use anyhow::{
    Result,
    anyhow,
};
use mlua::LuaSerdeExt;
use mlua::Value as LuaValue;
use serde_json::Value;
use std::sync::LazyLock;
use tokio::sync::oneshot;

use crate::{
    STATE,
    db::{
        self,
        Artist,
        Release,
        Track,
        libraries::Library,
    },
    services::providers::{
        PROVIDER_REGISTRY,
        ProviderCoverRequireSpec,
    },
};

use super::CoverImageCandidate;

const PROVIDER_COVER_CACHE_HIT_TTL: Duration = Duration::from_secs(30 * 60);
const PROVIDER_COVER_CACHE_MISS_TTL: Duration = Duration::from_secs(5 * 60);
pub(crate) const DEFAULT_COVER_HANDLER_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) use crate::services::EntityType;

pub(crate) struct ProviderSearchRequest<'a> {
    pub(crate) provider_id: &'a str,
    pub(crate) entity_type: EntityType,
    pub(crate) query: &'a str,
    pub(crate) include_cover_urls: bool,
    pub(crate) force_refresh: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderSearchError {
    #[error("No search handler for provider '{provider_id}' and type '{entity_type}'")]
    NoSearchHandler {
        provider_id: String,
        entity_type: &'static str,
    },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<mlua::Error> for ProviderSearchError {
    fn from(value: mlua::Error) -> Self {
        Self::Internal(anyhow::Error::from(value))
    }
}

impl From<serde_json::Error> for ProviderSearchError {
    fn from(value: serde_json::Error) -> Self {
        Self::Internal(anyhow::Error::from(value))
    }
}

pub(crate) enum NormalizedProviderSearchResult {
    Release(NormalizedProviderReleaseSearchResult),
    Artist(NormalizedProviderArtistSearchResult),
    Track(NormalizedProviderTrackSearchResult),
}

pub(crate) struct NormalizedProviderReleaseSearchResult {
    pub(crate) title: String,
    pub(crate) redirect_url: String,
    pub(crate) artist_name: Option<String>,
    pub(crate) release_date: Option<String>,
    pub(crate) genres: Option<Vec<String>>,
    pub(crate) description: Option<String>,
    pub(crate) sort_name: Option<String>,
    pub(crate) sort_title: Option<String>,
    pub(crate) ids: Option<HashMap<String, String>>,
    pub(crate) cover_url: Option<String>,
    pub(crate) raw: Value,
}

pub(crate) struct NormalizedProviderArtistSearchResult {
    pub(crate) title: String,
    pub(crate) redirect_url: String,
    pub(crate) artist_name: Option<String>,
    pub(crate) sort_name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) ids: Option<HashMap<String, String>>,
    pub(crate) cover_url: Option<String>,
    pub(crate) raw: Value,
}

pub(crate) struct NormalizedProviderTrackSearchResult {
    pub(crate) title: String,
    pub(crate) redirect_url: String,
    pub(crate) artist_name: Option<String>,
    pub(crate) release_title: Option<String>,
    pub(crate) sort_title: Option<String>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) disc: Option<u32>,
    pub(crate) disc_total: Option<u32>,
    pub(crate) track: Option<u32>,
    pub(crate) track_total: Option<u32>,
    pub(crate) ids: Option<HashMap<String, String>>,
    pub(crate) cover_url: Option<String>,
    pub(crate) raw: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ProviderCoverSearchCacheKey {
    provider_id: String,
    entity_type: EntityType,
    context_hash: u64,
}

#[derive(Clone, Debug)]
struct CachedProviderCoverSearchResult {
    result: Option<super::ProviderCoverSearchResult>,
    expires_at: Instant,
}

#[derive(Debug)]
enum ProviderCoverSearchCacheEntry {
    Ready(CachedProviderCoverSearchResult),
    Pending(Vec<oneshot::Sender<()>>),
}

static PROVIDER_COVER_SEARCH_CACHE: LazyLock<
    tokio::sync::Mutex<HashMap<ProviderCoverSearchCacheKey, ProviderCoverSearchCacheEntry>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

pub(crate) async fn clear_cover_search_cache() {
    PROVIDER_COVER_SEARCH_CACHE.lock().await.clear();
}

fn as_string_value(value: &Value) -> Option<String> {
    value.as_str().map(str::to_string)
}

fn first_string_value(value: &Value, keys: &[&str]) -> Option<String> {
    let object = value.as_object()?;
    keys.iter()
        .find_map(|key| object.get(*key).and_then(as_string_value))
        .filter(|value| !value.is_empty())
}

fn as_u32_value(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(value) => value.parse::<u32>().ok(),
        _ => None,
    }
}

fn as_u64_value(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn as_genres(value: &Value) -> Option<Vec<String>> {
    let genres = value.as_array()?;
    let items: Vec<String> = genres.iter().filter_map(as_string_value).collect();
    if items.is_empty() { None } else { Some(items) }
}

fn as_ids(value: &Value) -> Option<HashMap<String, String>> {
    let object = value.as_object()?;
    let ids = object.get("ids")?;
    let ids = ids.as_object()?;
    let mut result = HashMap::new();
    for (key, value) in ids {
        let value = match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        };
        if let Some(value) = value {
            result.insert(key.to_string(), value);
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn required_title(value: &Value, keys: &[&str]) -> String {
    first_string_value(value, keys).unwrap_or_else(|| "Unknown".to_string())
}

fn required_redirect_url(value: &Value) -> String {
    first_string_value(value, &["redirect_url", "url", "link", "href"]).unwrap_or_default()
}

fn normalize_provider_search_result(
    entity_type: EntityType,
    value: Value,
    include_cover_url: bool,
    resolved_cover_url: Option<String>,
) -> NormalizedProviderSearchResult {
    let ids = as_ids(&value);
    let cover_url = if include_cover_url {
        resolved_cover_url.or_else(|| extract_cover_url(&value))
    } else {
        None
    };
    let redirect_url = required_redirect_url(&value);

    match entity_type {
        EntityType::Artist => {
            NormalizedProviderSearchResult::Artist(NormalizedProviderArtistSearchResult {
                title: required_title(&value, &["title", "artist_name", "name"]),
                redirect_url: redirect_url.clone(),
                artist_name: first_string_value(&value, &["artist_name", "name"]),
                sort_name: first_string_value(&value, &["sort_name"]),
                description: first_string_value(&value, &["description"]),
                ids,
                cover_url,
                raw: value,
            })
        }
        EntityType::Track => {
            NormalizedProviderSearchResult::Track(NormalizedProviderTrackSearchResult {
                title: required_title(&value, &["title", "track_title", "name"]),
                redirect_url: redirect_url.clone(),
                artist_name: first_string_value(&value, &["artist_name", "artist", "name"]),
                release_title: first_string_value(&value, &["release_title"]),
                sort_title: first_string_value(&value, &["sort_title"]),
                duration_ms: value
                    .as_object()
                    .and_then(|o| o.get("duration_ms"))
                    .and_then(as_u64_value),
                disc: value
                    .as_object()
                    .and_then(|o| o.get("disc"))
                    .and_then(as_u32_value),
                disc_total: value
                    .as_object()
                    .and_then(|o| o.get("disc_total"))
                    .and_then(as_u32_value),
                track: value
                    .as_object()
                    .and_then(|o| o.get("track"))
                    .and_then(as_u32_value),
                track_total: value
                    .as_object()
                    .and_then(|o| o.get("track_total"))
                    .and_then(as_u32_value),
                ids,
                cover_url,
                raw: value,
            })
        }
        EntityType::Release => {
            NormalizedProviderSearchResult::Release(NormalizedProviderReleaseSearchResult {
                title: required_title(&value, &["title", "release_title", "track_title", "name"]),
                redirect_url,
                artist_name: first_string_value(&value, &["artist_name", "artist", "name"]),
                release_date: value
                    .as_object()
                    .and_then(|o| o.get("release_date"))
                    .and_then(|v| v.as_str())
                    .and_then(db::releases::normalize_release_date),
                genres: value
                    .as_object()
                    .and_then(|o| o.get("genres"))
                    .and_then(as_genres),
                description: first_string_value(&value, &["description"]),
                sort_name: first_string_value(&value, &["sort_name"]),
                sort_title: first_string_value(&value, &["sort_title"]),
                ids,
                cover_url,
                raw: value,
            })
        }
    }
}

pub(crate) async fn search_provider(
    request: ProviderSearchRequest<'_>,
) -> std::result::Result<Vec<NormalizedProviderSearchResult>, ProviderSearchError> {
    let registry = PROVIDER_REGISTRY.read().await;
    let handler = registry
        .get_search_handler(request.provider_id, request.entity_type)
        .ok_or_else(|| ProviderSearchError::NoSearchHandler {
            provider_id: request.provider_id.to_string(),
            entity_type: request.entity_type.as_str(),
        })?
        .clone();
    drop(registry);

    let results: Vec<LuaValue> = handler
        .call_async::<_, Vec<LuaValue>>(request.query.to_string())
        .await?;
    let include_cover_urls = request.include_cover_urls
        && matches!(
            request.entity_type,
            EntityType::Release | EntityType::Artist
        );

    let mut normalized = Vec::with_capacity(results.len());
    for result in results {
        let json_value = serde_json::to_value(&result)?;
        let resolved_cover_url = if include_cover_urls {
            let ids = as_ids(&json_value).unwrap_or_default();
            if ids.is_empty() {
                None
            } else {
                match resolve_provider_cover_url(
                    request.provider_id,
                    request.entity_type,
                    &serde_json::json!({ "ids": ids }),
                    request.force_refresh,
                )
                .await
                {
                    Ok(url) => url,
                    Err(err) => {
                        tracing::warn!(
                            provider = request.provider_id,
                            error = %err,
                            "provider cover URL lookup failed during search response build"
                        );
                        None
                    }
                }
            }
        } else {
            None
        };

        normalized.push(normalize_provider_search_result(
            request.entity_type,
            json_value,
            include_cover_urls,
            resolved_cover_url,
        ));
    }

    Ok(normalized)
}

pub(crate) fn extract_cover_url(result: &Value) -> Option<String> {
    let obj = result.as_object()?;
    for key in ["cover_url", "cover_image_url", "cover"] {
        if let Some(value) = obj.get(key).and_then(|v| v.as_str()) {
            let candidate = value.trim();
            if !candidate.is_empty() {
                return Some(candidate.to_string());
            }
        }
    }

    None
}

fn cover_handler_context(context: &Value, force_refresh: bool) -> Value {
    if !force_refresh {
        return context.clone();
    }

    let mut context = context.clone();
    if let Value::Object(object) = &mut context {
        object.insert(
            "cover_options".to_string(),
            serde_json::json!({
                "force_refresh": true,
            }),
        );
    }
    context
}

pub(crate) fn provider_external_ids_for_entity(
    db: &DbAny,
    entity_id: DbId,
    provider_id: &str,
) -> Result<HashMap<String, String>> {
    let mut ids = HashMap::new();
    for external_id in db::external_ids::get_for_entity(db, entity_id)? {
        if external_id.provider_id != provider_id {
            continue;
        }
        let value = external_id.id_value.trim();
        if value.is_empty() {
            continue;
        }
        ids.insert(external_id.id_type, value.to_string());
    }

    Ok(ids)
}

pub(crate) fn library_for_release(db: &DbAny, release_id: DbId) -> Result<Option<Library>> {
    Ok(db::libraries::get_by_release(db, release_id)?
        .into_iter()
        .next())
}

fn library_context_value(library: &Library) -> Value {
    serde_json::json!({
        "db_id": library.db_id.map(|id| id.0),
        "id": library.id,
        "name": library.name,
        "directory": library.path.to_string_lossy().to_string(),
        "language": library.language,
        "country": library.country,
    })
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cursor = value;
    for segment in path.split('.') {
        let Value::Object(object) = cursor else {
            return None;
        };
        cursor = object.get(segment)?;
    }
    Some(cursor)
}

fn is_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
        _ => true,
    }
}

fn path_is_present(context: &Value, path: &str) -> bool {
    value_at_path(context, path).is_some_and(is_present)
}

fn cover_requirements_match(context: &Value, require: &ProviderCoverRequireSpec) -> bool {
    if !require
        .all_of
        .iter()
        .all(|path| path_is_present(context, path))
    {
        return false;
    }
    if !require.any_of.is_empty()
        && !require
            .any_of
            .iter()
            .any(|path| path_is_present(context, path))
    {
        return false;
    }
    true
}

fn extract_cover_handler_url(obj: &serde_json::Map<String, Value>) -> Option<String> {
    for key in ["url", "cover_url", "cover_image_url", "cover"] {
        if let Some(value) = obj.get(key).and_then(Value::as_str) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }

    None
}

fn parse_cover_image_candidate(value: &Value) -> Option<CoverImageCandidate> {
    match value {
        Value::String(url) => {
            let url = url.trim();
            if url.is_empty() {
                return None;
            }
            Some(CoverImageCandidate {
                url: url.to_string(),
                width: None,
                height: None,
            })
        }
        Value::Object(obj) => {
            let url = extract_cover_handler_url(obj)?;
            let width = obj.get("width").and_then(as_u32_value);
            let height = obj.get("height").and_then(as_u32_value);
            Some(CoverImageCandidate { url, width, height })
        }
        _ => None,
    }
}

fn parse_cover_search_result(value: &Value) -> Option<super::ProviderCoverSearchResult> {
    let Value::Object(obj) = value else {
        return parse_cover_image_candidate(value).map(|candidate| {
            super::ProviderCoverSearchResult {
                provider_id: String::new(),
                candidates: vec![candidate],
                selected_index: Some(1),
            }
        });
    };

    if let Some(candidates_value) = obj.get("candidates") {
        let candidates_array = candidates_value.as_array()?;
        let candidates: Vec<CoverImageCandidate> = candidates_array
            .iter()
            .filter_map(parse_cover_image_candidate)
            .collect();
        if candidates.is_empty() {
            return None;
        }

        let selected_index = obj
            .get("selected_index")
            .and_then(as_u32_value)
            .filter(|index| {
                index
                    .checked_sub(1)
                    .and_then(|index| usize::try_from(index).ok())
                    .is_some_and(|idx| idx < candidates.len())
            });

        return Some(super::ProviderCoverSearchResult {
            provider_id: String::new(),
            candidates,
            selected_index,
        });
    }

    parse_cover_image_candidate(value).map(|candidate| super::ProviderCoverSearchResult {
        provider_id: String::new(),
        candidates: vec![candidate],
        selected_index: Some(1),
    })
}

fn push_length_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    buffer.extend_from_slice(bytes);
}

fn encode_canonical_json_for_hash(buffer: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buffer.push(0),
        Value::Bool(flag) => {
            buffer.push(1);
            buffer.push(u8::from(*flag));
        }
        Value::Number(number) => {
            buffer.push(2);
            push_length_prefixed_bytes(buffer, number.to_string().as_bytes());
        }
        Value::String(string) => {
            buffer.push(3);
            push_length_prefixed_bytes(buffer, string.as_bytes());
        }
        Value::Array(values) => {
            buffer.push(4);
            buffer.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for item in values {
                encode_canonical_json_for_hash(buffer, item);
            }
        }
        Value::Object(map) => {
            buffer.push(5);
            buffer.extend_from_slice(&(map.len() as u64).to_le_bytes());

            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (key, item) in entries {
                push_length_prefixed_bytes(buffer, key.as_bytes());
                encode_canonical_json_for_hash(buffer, item);
            }
        }
    }
}

fn provider_cover_context_hash(context: &Value) -> u64 {
    let mut encoded = Vec::new();
    encode_canonical_json_for_hash(&mut encoded, context);
    xxh3::hash64_with_seed(&encoded, 0)
}

fn provider_cover_cache_key(
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
) -> ProviderCoverSearchCacheKey {
    ProviderCoverSearchCacheKey {
        provider_id: provider_id.to_string(),
        entity_type,
        context_hash: provider_cover_context_hash(context),
    }
}

fn provider_cover_cache_ttl(result: &Option<super::ProviderCoverSearchResult>) -> Duration {
    if result.is_some() {
        PROVIDER_COVER_CACHE_HIT_TTL
    } else {
        PROVIDER_COVER_CACHE_MISS_TTL
    }
}

pub(crate) fn release_context_value(
    release: &Release,
    tracks: &[Track],
    artists: &[Artist],
    external_ids: &HashMap<String, String>,
    library: Option<&Library>,
) -> Result<Value> {
    let mut context = serde_json::to_value(release)?;
    let Value::Object(object) = &mut context else {
        return Err(anyhow!("release context must be an object"));
    };
    if let Some(library) = library {
        object.insert("library".to_string(), library_context_value(library));
    }
    object.insert("tracks".to_string(), serde_json::to_value(tracks)?);
    object.insert("artists".to_string(), serde_json::to_value(artists)?);
    object.insert(
        "artist_names".to_string(),
        serde_json::to_value(
            artists
                .iter()
                .map(|artist| artist.artist_name.trim())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>(),
        )?,
    );
    object.insert("ids".to_string(), serde_json::to_value(external_ids)?);
    Ok(context)
}

pub(crate) fn artist_context_value(
    artist: &Artist,
    external_ids: &HashMap<String, String>,
) -> Result<Value> {
    let mut context = serde_json::to_value(artist)?;
    let Value::Object(object) = &mut context else {
        return Err(anyhow!("artist context must be an object"));
    };
    object.insert("ids".to_string(), serde_json::to_value(external_ids)?);
    Ok(context)
}

async fn resolve_provider_cover_search_result_uncached(
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
    force_refresh: bool,
) -> Result<Option<super::ProviderCoverSearchResult>> {
    let cover_specs = {
        let registry = PROVIDER_REGISTRY.read().await;
        registry.get_cover_handlers(provider_id, entity_type)
    };

    for cover_spec in cover_specs {
        if !cover_requirements_match(context, &cover_spec.require) {
            continue;
        }

        let Some(lua) = cover_spec.handler.try_upgrade_lua() else {
            tracing::warn!(
                provider_id,
                "cover handler's lua instance is no longer valid, skipping"
            );
            continue;
        };
        let handler_context = cover_handler_context(context, force_refresh);
        let lua_ctx = lua
            .to_value_with(&handler_context, crate::plugins::LUA_SERIALIZE_OPTIONS)
            .map_err(anyhow::Error::from)?;
        let timeout = if cover_spec.timeout.is_zero() {
            DEFAULT_COVER_HANDLER_TIMEOUT
        } else {
            cover_spec.timeout
        };
        let call = cover_spec.handler.call_async::<_, LuaValue>(lua_ctx);
        let result: LuaValue = match tokio::time::timeout(timeout, call).await {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => return Err(err.into()),
            Err(_elapsed) => {
                return Err(anyhow!(
                    "cover handler for provider '{provider_id}' timed out after {}ms",
                    timeout.as_millis()
                ));
            }
        };
        let value = serde_json::to_value(&result)?;
        if value.is_null() {
            continue;
        }
        let Some(search_result) = parse_cover_search_result(&value) else {
            continue;
        };
        return Ok(Some(search_result));
    }

    Ok(None)
}

async fn resolve_provider_cover_search_result(
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
    force_refresh: bool,
) -> Result<Option<super::ProviderCoverSearchResult>> {
    {
        let registry = PROVIDER_REGISTRY.read().await;
        if registry
            .get_cover_handlers(provider_id, entity_type)
            .is_empty()
        {
            return Ok(None);
        }
    }

    let cache_key = provider_cover_cache_key(provider_id, entity_type, context);

    loop {
        let pending = {
            let now = Instant::now();
            let mut cache = PROVIDER_COVER_SEARCH_CACHE.lock().await;
            cache.retain(|_, entry| match entry {
                ProviderCoverSearchCacheEntry::Ready(cached) => cached.expires_at > now,
                ProviderCoverSearchCacheEntry::Pending(_) => true,
            });

            match cache.get_mut(&cache_key) {
                Some(ProviderCoverSearchCacheEntry::Ready(cached)) if !force_refresh => {
                    return Ok(cached.result.clone());
                }
                Some(ProviderCoverSearchCacheEntry::Ready(_)) => {
                    cache.insert(
                        cache_key.clone(),
                        ProviderCoverSearchCacheEntry::Pending(Vec::new()),
                    );
                    None
                }
                Some(ProviderCoverSearchCacheEntry::Pending(waiters)) => {
                    let (sender, receiver) = oneshot::channel();
                    waiters.push(sender);
                    Some(receiver)
                }
                None => {
                    cache.insert(
                        cache_key.clone(),
                        ProviderCoverSearchCacheEntry::Pending(Vec::new()),
                    );
                    None
                }
            }
        };

        if let Some(receiver) = pending {
            let _ = receiver.await;
            continue;
        }

        break;
    }

    let result = resolve_provider_cover_search_result_uncached(
        provider_id,
        entity_type,
        context,
        force_refresh,
    )
    .await;
    let waiters = {
        let mut cache = PROVIDER_COVER_SEARCH_CACHE.lock().await;
        match &result {
            Ok(search_result) => {
                let entry = ProviderCoverSearchCacheEntry::Ready(CachedProviderCoverSearchResult {
                    result: search_result.clone(),
                    expires_at: Instant::now() + provider_cover_cache_ttl(search_result),
                });
                match cache.insert(cache_key, entry) {
                    Some(ProviderCoverSearchCacheEntry::Pending(waiters)) => waiters,
                    _ => Vec::new(),
                }
            }
            Err(_) => match cache.remove(&cache_key) {
                Some(ProviderCoverSearchCacheEntry::Pending(waiters)) => waiters,
                _ => Vec::new(),
            },
        }
    };

    for waiter in waiters {
        let _ = waiter.send(());
    }

    result
}

pub(crate) async fn resolve_provider_cover_url(
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
    force_refresh: bool,
) -> Result<Option<String>> {
    let Some(search_result) =
        resolve_provider_cover_search_result(provider_id, entity_type, context, force_refresh)
            .await?
    else {
        return Ok(None);
    };

    let selected = search_result
        .selected_candidate()
        .or_else(|| search_result.candidates.first());
    Ok(selected.map(|candidate| candidate.url.clone()))
}

pub(crate) async fn resolve_provider_release_cover_url(
    provider_id: &str,
    context: &Value,
    force_refresh: bool,
) -> Result<Option<String>> {
    resolve_provider_cover_url(provider_id, EntityType::Release, context, force_refresh).await
}

pub(crate) async fn resolve_provider_artist_cover_url(
    provider_id: &str,
    context: &Value,
    force_refresh: bool,
) -> Result<Option<String>> {
    resolve_provider_cover_url(provider_id, EntityType::Artist, context, force_refresh).await
}

async fn search_cover_candidates_for_contexts(
    entity_type: EntityType,
    entity_id: DbId,
    provider_contexts: Vec<(String, Value)>,
    force_refresh: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let mut results = Vec::new();
    for (provider_id, context) in provider_contexts {
        match resolve_provider_cover_search_result(
            &provider_id,
            entity_type,
            &context,
            force_refresh,
        )
        .await
        {
            Ok(Some(mut search_result)) => {
                search_result.provider_id = provider_id;
                results.push(search_result);
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    provider = %provider_id,
                    entity_type = entity_type.as_str(),
                    entity_id = entity_id.0,
                    error = %err,
                    "provider cover candidate search failed"
                );
            }
        }
    }

    Ok(results)
}

pub(crate) async fn search_release_cover_candidates(
    release_id: DbId,
    provider_filter: Option<&str>,
    force_refresh: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let provider_contexts = {
        let db = STATE.db.read().await;
        let release = db::releases::get_by_id(&db, release_id)?
            .ok_or_else(|| anyhow!("release not found: {}", release_id.0))?;
        let tracks = db::tracks::get_direct(&db, release_id)?;
        let artists = db::artists::get(&db, release_id)?;
        let library = library_for_release(&db, release_id)?;

        let mut providers = db::providers::get(&db)?;
        providers.retain(|provider| provider.enabled);
        if let Some(provider_filter) = provider_filter
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            providers.retain(|provider| provider.provider_id == provider_filter);
        }
        providers.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.provider_id.cmp(&b.provider_id))
        });

        let mut provider_contexts = Vec::new();
        for provider in providers {
            let provider_id = provider.provider_id;
            let provider_ids = provider_external_ids_for_entity(&db, release_id, &provider_id)?;
            let context = release_context_value(
                &release,
                &tracks,
                &artists,
                &provider_ids,
                library.as_ref(),
            )?;
            provider_contexts.push((provider_id, context));
        }

        provider_contexts
    };

    search_cover_candidates_for_contexts(
        EntityType::Release,
        release_id,
        provider_contexts,
        force_refresh,
    )
    .await
}

pub(crate) async fn search_artist_cover_candidates(
    artist_id: DbId,
    provider_filter: Option<&str>,
    force_refresh: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let provider_contexts = {
        let db = STATE.db.read().await;
        let artist = db::artists::get_by_id(&db, artist_id)?
            .ok_or_else(|| anyhow!("artist not found: {}", artist_id.0))?;

        let mut providers = db::providers::get(&db)?;
        providers.retain(|provider| provider.enabled);
        if let Some(provider_filter) = provider_filter
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            providers.retain(|provider| provider.provider_id == provider_filter);
        }
        providers.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.provider_id.cmp(&b.provider_id))
        });

        let mut provider_contexts = Vec::new();
        for provider in providers {
            let provider_id = provider.provider_id;
            let provider_ids = provider_external_ids_for_entity(&db, artist_id, &provider_id)?;
            let context = artist_context_value(&artist, &provider_ids)?;
            provider_contexts.push((provider_id, context));
        }

        provider_contexts
    };

    search_cover_candidates_for_contexts(
        EntityType::Artist,
        artist_id,
        provider_contexts,
        force_refresh,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanoid::nanoid;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn cover_requirements_match_handles_all_of_and_any_of() {
        let context = json!({
            "ids": {
                "release_id": "release-1"
            },
            "artist_names": ["Artist A"]
        });
        let require = ProviderCoverRequireSpec {
            all_of: vec!["ids.release_id".to_string()],
            any_of: vec![
                "ids.release_group_id".to_string(),
                "artist_names".to_string(),
            ],
        };

        assert!(cover_requirements_match(&context, &require));
    }

    #[test]
    fn cover_requirements_match_fails_when_required_paths_missing() {
        let context = json!({
            "ids": {},
            "artist_names": []
        });
        let require = ProviderCoverRequireSpec {
            all_of: vec!["ids.release_id".to_string()],
            any_of: vec!["artist_names".to_string()],
        };

        assert!(!cover_requirements_match(&context, &require));
    }

    #[test]
    fn release_context_value_includes_library_and_provider_ids() -> anyhow::Result<()> {
        let release = Release {
            db_id: Some(DbId(10).into()),
            id: nanoid!(),
            release_title: "Release".to_string(),
            sort_title: None,
            release_type: None,
            release_date: Some("2024-02-29".to_string()),
            locked: None,
            created_at: None,
            ctime: None,
        };
        let track = Track {
            db_id: Some(DbId(20).into()),
            id: nanoid!(),
            track_title: "Track".to_string(),
            sort_title: None,
            year: None,
            disc: Some(1),
            disc_total: Some(1),
            track: Some(1),
            track_total: Some(1),
            duration_ms: Some(123_000),
            sample_rate_hz: None,
            channel_count: None,
            bit_depth: None,
            bitrate_bps: None,
            locked: None,
            created_at: None,
            ctime: None,
        };
        let artist = Artist {
            db_id: Some(DbId(30).into()),
            id: nanoid!(),
            artist_name: "Artist".to_string(),
            scan_name: "artist".to_string(),
            sort_name: None,
            artist_type: None,
            description: None,
            verified: false,
            locked: None,
            created_at: None,
        };
        let external_ids = HashMap::from([
            ("release_id".to_string(), "release-123".to_string()),
            ("release_group_id".to_string(), "group-123".to_string()),
        ]);
        let library = Library {
            db_id: Some(DbId(99)),
            id: nanoid!(),
            name: "Library".to_string(),
            name_key: "library".to_string(),
            path: PathBuf::from("/music"),
            path_key: "/music".to_string(),
            language: Some("jpn".to_string()),
            country: Some("JP".to_string()),
        };

        let context =
            release_context_value(&release, &[track], &[artist], &external_ids, Some(&library))?;

        assert_eq!(context["library"]["db_id"], json!(99));
        assert_eq!(context["library"]["language"], json!("jpn"));
        assert_eq!(context["ids"]["release_id"], json!("release-123"));
        assert_eq!(context["ids"]["release_group_id"], json!("group-123"));

        Ok(())
    }

    #[test]
    fn artist_context_value_includes_provider_ids() -> anyhow::Result<()> {
        let artist = Artist {
            db_id: Some(DbId(30).into()),
            id: nanoid!(),
            artist_name: "Artist".to_string(),
            scan_name: "artist".to_string(),
            sort_name: Some("Artist, The".to_string()),
            artist_type: None,
            description: Some("desc".to_string()),
            verified: true,
            locked: None,
            created_at: None,
        };
        let external_ids = HashMap::from([("artist_id".to_string(), "artist-123".to_string())]);

        let context = artist_context_value(&artist, &external_ids)?;

        assert_eq!(context["artist_name"], json!("Artist"));
        assert_eq!(context["sort_name"], json!("Artist, The"));
        assert_eq!(context["ids"]["artist_id"], json!("artist-123"));

        Ok(())
    }

    #[test]
    fn provider_cover_context_hash_is_stable_across_object_key_order() {
        let a = json!({
            "ids": {
                "release_id": "release-123",
                "release_group_id": "group-123"
            },
            "library": {
                "country": "JP",
                "language": "jpn"
            },
            "tracks": [
                { "track_title": "A", "disc": 1, "track": 1 }
            ]
        });
        let b = json!({
            "tracks": [
                { "track": 1, "disc": 1, "track_title": "A" }
            ],
            "library": {
                "language": "jpn",
                "country": "JP"
            },
            "ids": {
                "release_group_id": "group-123",
                "release_id": "release-123"
            }
        });

        assert_eq!(
            provider_cover_context_hash(&a),
            provider_cover_context_hash(&b)
        );
    }

    #[test]
    fn provider_cover_cache_key_separates_providers() {
        let context = json!({
            "ids": {
                "release_id": "release-123"
            }
        });

        let a = provider_cover_cache_key("musicbrainz", EntityType::Release, &context);
        let b = provider_cover_cache_key("other-provider", EntityType::Release, &context);

        assert_ne!(a, b);
        assert_eq!(a.context_hash, b.context_hash);
    }

    #[test]
    fn provider_cover_cache_key_separates_entity_types() {
        let context = json!({
            "ids": {
                "shared_id": "123"
            }
        });

        let release = provider_cover_cache_key("musicbrainz", EntityType::Release, &context);
        let artist = provider_cover_cache_key("musicbrainz", EntityType::Artist, &context);

        assert_ne!(release, artist);
        assert_eq!(release.context_hash, artist.context_hash);
    }

    #[test]
    fn cover_handler_context_adds_force_refresh_option_only_when_requested() {
        let context = json!({
            "ids": {
                "release_id": "release-123"
            }
        });

        let unchanged = cover_handler_context(&context, false);
        assert_eq!(unchanged, context);

        let refreshed = cover_handler_context(&context, true);
        assert_eq!(refreshed["ids"]["release_id"], json!("release-123"));
        assert_eq!(refreshed["cover_options"]["force_refresh"], json!(true));
    }

    #[test]
    fn parse_cover_search_result_reads_candidates_and_selected_index() {
        let value = json!({
            "candidates": [
                { "url": "https://example.com/a.jpg", "width": 1200, "height": 1200 },
                { "url": "https://example.com/b.jpg", "width": 600, "height": 600 }
            ],
            "selected_index": 2
        });

        let parsed =
            parse_cover_search_result(&value).expect("expected candidates payload to parse");
        assert_eq!(parsed.candidates.len(), 2);
        assert_eq!(parsed.selected_index, Some(2));
        assert_eq!(
            parsed
                .selected_candidate()
                .map(|candidate| candidate.url.as_str()),
            Some("https://example.com/b.jpg")
        );
    }

    #[test]
    fn parse_cover_search_result_accepts_single_candidate_object() {
        let value = json!({
            "url": "https://example.com/front.png",
            "width": 800,
            "height": 800
        });

        let parsed =
            parse_cover_search_result(&value).expect("expected single candidate object to parse");
        assert_eq!(parsed.selected_index, Some(1));
        assert_eq!(parsed.candidates.len(), 1);
        assert_eq!(parsed.candidates[0].width, Some(800));
        assert_eq!(parsed.candidates[0].height, Some(800));
    }

    #[test]
    fn provider_cover_search_result_ignores_zero_selected_index() {
        let search_result = super::super::ProviderCoverSearchResult {
            provider_id: "provider".to_string(),
            candidates: vec![CoverImageCandidate {
                url: "https://example.com/front.jpg".to_string(),
                width: None,
                height: None,
            }],
            selected_index: Some(0),
        };

        assert!(search_result.selected_candidate().is_none());
    }

    #[test]
    fn parse_cover_search_result_rejects_zero_selected_index() {
        let value = json!({
            "candidates": [
                { "url": "https://example.com/front.jpg", "width": 1200, "height": 1200 }
            ],
            "selected_index": 0
        });

        let parsed =
            parse_cover_search_result(&value).expect("expected candidates payload to parse");
        assert_eq!(parsed.selected_index, None);
        assert!(parsed.selected_candidate().is_none());
    }
}
