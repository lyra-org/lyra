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
    Context,
    Result,
    anyhow,
};
use serde_json::Value;
use std::sync::LazyLock;

use crate::{
    services::plugin_dispatch::{
        CallSource,
        MetadataRefreshRequest,
        Published,
        RuntimeCalls,
        TransitionWait,
    },
    services::providers::{
        ExternalIdsByScheme,
        IdSchemes,
        ProviderCallError,
        ProviderCallStage,
        ProviderLookup,
        active_providers,
        id_schemes,
        requirements_match,
        with_provider_call,
        with_provider_call_on,
    },
};
use lyra_db::{
    Artist,
    Library,
    Release,
    Track,
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
    pub(crate) bypass_cache: bool,
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

static PROVIDER_COVER_SEARCH_CACHE: LazyLock<
    tokio::sync::Mutex<HashMap<ProviderCoverSearchCacheKey, CachedProviderCoverSearchResult>>,
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
                    .and_then(lyra_db::releases::normalize_release_date),
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
    let provider_id = request.provider_id.to_string();
    let entity_type = request.entity_type;
    let include_cover_urls = request.include_cover_urls;
    let bypass_cache = request.bypass_cache;
    let query = request.query.to_string();
    let provider = provider_id.as_str();

    let result = with_provider_call(
        provider,
        ProviderCallStage::Search,
        TransitionWait::interactive(),
        |registry| {
            registry
                .get_search_callback(provider, entity_type)
                .copied()
                .map(ProviderLookup::Call)
        },
        |handler, calls| {
            let query = query.clone();
            async move {
                calls
                    .metadata_refresh(MetadataRefreshRequest {
                        handler,
                        context: serde_json::Value::String(query),
                        deadline: std::time::Instant::now() + Duration::from_secs(300),
                    })
                    .await
                    .with_context(|| format!("provider search handler failed for '{provider}'"))
            }
        },
    )
    .await
    .map_err(|err| match err {
        ProviderCallError::NotRegistered => ProviderSearchError::NoSearchHandler {
            provider_id: provider_id.clone(),
            entity_type: entity_type.as_str(),
        },
        ProviderCallError::Failed(err) | ProviderCallError::TargetGone(err) => {
            ProviderSearchError::Internal(err)
        }
    })?;

    let values = flatten_search_values(result.values);
    let schemes = if include_cover_urls {
        Some(id_schemes().await)
    } else {
        None
    };
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let resolved_cover_url = if let Some(schemes) = &schemes {
            let external_ids = search_hit_external_ids(schemes, &provider_id, &value);
            if external_ids.is_empty() {
                None
            } else {
                match resolve_provider_cover_url(
                    &Published::interactive(),
                    &provider_id,
                    entity_type,
                    &serde_json::json!({ "external_ids": external_ids }),
                    bypass_cache,
                )
                .await
                {
                    Ok(url) => url,
                    Err(err) => {
                        tracing::warn!(
                            provider = provider_id,
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
            entity_type,
            value,
            include_cover_urls,
            resolved_cover_url,
        ));
    }
    Ok(normalized)
}

/// A search hit's ids as its provider's stored ids would resolve: keyed by the schemes it
/// registered for their id types, by the same rules as an entity's.
fn search_hit_external_ids(
    schemes: &IdSchemes,
    provider_id: &str,
    hit: &Value,
) -> ExternalIdsByScheme {
    let rows = as_ids(hit)
        .unwrap_or_default()
        .into_iter()
        .map(|(id_type, id_value)| lyra_db::external_ids::ExternalId {
            db_id: None,
            provider_id: provider_id.to_string(),
            id_type,
            id_value,
            source: lyra_db::IdSource::Resolved,
            resolved_value: None,
        })
        .collect::<Vec<_>>();
    schemes.resolve(&rows)
}

fn flatten_search_values(values: Vec<Value>) -> Vec<Value> {
    if values.len() == 1
        && let Some(array) = values.first().and_then(Value::as_array)
    {
        return array.clone();
    }
    values
        .into_iter()
        .filter(|value| !value.is_null())
        .collect()
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

fn cover_handler_context(context: &Value, bypass_cache: bool) -> Value {
    if !bypass_cache {
        return context.clone();
    }

    let mut context = context.clone();
    if let Value::Object(object) = &mut context {
        object.insert(
            "cover_options".to_string(),
            serde_json::json!({
                "bypass_cache": true,
            }),
        );
    }
    context
}

/// Every provider's ids on the entity, keyed by scheme, as cover handlers see them.
pub(crate) fn cover_external_ids(
    db: &DbAny,
    entity_id: DbId,
    schemes: &IdSchemes,
) -> Result<ExternalIdsByScheme> {
    Ok(schemes.resolve(&lyra_db::external_ids::get_for_entity(db, entity_id)?))
}

pub(crate) fn library_for_release(db: &DbAny, release_id: DbId) -> Result<Option<Library>> {
    Ok(lyra_db::libraries::get_by_release(db, release_id)?
        .into_iter()
        .next())
}

fn library_context_value(library: &Library) -> Value {
    serde_json::json!({
        "id": library.id,
        "name": library.name,
        "directory": library.path.to_string_lossy().to_string(),
        "language": library.language,
        "country": library.country,
    })
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
    external_ids: &ExternalIdsByScheme,
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
    object.insert(
        "external_ids".to_string(),
        serde_json::to_value(external_ids)?,
    );
    Ok(context)
}

pub(crate) fn artist_context_value(
    artist: &Artist,
    external_ids: &ExternalIdsByScheme,
) -> Result<Value> {
    let mut context = serde_json::to_value(artist)?;
    let Value::Object(object) = &mut context else {
        return Err(anyhow!("artist context must be an object"));
    };
    object.insert(
        "external_ids".to_string(),
        serde_json::to_value(external_ids)?,
    );
    Ok(context)
}

async fn resolve_provider_cover_url<S: CallSource>(
    source: &S,
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
    bypass_cache: bool,
) -> Result<Option<String>> {
    Ok(
        search_provider_cover(source, provider_id, entity_type, context, bypass_cache)
            .await?
            .and_then(|result| {
                result
                    .selected_candidate()
                    .map(|candidate| candidate.url.clone())
            }),
    )
}

pub(crate) async fn resolve_provider_release_cover_url<S: CallSource>(
    source: &S,
    provider_id: &str,
    context: &Value,
    bypass_cache: bool,
) -> Result<Option<String>> {
    resolve_provider_cover_url(
        source,
        provider_id,
        EntityType::Release,
        context,
        bypass_cache,
    )
    .await
}

pub(crate) async fn resolve_provider_artist_cover_url<S: CallSource>(
    source: &S,
    provider_id: &str,
    context: &Value,
    bypass_cache: bool,
) -> Result<Option<String>> {
    resolve_provider_cover_url(
        source,
        provider_id,
        EntityType::Artist,
        context,
        bypass_cache,
    )
    .await
}

async fn search_cover_candidates_for_contexts(
    entity_type: EntityType,
    entity_id: DbId,
    provider_contexts: Vec<(String, Value)>,
    bypass_cache: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let mut results = Vec::new();
    for (provider_id, context) in provider_contexts {
        match search_provider_cover(
            &Published::interactive(),
            &provider_id,
            entity_type,
            &context,
            bypass_cache,
        )
        .await
        {
            Ok(Some(result)) => results.push(result),
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    provider_id,
                    entity_type = entity_type.as_str(),
                    entity_id = entity_id.0,
                    error = %err,
                    "provider cover search failed"
                );
            }
        }
    }
    Ok(results)
}

async fn search_provider_cover<S: CallSource>(
    source: &S,
    provider_id: &str,
    entity_type: EntityType,
    context: &Value,
    bypass_cache: bool,
) -> Result<Option<super::ProviderCoverSearchResult>> {
    let cache_key = provider_cover_cache_key(provider_id, entity_type, context);
    if !bypass_cache && let Some(cached) = provider_cover_cache_get(&cache_key).await {
        return Ok(cached);
    }

    let handler_context = cover_handler_context(context, bypass_cache);
    let dispatch = match with_provider_call_on(
        source,
        provider_id,
        ProviderCallStage::CoverSearch,
        |registry| {
            let spec = registry.get_cover_handler(provider_id, entity_type)?;
            Some(if requirements_match(context, &spec.require) {
                ProviderLookup::Call(spec.clone())
            } else {
                ProviderLookup::Answer(None)
            })
        },
        |spec, calls| {
            let handler_context = handler_context.clone();
            async move {
                let timeout = spec.timeout;
                tokio::time::timeout(
                    timeout,
                    calls.metadata_refresh(MetadataRefreshRequest {
                        handler: spec.handler,
                        context: handler_context,
                        deadline: std::time::Instant::now() + timeout,
                    }),
                )
                .await
                .with_context(|| {
                    format!(
                        "provider cover handler for '{provider_id}' timed out after {}ms",
                        timeout.as_millis()
                    )
                })?
                .map(Some)
            }
        },
    )
    .await
    {
        Ok(Some(dispatch)) => dispatch,
        Ok(None) => {
            provider_cover_cache_put(cache_key, None).await;
            return Ok(None);
        }
        Err(ProviderCallError::NotRegistered) => return Ok(None),
        Err(ProviderCallError::Failed(err) | ProviderCallError::TargetGone(err)) => {
            return Err(err);
        }
    };

    tracing::trace!(
        provider_id,
        entity_type = entity_type.as_str(),
        "provider cover handler returned"
    );

    let mut result = dispatch
        .values
        .first()
        .and_then(parse_cover_search_result)
        .map(|mut result| {
            result.provider_id = provider_id.to_string();
            result
        });
    if result
        .as_ref()
        .is_some_and(|result| result.candidates.is_empty())
    {
        result = None;
    }
    provider_cover_cache_put(cache_key, result.clone()).await;
    Ok(result)
}

async fn provider_cover_cache_get(
    key: &ProviderCoverSearchCacheKey,
) -> Option<Option<super::ProviderCoverSearchResult>> {
    let mut cache = PROVIDER_COVER_SEARCH_CACHE.lock().await;
    let now = Instant::now();
    match cache.get(key) {
        Some(entry) if entry.expires_at > now => Some(entry.result.clone()),
        Some(_) => {
            cache.remove(key);
            None
        }
        None => None,
    }
}

async fn provider_cover_cache_put(
    key: ProviderCoverSearchCacheKey,
    result: Option<super::ProviderCoverSearchResult>,
) {
    let expires_at = Instant::now() + provider_cover_cache_ttl(&result);
    PROVIDER_COVER_SEARCH_CACHE
        .lock()
        .await
        .insert(key, CachedProviderCoverSearchResult { result, expires_at });
}

pub(crate) async fn search_release_cover_candidates(
    db: &lyra_db::DbAsync,
    release_public_id: &str,
    provider_filter: Option<&str>,
    bypass_cache: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let schemes = id_schemes().await;
    let (release_id, provider_contexts) = {
        let db = db.read().await;
        let not_found = || anyhow!("release not found: {release_public_id}");
        let release_id =
            lyra_db::lookup::find_node_id_by_id(&*db, release_public_id)?.ok_or_else(not_found)?;
        let release = lyra_db::releases::get_by_id(&db, release_id)?.ok_or_else(not_found)?;
        let tracks = lyra_db::tracks::get_direct(&db, release_id)?;
        let artists = lyra_db::artists::get(&db, release_id)?;
        let library = library_for_release(&db, release_id)?;
        let external_ids = cover_external_ids(&db, release_id, &schemes)?;
        let context =
            release_context_value(&release, &tracks, &artists, &external_ids, library.as_ref())?;

        let provider_contexts = active_providers(&db)?
            .only(provider_filter)
            .into_iter()
            .map(|provider| (provider.provider_id, context.clone()))
            .collect();

        (release_id, provider_contexts)
    };

    search_cover_candidates_for_contexts(
        EntityType::Release,
        release_id,
        provider_contexts,
        bypass_cache,
    )
    .await
}

pub(crate) async fn search_artist_cover_candidates(
    db: &lyra_db::DbAsync,
    artist_public_id: &str,
    provider_filter: Option<&str>,
    bypass_cache: bool,
) -> Result<Vec<super::ProviderCoverSearchResult>> {
    let schemes = id_schemes().await;
    let (artist_id, provider_contexts) = {
        let db = db.read().await;
        let not_found = || anyhow!("artist not found: {artist_public_id}");
        let artist_id =
            lyra_db::lookup::find_node_id_by_id(&*db, artist_public_id)?.ok_or_else(not_found)?;
        let artist = lyra_db::artists::get_by_id(&db, artist_id)?.ok_or_else(not_found)?;
        let external_ids = cover_external_ids(&db, artist_id, &schemes)?;
        let context = artist_context_value(&artist, &external_ids)?;

        let provider_contexts = active_providers(&db)?
            .only(provider_filter)
            .into_iter()
            .map(|provider| (provider.provider_id, context.clone()))
            .collect();

        (artist_id, provider_contexts)
    };

    search_cover_candidates_for_contexts(
        EntityType::Artist,
        artist_id,
        provider_contexts,
        bypass_cache,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_db::IdSource;
    use lyra_db::fixtures::{
        insert_artist,
        insert_release,
    };
    use lyra_db::nanoid;
    use lyra_db::test_db::new_test_db;
    use serde_json::json;
    use std::path::PathBuf;

    fn upsert_id(
        db: &mut DbAny,
        entity_id: DbId,
        provider_id: &str,
        id_type: &str,
        value: &str,
        source: IdSource,
    ) -> anyhow::Result<()> {
        lyra_db::external_ids::upsert(db, entity_id, provider_id, id_type, value, source)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_search_for_an_unknown_provider_adds_no_call_lock() -> anyhow::Result<()> {
        let _guard = crate::services::testing::runtime_test_lock().await;
        crate::services::testing::init_default_test_state()?;

        let result = search_provider(ProviderSearchRequest {
            provider_id: "made-up",
            entity_type: EntityType::Release,
            query: "anything",
            include_cover_urls: false,
            bypass_cache: false,
        })
        .await;

        assert!(matches!(
            result,
            Err(ProviderSearchError::NoSearchHandler { .. })
        ));
        assert_eq!(
            crate::services::providers::registry_tests::call_lock_count().await,
            0
        );
        Ok(())
    }

    #[test]
    fn release_context_value_includes_library_and_ids() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let release_id = insert_release(&mut db, "Release")?;
        upsert_id(
            &mut db,
            release_id,
            "musicbrainz",
            "release_id",
            "release-123",
            IdSource::Resolved,
        )?;
        upsert_id(
            &mut db,
            release_id,
            "theaudiodb",
            "album_id",
            "album-123",
            IdSource::Manual,
        )?;
        let mut schemes = IdSchemes::default();
        schemes.insert("musicbrainz", "release_id", "musicbrainz:release");
        schemes.insert("theaudiodb", "album_id", "theaudiodb:album");
        let release = lyra_db::releases::get_by_id(&db, release_id)?.expect("release");
        let library = Library {
            db_id: Some(DbId(99)),
            id: nanoid(),
            name: "Library".to_string(),
            name_key: "library".to_string(),
            path: PathBuf::from("/music"),
            path_key: "/music".to_string(),
            language: Some("jpn".to_string()),
            country: Some("JP".to_string()),
            purge_missing_after_days: None,
            follow_links: None,
        };
        let external_ids = cover_external_ids(&db, release_id, &schemes)?;

        let context = release_context_value(&release, &[], &[], &external_ids, Some(&library))?;

        assert_eq!(context["library"]["id"], json!(library.id));
        assert_eq!(context["library"]["language"], json!("jpn"));
        assert_eq!(
            context["external_ids"],
            json!({ "musicbrainz:release": "release-123", "theaudiodb:album": "album-123" })
        );
        for removed in ["ids", "identifiers", "manual_ids", "user_set"] {
            assert!(context.get(removed).is_none(), "{removed}");
        }

        Ok(())
    }

    #[test]
    fn artist_context_value_keys_every_providers_ids_by_scheme() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let artist_id = insert_artist(&mut db, "Artist")?;
        upsert_id(
            &mut db,
            artist_id,
            "musicbrainz",
            "artist_id",
            "mbid-1",
            IdSource::Resolved,
        )?;
        upsert_id(
            &mut db,
            artist_id,
            "theaudiodb",
            "artist_id",
            "123",
            IdSource::Manual,
        )?;
        let mut schemes = IdSchemes::default();
        schemes.insert("musicbrainz", "artist_id", "musicbrainz:artist");
        schemes.insert("theaudiodb", "artist_id", "theaudiodb:artist");
        let artist = lyra_db::artists::get_by_id(&db, artist_id)?.expect("artist");
        let external_ids = cover_external_ids(&db, artist_id, &schemes)?;

        let context = artist_context_value(&artist, &external_ids)?;

        assert_eq!(context["artist_name"], json!("Artist"));
        assert_eq!(
            context["external_ids"],
            json!({ "musicbrainz:artist": "mbid-1", "theaudiodb:artist": "123" })
        );

        Ok(())
    }

    #[test]
    fn a_search_hits_ids_resolve_as_stored_ids_do() {
        let mut schemes = IdSchemes::default();
        schemes.insert("theaudiodb", "artist_id", "theaudiodb:artist");
        schemes.insert("theaudiodb", "z_artist_id", "theaudiodb:artist");
        schemes.insert("theaudiodb", "album_id", "theaudiodb:album");
        let hit = json!({ "ids": {
            "z_artist_id": "999",
            "artist_id": "123",
            "album_id": " ",
            "unregistered_id": "x",
        } });

        assert_eq!(
            search_hit_external_ids(&schemes, "theaudiodb", &hit),
            ExternalIdsByScheme::from([("theaudiodb:artist".to_string(), "123".to_string())])
        );
        assert!(search_hit_external_ids(&schemes, "musicbrainz", &hit).is_empty());
    }

    #[test]
    fn provider_cover_context_hash_is_stable_across_object_key_order() {
        let a = json!({
            "external_ids": {
                "musicbrainz:release": "release-123",
                "musicbrainz:release-group": "group-123"
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
            "external_ids": {
                "musicbrainz:release-group": "group-123",
                "musicbrainz:release": "release-123"
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
            "external_ids": {
                "musicbrainz:release": "release-123"
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
            "external_ids": {
                "shared_id": "123"
            }
        });

        let release = provider_cover_cache_key("musicbrainz", EntityType::Release, &context);
        let artist = provider_cover_cache_key("musicbrainz", EntityType::Artist, &context);

        assert_ne!(release, artist);
        assert_eq!(release.context_hash, artist.context_hash);
    }

    #[test]
    fn cover_handler_context_adds_bypass_cache_option_only_when_requested() {
        let context = json!({
            "external_ids": {
                "musicbrainz:release": "release-123"
            }
        });

        let unchanged = cover_handler_context(&context, false);
        assert_eq!(unchanged, context);

        let refreshed = cover_handler_context(&context, true);
        assert_eq!(
            refreshed["external_ids"]["musicbrainz:release"],
            json!("release-123")
        );
        assert_eq!(refreshed["cover_options"]["bypass_cache"], json!(true));
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
