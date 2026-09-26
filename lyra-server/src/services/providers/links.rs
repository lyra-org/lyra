// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    future::Future,
    sync::{
        Arc,
        Mutex,
    },
    time::{
        Duration,
        Instant,
    },
};

use agdb::DbId;
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
use percent_encoding::{
    AsciiSet,
    NON_ALPHANUMERIC,
    utf8_percent_encode,
};
use serde::Serialize;

use super::registry::ProviderIdUrlGenerator;
use crate::db::{
    Library,
    external_ids::ExternalId,
};
use crate::services::EntityType;

/// CPU budget for each resume of one generator call.
pub(crate) const ID_LINK_CALL_BUDGET: Duration = Duration::from_millis(100);
/// Wall-clock budget for one provider's batch of generator calls.
pub(crate) const ID_LINK_BATCH_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a generator that timed out is skipped, unless its plugin reloads first.
const ID_LINK_SUSPENSION: Duration = Duration::from_secs(10 * 60);
const ID_LINK_CACHE_CAPACITY: usize = 16_384;
const MAX_ID_LINK_URL_BYTES: usize = 2048;

/// Everything but RFC 3986 unreserved characters, so an id stays one path
/// segment or query value.
const ID_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Checks that `template` is an absolute http(s) URL with exactly one `{id}`
/// placeholder, placed after the host.
pub(crate) fn validate_id_link_template(template: &str) -> Result<(), String> {
    let Some((prefix, rest)) = template.split_once("{id}") else {
        return Err("URL template must contain an {id} placeholder".to_string());
    };
    if rest.contains("{id}") {
        return Err("URL template must contain exactly one {id} placeholder".to_string());
    }
    let after_host = prefix
        .split_once("://")
        .is_some_and(|(_, authority)| authority.contains(['/', '?', '#']));
    if !after_host || parse_link_url(&template.replace("{id}", "id")).is_none() {
        return Err(
            "URL template must be an absolute http(s) URL with {id} after the host".to_string(),
        );
    }
    Ok(())
}

/// `None` for `.` and `..`, which stay dot segments even when encoded.
fn render_template(template: &str, id: &str) -> Option<String> {
    if matches!(id, "." | "..") {
        return None;
    }
    Some(template.replace("{id}", &utf8_percent_encode(id, ID_ENCODE_SET).to_string()))
}

fn parse_link_url(url: &str) -> Option<url::Url> {
    if url.len() > MAX_ID_LINK_URL_BYTES {
        return None;
    }
    url::Url::parse(url)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
}

#[derive(Clone, Debug)]
pub(crate) struct IdLinkGenerator {
    pub(crate) scheme: Option<String>,
    pub(crate) label: String,
    pub(crate) generator: ProviderIdUrlGenerator,
}

/// Link generators keyed by provider id, then entity and id type.
#[derive(Clone, Debug, Default)]
pub(crate) struct IdLinkGenerators(HashMap<String, HashMap<(EntityType, String), IdLinkGenerator>>);

impl IdLinkGenerators {
    pub(crate) fn insert(
        &mut self,
        provider_id: &str,
        entity: EntityType,
        id_type: &str,
        generator: IdLinkGenerator,
    ) {
        self.0
            .entry(provider_id.to_string())
            .or_default()
            .insert((entity, id_type.to_string()), generator);
    }

    pub(crate) fn get(
        &self,
        provider_id: &str,
        entity: EntityType,
        id_type: &str,
    ) -> Option<&IdLinkGenerator> {
        self.0.get(provider_id)?.get(&(entity, id_type.to_string()))
    }

    /// Whether any row needs a plugin function, and so an `IdLinkContext`.
    pub(crate) fn needs_context(&self, entity: EntityType, rows: &[ExternalId]) -> bool {
        rows.iter().any(|row| {
            matches!(
                self.get(&row.provider_id, entity, &row.id_type),
                Some(IdLinkGenerator {
                    generator: ProviderIdUrlGenerator::Function { .. },
                    ..
                })
            )
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize)]
pub(crate) struct IdLinkLocale {
    pub(crate) language: Option<String>,
    pub(crate) country: Option<String>,
}

impl From<&Library> for IdLinkLocale {
    fn from(library: &Library) -> Self {
        Self {
            language: library.language.clone(),
            country: library.country.clone(),
        }
    }
}

/// One entity whose external IDs should be turned into links.
#[derive(Clone, Debug)]
pub(crate) struct IdLinkTarget {
    pub(crate) entity_db_id: DbId,
    pub(crate) entity: EntityType,
    pub(crate) library: IdLinkLocale,
    pub(crate) rows: Vec<ExternalId>,
}

/// One generator function call; the executor builds `IdLinkContext` from it.
#[derive(Clone, Debug)]
pub(crate) struct IdLinkCall {
    pub(crate) handler_id: u64,
    pub(crate) vm_id: u64,
    pub(crate) entity: EntityType,
    pub(crate) id_type: String,
    pub(crate) id: String,
    pub(crate) library: IdLinkLocale,
    pub(crate) external_ids: Arc<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IdLinkCallError {
    Failed(String),
    /// Used up its full CPU budget.
    TimedOut,
    /// Not started or not finished before the batch deadline, or skipped
    /// after its generator timed out.
    NotRun,
}

impl std::fmt::Display for IdLinkCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(error) => f.write_str(error),
            Self::TimedOut => f.write_str("timed out"),
            Self::NotRun => f.write_str("not run within the batch deadline"),
        }
    }
}

/// Per-call outcome of a batch: a URL, `None` for no link, or an error.
pub(crate) type IdLinkCallResult = Result<Option<String>, IdLinkCallError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct IdLink {
    pub(crate) provider_id: String,
    /// Name of the site the link points to.
    pub(crate) label: String,
    pub(crate) scheme: Option<String>,
    pub(crate) id_type: String,
    pub(crate) id: String,
    pub(crate) url: String,
}

impl LuauTypeInfo for IdLink {
    fn luau_type() -> LuauType {
        LuauType::literal("IdLink")
    }
}

impl DescribeInterface for IdLink {
    fn interface_descriptor() -> InterfaceDescriptor {
        let field = |name, ty, description| FieldDescriptor {
            name,
            ty,
            description,
        };
        InterfaceDescriptor {
            name: "IdLink",
            description: Some(
                "Web link for one external ID, from the `links` include. Computed when read.",
            ),
            fields: vec![
                field("provider_id", String::luau_type(), None),
                field(
                    "label",
                    String::luau_type(),
                    Some(
                        "Name of the site the link points to: the registration's `label`, else the provider's display name.",
                    ),
                ),
                field("scheme", Option::<String>::luau_type(), None),
                field("id_type", String::luau_type(), None),
                field("id", String::luau_type(), None),
                field("url", String::luau_type(), Some("Absolute http(s) URL.")),
            ],
        }
    }
}

/// A generator function's identity: its VM and handler id. Both change
/// whenever the plugin reloads, so cached results never outlive it.
type GeneratorId = (u64, u64);

/// Everything a generator call sees, so cached results are never served for
/// a different `IdLinkContext`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct IdLinkCacheKey {
    generator: GeneratorId,
    id: String,
    library: IdLinkLocale,
    external_ids: Arc<BTreeMap<String, String>>,
}

impl IdLinkCacheKey {
    fn of(call: &IdLinkCall) -> Self {
        // Exhaustive, so a new call field must be added to the key too.
        let IdLinkCall {
            handler_id,
            vm_id,
            // Fixed per generator.
            entity: _,
            id_type: _,
            id,
            library,
            external_ids,
        } = call;
        Self {
            generator: (*vm_id, *handler_id),
            id: id.clone(),
            library: library.clone(),
            external_ids: external_ids.clone(),
        }
    }
}

/// Generator function results, with `None` for "no link", and generators
/// suspended after timing out.
#[derive(Default)]
pub(crate) struct IdLinkCache {
    links: HashMap<IdLinkCacheKey, Option<String>>,
    suspended: HashMap<GeneratorId, Instant>,
    warned: HashSet<GeneratorId>,
}

impl IdLinkCache {
    fn is_suspended(&self, generator: GeneratorId, now: Instant) -> bool {
        self.suspended
            .get(&generator)
            .is_some_and(|since| now.duration_since(*since) < ID_LINK_SUSPENSION)
    }

    fn suspend(&mut self, generator: GeneratorId, now: Instant) {
        self.suspended
            .retain(|_, since| now.duration_since(*since) < ID_LINK_SUSPENSION);
        self.suspended.insert(generator, now);
    }

    fn insert(&mut self, key: IdLinkCacheKey, url: Option<String>) {
        if self.links.len() >= ID_LINK_CACHE_CAPACITY {
            self.links.clear();
            self.warned.clear();
        }
        self.links.insert(key, url);
    }

    fn warn_once(
        &mut self,
        generator: GeneratorId,
        provider_id: &str,
        id_type: &str,
        error: &IdLinkCallError,
    ) {
        if self.warned.insert(generator) {
            tracing::warn!(
                provider_id,
                id_type,
                error = %error,
                "id link generator failed; omitting its links"
            );
        }
    }
}

enum LinkSlot {
    Ready(IdLink),
    Pending(IdLink, IdLinkCacheKey),
}

/// Resolves links for every target, calling `dispatch` at most once per
/// provider. Generator failures only drop the affected links.
pub(crate) async fn resolve_id_links<F, Fut>(
    targets: Vec<IdLinkTarget>,
    generators: &IdLinkGenerators,
    dispatch: F,
) -> HashMap<DbId, Vec<IdLink>>
where
    F: Fn(String, Vec<IdLinkCall>) -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<IdLinkCallResult>>>,
{
    let cache = crate::STATE.generation().providers.id_link_cache();
    resolve_with_cache(&cache, targets, generators, dispatch).await
}

async fn resolve_with_cache<F, Fut>(
    cache: &Mutex<IdLinkCache>,
    targets: Vec<IdLinkTarget>,
    generators: &IdLinkGenerators,
    dispatch: F,
) -> HashMap<DbId, Vec<IdLink>>
where
    F: Fn(String, Vec<IdLinkCall>) -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<IdLinkCallResult>>>,
{
    let lock = || {
        cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    };
    let mut slots = Vec::new();
    let mut batches: BTreeMap<String, Vec<(IdLinkCacheKey, IdLinkCall)>> = BTreeMap::new();
    {
        let cache = lock();
        let now = Instant::now();
        let mut queued = HashSet::new();
        for target in &targets {
            let mut rows = target
                .rows
                .iter()
                .filter(|row| !row.id_value.trim().is_empty())
                .collect::<Vec<_>>();
            rows.sort_by(|a, b| (&a.provider_id, &a.id_type).cmp(&(&b.provider_id, &b.id_type)));
            let mut provider_ids: HashMap<&str, Arc<BTreeMap<String, String>>> = HashMap::new();
            for row in rows {
                let Some(generator) = generators.get(&row.provider_id, target.entity, &row.id_type)
                else {
                    continue;
                };
                let id = row.id_value.trim().to_string();
                let mut link = IdLink {
                    provider_id: row.provider_id.clone(),
                    label: generator.label.clone(),
                    scheme: generator.scheme.clone(),
                    id_type: row.id_type.clone(),
                    id: id.clone(),
                    url: String::new(),
                };
                let generator_id = match &generator.generator {
                    ProviderIdUrlGenerator::Template(template) => {
                        if let Some(url) = render_template(template, &id) {
                            link.url = url;
                            slots.push((target.entity_db_id, LinkSlot::Ready(link)));
                        }
                        continue;
                    }
                    ProviderIdUrlGenerator::Function { handler, vm_id } => {
                        (*vm_id, handler.handler_id)
                    }
                };
                let call = IdLinkCall {
                    handler_id: generator_id.1,
                    vm_id: generator_id.0,
                    entity: target.entity,
                    id_type: row.id_type.clone(),
                    id,
                    library: target.library.clone(),
                    external_ids: provider_ids
                        .entry(&row.provider_id)
                        .or_insert_with(|| provider_external_ids(&target.rows, &row.provider_id))
                        .clone(),
                };
                let key = IdLinkCacheKey::of(&call);
                match cache.links.get(&key) {
                    Some(Some(url)) => {
                        link.url = url.clone();
                        slots.push((target.entity_db_id, LinkSlot::Ready(link)));
                    }
                    Some(None) => {}
                    None if cache.is_suspended(generator_id, now) => {}
                    None => {
                        if queued.insert(key.clone()) {
                            batches
                                .entry(row.provider_id.clone())
                                .or_default()
                                .push((key.clone(), call));
                        }
                        slots.push((target.entity_db_id, LinkSlot::Pending(link, key)));
                    }
                }
            }
        }
    }

    let outcomes = futures::future::join_all(batches.into_iter().map(|(provider_id, batch)| {
        let (keys, calls): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .map(|(key, call)| ((key, call.id_type.clone()), call))
            .unzip();
        let call = dispatch(provider_id.clone(), calls);
        async move { (provider_id, keys, call.await) }
    }))
    .await;

    let mut resolved = HashMap::new();
    {
        let mut cache = lock();
        let now = Instant::now();
        for (provider_id, keys, outcome) in outcomes {
            let results = match outcome {
                Ok(results) if results.len() == keys.len() => results,
                Ok(results) => {
                    tracing::warn!(
                        provider_id,
                        expected = keys.len(),
                        got = results.len(),
                        "id link batch returned the wrong number of results; omitting its links"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        provider_id,
                        error = format!("{error:#}"),
                        "id link batch could not run; omitting its links"
                    );
                    continue;
                }
            };
            for ((key, id_type), result) in keys.into_iter().zip(results) {
                let result = result.and_then(|url| {
                    url.map(|url| {
                        parse_link_url(&url).map(String::from).ok_or_else(|| {
                            IdLinkCallError::Failed(format!(
                                "generator returned an invalid http(s) URL: {url}"
                            ))
                        })
                    })
                    .transpose()
                });
                match result {
                    Ok(url) => {
                        cache.insert(key.clone(), url.clone());
                        resolved.insert(key, url);
                    }
                    Err(IdLinkCallError::NotRun) => {}
                    Err(error) => {
                        if error == IdLinkCallError::TimedOut {
                            cache.suspend(key.generator, now);
                        }
                        cache.warn_once(key.generator, &provider_id, &id_type, &error);
                    }
                }
            }
        }
    }

    let mut links: HashMap<DbId, Vec<IdLink>> = HashMap::new();
    for (entity_db_id, slot) in slots {
        let link = match slot {
            LinkSlot::Ready(link) => link,
            LinkSlot::Pending(mut link, key) => match resolved.get(&key) {
                Some(Some(url)) => {
                    link.url = url.clone();
                    link
                }
                _ => continue,
            },
        };
        links.entry(entity_db_id).or_default().push(link);
    }
    links
}

fn provider_external_ids(rows: &[ExternalId], provider_id: &str) -> Arc<BTreeMap<String, String>> {
    Arc::new(
        rows.iter()
            .filter(|row| row.provider_id == provider_id && !row.id_value.trim().is_empty())
            .map(|row| (row.id_type.clone(), row.id_value.trim().to_string()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

    use super::*;
    use crate::db::IdSource;
    use crate::services::providers::ProviderCallbackHandle;

    fn row(provider_id: &str, id_type: &str, id_value: &str) -> ExternalId {
        ExternalId {
            db_id: None,
            provider_id: provider_id.to_string(),
            id_type: id_type.to_string(),
            id_value: id_value.to_string(),
            source: IdSource::Plugin,
        }
    }

    fn target(db_id: i64, entity: EntityType, rows: Vec<ExternalId>) -> IdLinkTarget {
        IdLinkTarget {
            entity_db_id: DbId(db_id),
            entity,
            library: IdLinkLocale {
                language: Some("eng".to_string()),
                country: None,
            },
            rows,
        }
    }

    fn template(url: &str) -> IdLinkGenerator {
        IdLinkGenerator {
            scheme: Some("example:thing".to_string()),
            label: "Alpha".to_string(),
            generator: ProviderIdUrlGenerator::Template(url.to_string()),
        }
    }

    fn function(handler_id: u64) -> IdLinkGenerator {
        IdLinkGenerator {
            scheme: None,
            label: "Alpha".to_string(),
            generator: ProviderIdUrlGenerator::Function {
                handler: ProviderCallbackHandle { handler_id },
                vm_id: 1,
            },
        }
    }

    /// Dispatch stub answering `https://example.test/<id_type>/<id>`, plus
    /// `?sibling=<sibling_id>` or `?lang=<language>` when present, and
    /// counting calls.
    fn counting_dispatch(
        calls: Arc<AtomicUsize>,
    ) -> impl Fn(String, Vec<IdLinkCall>) -> std::future::Ready<anyhow::Result<Vec<IdLinkCallResult>>>
    {
        move |_provider_id, batch| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(batch
                .into_iter()
                .map(|call| {
                    let suffix = call
                        .external_ids
                        .get("sibling_id")
                        .map(|sibling| format!("?sibling={sibling}"))
                        .or_else(|| {
                            call.library
                                .language
                                .filter(|language| language != "eng")
                                .map(|language| format!("?lang={language}"))
                        })
                        .unwrap_or_default();
                    Ok(Some(format!(
                        "https://example.test/{}/{}{suffix}",
                        call.id_type, call.id
                    )))
                })
                .collect()))
        }
    }

    fn urls(links: &HashMap<DbId, Vec<IdLink>>, db_id: i64) -> Vec<String> {
        links
            .get(&DbId(db_id))
            .map(|links| links.iter().map(|link| link.url.clone()).collect())
            .unwrap_or_default()
    }

    #[test]
    fn templates_must_be_absolute_http_urls_with_the_id_after_the_host() {
        for template in [
            "https://example.test/release/{id}",
            "http://example.test/?id={id}",
            "https://example.test#{id}",
        ] {
            assert!(validate_id_link_template(template).is_ok(), "{template}");
        }
        for template in [
            "{id}",
            "/release/{id}",
            "example.test/{id}",
            "javascript:alert('{id}')",
            "ftp://example.test/{id}",
            "https://{id}.example.test/",
            "https://example.test{id}",
            "https://user:{id}@example.test/",
            "https://example.test/{id}/{id}",
            "https://example.test/release",
        ] {
            assert!(validate_id_link_template(template).is_err(), "{template}");
        }
    }

    #[tokio::test]
    async fn templates_substitute_the_encoded_id_without_calling_plugins() {
        let mut generators = IdLinkGenerators::default();
        generators.insert(
            "alpha",
            EntityType::Release,
            "thing_id",
            template("https://example.test/thing/{id}"),
        );
        let calls = Arc::new(AtomicUsize::new(0));

        let links = resolve_with_cache(
            &Mutex::default(),
            vec![
                target(
                    1,
                    EntityType::Release,
                    vec![
                        row("alpha", "thing_id", " a/b?c#d e "),
                        row("alpha", "other", "x"),
                    ],
                ),
                target(2, EntityType::Release, vec![row("alpha", "thing_id", "..")]),
                target(3, EntityType::Release, vec![row("alpha", "thing_id", ".")]),
            ],
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            links[&DbId(1)],
            vec![IdLink {
                provider_id: "alpha".to_string(),
                label: "Alpha".to_string(),
                scheme: Some("example:thing".to_string()),
                id_type: "thing_id".to_string(),
                id: "a/b?c#d e".to_string(),
                url: "https://example.test/thing/a%2Fb%3Fc%23d%20e".to_string(),
            }]
        );
        assert!(urls(&links, 2).is_empty(), "dot segments are dropped");
        assert!(urls(&links, 3).is_empty(), "dot segments are dropped");
    }

    #[tokio::test]
    async fn same_id_type_on_two_entities_uses_each_entitys_generator() {
        let mut generators = IdLinkGenerators::default();
        generators.insert(
            "alpha",
            EntityType::Release,
            "item",
            template("https://example.test/release/{id}"),
        );
        generators.insert(
            "alpha",
            EntityType::Artist,
            "item",
            template("https://example.test/artist/{id}"),
        );

        let links = resolve_with_cache(
            &Mutex::default(),
            vec![
                target(1, EntityType::Release, vec![row("alpha", "item", "r")]),
                target(2, EntityType::Artist, vec![row("alpha", "item", "a")]),
            ],
            &generators,
            counting_dispatch(Arc::default()),
        )
        .await;

        assert_eq!(urls(&links, 1), vec!["https://example.test/release/r"]);
        assert_eq!(urls(&links, 2), vec!["https://example.test/artist/a"]);
    }

    #[tokio::test]
    async fn functions_are_batched_per_provider_and_cached_per_generator() {
        let mut generators = IdLinkGenerators::default();
        generators.insert("alpha", EntityType::Release, "thing_id", function(1));
        generators.insert("alpha", EntityType::Track, "thing_id", function(2));
        generators.insert("beta", EntityType::Release, "thing_id", function(3));
        let targets = || {
            vec![
                target(
                    1,
                    EntityType::Release,
                    vec![
                        row("alpha", "thing_id", "r1"),
                        row("beta", "thing_id", "b1"),
                    ],
                ),
                target(2, EntityType::Track, vec![row("alpha", "thing_id", "t1")]),
                target(3, EntityType::Track, vec![row("alpha", "thing_id", "t2")]),
            ]
        };
        let cache = Mutex::default();
        let calls = Arc::new(AtomicUsize::new(0));

        let links = resolve_with_cache(
            &cache,
            targets(),
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            urls(&links, 1),
            vec![
                "https://example.test/thing_id/r1",
                "https://example.test/thing_id/b1"
            ]
        );
        assert_eq!(urls(&links, 3), vec!["https://example.test/thing_id/t2"]);

        let cached = resolve_with_cache(
            &cache,
            targets(),
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cached, links);

        let mut sibling_changed = targets();
        sibling_changed[0]
            .rows
            .push(row("alpha", "sibling_id", "s1"));
        let refreshed = resolve_with_cache(
            &cache,
            sibling_changed,
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "a changed sibling id misses the cache"
        );
        assert_eq!(
            urls(&refreshed, 1),
            vec![
                "https://example.test/thing_id/r1?sibling=s1",
                "https://example.test/thing_id/b1"
            ]
        );
        assert_eq!(urls(&refreshed, 3), urls(&links, 3));

        let mut reloaded = generators.clone();
        reloaded.insert("alpha", EntityType::Release, "thing_id", function(4));
        resolve_with_cache(
            &cache,
            targets(),
            &reloaded,
            counting_dispatch(calls.clone()),
        )
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "a re-registered generator never reads the old one's results"
        );
    }

    #[tokio::test]
    async fn locale_is_part_of_the_cache_key() {
        let mut generators = IdLinkGenerators::default();
        generators.insert("alpha", EntityType::Release, "thing_id", function(1));
        let cache = Mutex::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let localized = |language: &str| {
            let mut target = target(1, EntityType::Release, vec![row("alpha", "thing_id", "r")]);
            target.library.language = Some(language.to_string());
            vec![target]
        };

        let english = resolve_with_cache(
            &cache,
            localized("eng"),
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;
        let japanese = resolve_with_cache(
            &cache,
            localized("jpn"),
            &generators,
            counting_dispatch(calls.clone()),
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(urls(&english, 1), vec!["https://example.test/thing_id/r"]);
        assert_eq!(
            urls(&japanese, 1),
            vec!["https://example.test/thing_id/r?lang=jpn"]
        );
    }

    #[tokio::test]
    async fn failed_calls_drop_only_their_links_and_are_not_cached() {
        let mut generators = IdLinkGenerators::default();
        generators.insert("alpha", EntityType::Release, "fails", function(1));
        generators.insert("alpha", EntityType::Release, "nil", function(2));
        generators.insert("alpha", EntityType::Release, "not_run", function(3));
        generators.insert("alpha", EntityType::Release, "ok", function(4));
        generators.insert("alpha", EntityType::Release, "unsafe", function(5));
        generators.insert(
            "beta",
            EntityType::Release,
            "thing_id",
            template("https://example.test/{id}"),
        );
        generators.insert("gamma", EntityType::Release, "thing_id", function(6));
        let cache = Mutex::default();
        let targets = || {
            vec![target(
                1,
                EntityType::Release,
                vec![
                    row("alpha", "fails", "1"),
                    row("alpha", "nil", "2"),
                    row("alpha", "not_run", "3"),
                    row("alpha", "ok", "4"),
                    row("alpha", "unsafe", "5"),
                    row("beta", "thing_id", "6"),
                    row("gamma", "thing_id", "7"),
                ],
            )]
        };
        let dispatched = Arc::new(Mutex::new(Vec::new()));
        let dispatch = |provider_id: String, batch: Vec<IdLinkCall>| {
            dispatched
                .lock()
                .unwrap()
                .extend(batch.iter().map(|call| call.id_type.clone()));
            std::future::ready(if provider_id == "gamma" {
                Err(anyhow::anyhow!("runtime changed"))
            } else {
                Ok(batch
                    .into_iter()
                    .map(|call| match call.id_type.as_str() {
                        "fails" => Err(IdLinkCallError::Failed("boom".to_string())),
                        "nil" => Ok(None),
                        "not_run" => Err(IdLinkCallError::NotRun),
                        "unsafe" => Ok(Some("javascript:alert(1)".to_string())),
                        _ => Ok(Some(format!("https://example.test/ok/{}", call.id))),
                    })
                    .collect())
            })
        };

        let links = resolve_with_cache(&cache, targets(), &generators, dispatch).await;
        assert_eq!(
            urls(&links, 1),
            vec!["https://example.test/ok/4", "https://example.test/6"]
        );
        assert_eq!(cache.lock().unwrap().links.len(), 2, "only nil and ok");
        assert!(cache.lock().unwrap().suspended.is_empty());

        dispatched.lock().unwrap().clear();
        let again = resolve_with_cache(&cache, targets(), &generators, dispatch).await;
        assert_eq!(again, links);
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec!["fails", "not_run", "unsafe", "thing_id"],
            "failures are retried on the next read"
        );
    }

    #[tokio::test]
    async fn a_timeout_suspends_only_that_generator() {
        let mut generators = IdLinkGenerators::default();
        generators.insert("alpha", EntityType::Release, "slow", function(1));
        generators.insert("alpha", EntityType::Release, "fast", function(2));
        let cache = Mutex::default();
        let dispatched = Arc::new(Mutex::new(Vec::new()));
        let dispatch = |_provider_id: String, batch: Vec<IdLinkCall>| {
            dispatched
                .lock()
                .unwrap()
                .extend(batch.iter().map(|call| call.id.clone()));
            std::future::ready(Ok(batch
                .into_iter()
                .map(|call| match call.id_type.as_str() {
                    "slow" => Err(IdLinkCallError::TimedOut),
                    _ => Ok(Some(format!("https://example.test/{}", call.id))),
                })
                .collect()))
        };

        let links = resolve_with_cache(
            &cache,
            vec![target(
                1,
                EntityType::Release,
                vec![row("alpha", "fast", "a"), row("alpha", "slow", "s")],
            )],
            &generators,
            dispatch,
        )
        .await;
        assert_eq!(urls(&links, 1), vec!["https://example.test/a"]);

        dispatched.lock().unwrap().clear();
        let later = resolve_with_cache(
            &cache,
            vec![
                target(
                    2,
                    EntityType::Release,
                    vec![row("alpha", "fast", "a"), row("alpha", "slow", "s")],
                ),
                target(
                    3,
                    EntityType::Release,
                    vec![row("alpha", "fast", "b"), row("alpha", "slow", "t")],
                ),
            ],
            &generators,
            dispatch,
        )
        .await;
        assert_eq!(urls(&later, 2), vec!["https://example.test/a"]);
        assert_eq!(urls(&later, 3), vec!["https://example.test/b"]);
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec!["b"],
            "the slow generator is skipped, its sibling still runs"
        );

        let mut reloaded = generators.clone();
        reloaded.insert("alpha", EntityType::Release, "slow", function(3));
        dispatched.lock().unwrap().clear();
        resolve_with_cache(
            &cache,
            vec![target(
                4,
                EntityType::Release,
                vec![row("alpha", "slow", "u")],
            )],
            &reloaded,
            dispatch,
        )
        .await;
        assert_eq!(
            *dispatched.lock().unwrap(),
            vec!["u"],
            "a reloaded generator is not suspended"
        );
    }
}
