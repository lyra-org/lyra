// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        OnceLock,
    },
    time::{
        Duration,
        Instant,
    },
};

use base64::{
    Engine as _,
    engine::general_purpose::URL_SAFE_NO_PAD,
};

const CURSOR_VERSION: u8 = 1;
const SNAPSHOT_ID_LEN: usize = 16;
const CURSOR_MAC_LEN: usize = 16;
const CURSOR_BYTES_LEN: usize = 1 + SNAPSHOT_ID_LEN + size_of::<u64>() + CURSOR_MAC_LEN;
const CURSOR_ENCODED_LEN: usize = 55;

const SNAPSHOT_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_SNAPSHOTS: usize = 256;
const MAX_SNAPSHOTS_PER_OWNER: usize = 8;
const MAX_ITEMS_PER_SNAPSHOT: usize = 1_000_000;
const MAX_TOTAL_ITEMS: usize = 2_000_000;

pub(crate) const fn snapshot_item_capacity() -> usize {
    MAX_ITEMS_PER_SNAPSHOT
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PaginationError {
    #[error("malformed cursor")]
    MalformedCursor,
    #[error("pagination cursor expired; restart pagination")]
    ExpiredCursor,
    #[error("pagination cursor does not match this collection; restart pagination")]
    ContextMismatch,
    #[error("collection exceeds the pagination snapshot capacity")]
    SnapshotTooLarge,
}

#[derive(Clone)]
pub(crate) struct SnapshotKey {
    owner: String,
    digest: [u8; 32],
}

pub(crate) struct SnapshotKeyBuilder {
    owner: String,
    hasher: blake3::Hasher,
}

impl SnapshotKey {
    pub(crate) fn builder(owner: &str, collection: &str) -> SnapshotKeyBuilder {
        let mut hasher = blake3::Hasher::new();
        hash_component(&mut hasher, owner.as_bytes());
        hash_component(&mut hasher, collection.as_bytes());
        SnapshotKeyBuilder {
            owner: owner.to_string(),
            hasher,
        }
    }
}

impl SnapshotKeyBuilder {
    pub(crate) fn field(mut self, value: Option<&str>) -> Self {
        match value {
            Some(value) => {
                self.hasher.update(&[1]);
                hash_component(&mut self.hasher, value.as_bytes());
            }
            None => {
                self.hasher.update(&[0]);
            }
        }
        self
    }

    pub(crate) fn values(mut self, values: Option<&[String]>) -> Self {
        match values {
            Some(values) => {
                self.hasher.update(&[1]);
                self.hasher.update(&(values.len() as u64).to_be_bytes());
                for value in values {
                    hash_component(&mut self.hasher, value.as_bytes());
                }
            }
            None => {
                self.hasher.update(&[0]);
            }
        }
        self
    }

    pub(crate) fn finish(self) -> SnapshotKey {
        SnapshotKey {
            owner: self.owner,
            digest: *self.hasher.finalize().as_bytes(),
        }
    }
}

fn hash_component(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[derive(Debug)]
pub(crate) struct SnapshotPage {
    pub(crate) item_ids: Vec<String>,
    pub(crate) next_cursor: Option<String>,
}

struct Snapshot {
    owner: String,
    context_digest: [u8; 32],
    item_ids: Vec<String>,
    expires_at: Instant,
    created_at: Instant,
}

#[derive(Default)]
struct SnapshotState {
    snapshots: HashMap<[u8; SNAPSHOT_ID_LEN], Snapshot>,
    total_items: usize,
}

pub(crate) struct SnapshotRegistry {
    secret: [u8; 32],
    state: Mutex<SnapshotState>,
}

impl Default for SnapshotRegistry {
    fn default() -> Self {
        static PROCESS_SECRET: OnceLock<[u8; 32]> = OnceLock::new();
        Self {
            secret: *PROCESS_SECRET.get_or_init(rand::random),
            state: Mutex::new(SnapshotState::default()),
        }
    }
}

impl SnapshotRegistry {
    pub(crate) fn start(
        &self,
        key: &SnapshotKey,
        item_ids: Vec<String>,
        limit: usize,
    ) -> Result<SnapshotPage, PaginationError> {
        let limit = limit.max(1);
        if item_ids.len() <= limit {
            return Ok(SnapshotPage {
                item_ids,
                next_cursor: None,
            });
        }
        if item_ids.len() > MAX_ITEMS_PER_SNAPSHOT || item_ids.len() > MAX_TOTAL_ITEMS {
            return Err(PaginationError::SnapshotTooLarge);
        }

        let now = Instant::now();
        let mut state = self.state.lock().expect("pagination registry poisoned");
        prune_expired(&mut state, now);
        evict_for_owner(&mut state, &key.owner);
        evict_for_capacity(&mut state, item_ids.len());

        let snapshot_id = unique_snapshot_id(&state.snapshots);
        let first_page = item_ids[..limit].to_vec();
        let next_cursor = Some(self.encode_cursor(snapshot_id, limit as u64));
        state.total_items = state.total_items.saturating_add(item_ids.len());
        state.snapshots.insert(
            snapshot_id,
            Snapshot {
                owner: key.owner.clone(),
                context_digest: key.digest,
                item_ids,
                expires_at: now + SNAPSHOT_TTL,
                created_at: now,
            },
        );

        Ok(SnapshotPage {
            item_ids: first_page,
            next_cursor,
        })
    }

    pub(crate) fn resume(
        &self,
        key: &SnapshotKey,
        raw_cursor: &str,
        limit: usize,
    ) -> Result<SnapshotPage, PaginationError> {
        let limit = limit.max(1);
        let (snapshot_id, offset) = self.decode_cursor(raw_cursor)?;
        let offset = usize::try_from(offset).map_err(|_| PaginationError::MalformedCursor)?;
        let now = Instant::now();
        let mut state = self.state.lock().expect("pagination registry poisoned");
        prune_expired(&mut state, now);

        let snapshot = state
            .snapshots
            .get(&snapshot_id)
            .ok_or(PaginationError::ExpiredCursor)?;
        if snapshot.owner != key.owner || snapshot.context_digest != key.digest {
            return Err(PaginationError::ContextMismatch);
        }
        if offset > snapshot.item_ids.len() {
            return Err(PaginationError::MalformedCursor);
        }

        let end = offset.saturating_add(limit).min(snapshot.item_ids.len());
        let item_ids = snapshot.item_ids[offset..end].to_vec();
        let next_cursor =
            (end < snapshot.item_ids.len()).then(|| self.encode_cursor(snapshot_id, end as u64));

        Ok(SnapshotPage {
            item_ids,
            next_cursor,
        })
    }

    fn encode_cursor(&self, snapshot_id: [u8; SNAPSHOT_ID_LEN], offset: u64) -> String {
        let mut bytes = [0_u8; CURSOR_BYTES_LEN];
        bytes[0] = CURSOR_VERSION;
        bytes[1..1 + SNAPSHOT_ID_LEN].copy_from_slice(&snapshot_id);
        bytes[1 + SNAPSHOT_ID_LEN..1 + SNAPSHOT_ID_LEN + size_of::<u64>()]
            .copy_from_slice(&offset.to_be_bytes());
        let mac = blake3::keyed_hash(&self.secret, &bytes[..CURSOR_BYTES_LEN - CURSOR_MAC_LEN]);
        bytes[CURSOR_BYTES_LEN - CURSOR_MAC_LEN..]
            .copy_from_slice(&mac.as_bytes()[..CURSOR_MAC_LEN]);
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn decode_cursor(&self, raw: &str) -> Result<([u8; SNAPSHOT_ID_LEN], u64), PaginationError> {
        if raw.len() != CURSOR_ENCODED_LEN {
            return Err(PaginationError::MalformedCursor);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| PaginationError::MalformedCursor)?;
        if bytes.len() != CURSOR_BYTES_LEN || bytes[0] != CURSOR_VERSION {
            return Err(PaginationError::MalformedCursor);
        }
        let expected =
            blake3::keyed_hash(&self.secret, &bytes[..CURSOR_BYTES_LEN - CURSOR_MAC_LEN]);
        if expected.as_bytes()[..CURSOR_MAC_LEN] != bytes[CURSOR_BYTES_LEN - CURSOR_MAC_LEN..] {
            return Err(PaginationError::MalformedCursor);
        }

        let snapshot_id = bytes[1..1 + SNAPSHOT_ID_LEN]
            .try_into()
            .expect("snapshot id length is fixed");
        let offset = u64::from_be_bytes(
            bytes[1 + SNAPSHOT_ID_LEN..1 + SNAPSHOT_ID_LEN + size_of::<u64>()]
                .try_into()
                .expect("cursor offset length is fixed"),
        );
        Ok((snapshot_id, offset))
    }
}

fn unique_snapshot_id(
    snapshots: &HashMap<[u8; SNAPSHOT_ID_LEN], Snapshot>,
) -> [u8; SNAPSHOT_ID_LEN] {
    loop {
        let candidate = rand::random();
        if !snapshots.contains_key(&candidate) {
            return candidate;
        }
    }
}

fn prune_expired(state: &mut SnapshotState, now: Instant) {
    let expired = state
        .snapshots
        .iter()
        .filter_map(|(id, snapshot)| (snapshot.expires_at <= now).then_some(*id))
        .collect::<Vec<_>>();
    for id in expired {
        remove_snapshot(state, id);
    }
}

fn evict_for_owner(state: &mut SnapshotState, owner: &str) {
    while state
        .snapshots
        .values()
        .filter(|snapshot| snapshot.owner == owner)
        .count()
        >= MAX_SNAPSHOTS_PER_OWNER
    {
        let Some(id) = oldest_snapshot(state, |snapshot| snapshot.owner == owner) else {
            break;
        };
        remove_snapshot(state, id);
    }
}

fn evict_for_capacity(state: &mut SnapshotState, incoming_items: usize) {
    while state.snapshots.len() >= MAX_SNAPSHOTS
        || state.total_items.saturating_add(incoming_items) > MAX_TOTAL_ITEMS
    {
        let Some(id) = oldest_snapshot(state, |_| true) else {
            break;
        };
        remove_snapshot(state, id);
    }
}

fn oldest_snapshot(
    state: &SnapshotState,
    predicate: impl Fn(&Snapshot) -> bool,
) -> Option<[u8; SNAPSHOT_ID_LEN]> {
    state
        .snapshots
        .iter()
        .filter(|(_, snapshot)| predicate(snapshot))
        .min_by_key(|(_, snapshot)| snapshot.created_at)
        .map(|(id, _)| *id)
}

fn remove_snapshot(state: &mut SnapshotState, id: [u8; SNAPSHOT_ID_LEN]) {
    if let Some(snapshot) = state.snapshots.remove(&id) {
        state.total_items = state.total_items.saturating_sub(snapshot.item_ids.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(owner: &str, query: &str) -> SnapshotKey {
        SnapshotKey::builder(owner, "tracks")
            .field(Some(query))
            .finish()
    }

    #[test]
    fn snapshot_pages_are_stable_and_limit_can_change() -> anyhow::Result<()> {
        let registry = SnapshotRegistry::default();
        let key = key("user", "blue");
        let first = registry.start(
            &key,
            vec!["a".into(), "b".into(), "c".into(), "d".into()],
            1,
        )?;
        assert_eq!(first.item_ids, ["a"]);

        let second = registry.resume(&key, first.next_cursor.as_deref().expect("cursor"), 2)?;
        assert_eq!(second.item_ids, ["b", "c"]);

        let third = registry.resume(&key, second.next_cursor.as_deref().expect("cursor"), 2)?;
        assert_eq!(third.item_ids, ["d"]);
        assert!(third.next_cursor.is_none());
        Ok(())
    }

    #[test]
    fn cursor_is_bound_to_owner_and_query() -> anyhow::Result<()> {
        let registry = SnapshotRegistry::default();
        let snapshot_key = key("user", "blue");
        let first = registry.start(&snapshot_key, vec!["a".into(), "b".into()], 1)?;
        let cursor = first.next_cursor.as_deref().expect("cursor");

        assert!(matches!(
            registry.resume(&key("other", "blue"), cursor, 1),
            Err(PaginationError::ContextMismatch)
        ));
        assert!(matches!(
            registry.resume(&key("user", "red"), cursor, 1),
            Err(PaginationError::ContextMismatch)
        ));
        Ok(())
    }

    #[test]
    fn cursor_tampering_is_rejected() -> anyhow::Result<()> {
        let registry = SnapshotRegistry::default();
        let key = key("user", "blue");
        let first = registry.start(&key, vec!["a".into(), "b".into()], 1)?;
        let mut cursor = first.next_cursor.expect("cursor").into_bytes();
        cursor[10] = if cursor[10] == b'A' { b'B' } else { b'A' };
        let cursor = String::from_utf8(cursor)?;

        assert!(matches!(
            registry.resume(&key, &cursor, 1),
            Err(PaginationError::MalformedCursor)
        ));
        Ok(())
    }

    #[test]
    fn cursor_from_replaced_registry_is_expired() -> anyhow::Result<()> {
        let old_registry = SnapshotRegistry::default();
        let key = key("user", "blue");
        let first = old_registry.start(&key, vec!["a".into(), "b".into()], 1)?;

        let new_registry = SnapshotRegistry::default();
        assert!(matches!(
            new_registry.resume(&key, first.next_cursor.as_deref().expect("cursor"), 1),
            Err(PaginationError::ExpiredCursor)
        ));
        Ok(())
    }
}
