// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! An in-memory index of the catalog that queries find, search, sort and page over.
//!
//! Each entity gets a dense slot per kind. A [`Snapshot`] holds what browsing reads that is the
//! same for every viewer: names and their words, sortable fields, and the edges between kinds.
//! A query reads one snapshot taken under its database guard, rebuilding it first when the
//! database changed since; per-viewer data such as listens and ratings stays in the database.

pub(crate) mod bits;
pub(crate) mod words;

use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
        OnceLock,
    },
};

use agdb::{
    DbAny,
    DbId,
};

use self::{
    bits::{
        Bits,
        Links,
    },
    words::{
        Field,
        Strings,
        Words,
        WordsBuilder,
    },
};
use super::Direction;
use crate::services::search::{
    self,
    Needle,
    Tier,
};
use lyra_db::{
    ArtistType,
    CreditType,
    catalog::CatalogCredit,
};

/// A value that sorts after every other, standing for a missing one.
pub(crate) const MISSING: u64 = u64::MAX;

/// The entities of one kind, by slot.
#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) ids: Vec<DbId>,
    slots: HashMap<DbId, u32>,
    /// The lowercase sort name, which name ranges bound.
    sort_names: Strings,
    /// The search keys of each name and of each sort name differing from it.
    keys: Strings,
    sort_keys: Strings,
    /// Every slot in order of its name's search key, built on first use.
    by_key: OnceLock<Box<[u32]>>,
    /// Each slot's place in name order: sort name, lowercase name, name, id.
    pub(crate) rank: Vec<u32>,
    by_rank: Arc<[u32]>,
    /// Each slot's lowercase name as a dense rank, equal names equal.
    pub(crate) name_rank: Vec<u32>,
    pub(crate) sort_name_rank: Vec<u32>,
    words: Words,
    /// Columns and orders that are the same for every viewer.
    pub(crate) cache: Cache,
}

/// What names a cached column: its sort key's type and place in that type's key list.
pub(crate) type CacheKey = (std::any::TypeId, usize);

type OrderKey = (CacheKey, bool);

/// Columns of sort values and the orders they give, built on first use.
#[derive(Debug, Default)]
pub(crate) struct Cache {
    columns: Mutex<HashMap<CacheKey, Arc<[u64]>>>,
    /// Orders by key and whether they descend.
    orders: Mutex<HashMap<OrderKey, Arc<[u32]>>>,
    /// What the columns and orders hold, counted as they are added.
    bytes: std::sync::atomic::AtomicUsize,
}

impl Cache {
    pub(crate) fn column(&self, key: CacheKey, compute: impl FnOnce() -> Vec<u64>) -> Arc<[u64]> {
        let mut columns = self.columns.lock().expect("index columns poisoned");
        Arc::clone(columns.entry(key).or_insert_with(|| {
            let column = Arc::<[u64]>::from(compute());
            self.grew(column.len() * 8);
            column
        }))
    }

    fn grew(&self, bytes: usize) {
        self.bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Every slot ordered by `column` in `direction` and then by `rank`, with missing values
    /// last, or first when `missing_first`.
    pub(crate) fn order(
        &self,
        key: CacheKey,
        direction: Direction,
        missing_first: bool,
        column: &[u64],
        rank: &[u32],
    ) -> Arc<[u32]> {
        let descending = direction == Direction::Descending;
        let mut orders = self.orders.lock().expect("index orders poisoned");
        Arc::clone(orders.entry((key, descending)).or_insert_with(|| {
            let mut order = (0..column.len() as u32).collect::<Vec<_>>();
            order.sort_unstable_by_key(|slot| {
                let value = column[*slot as usize];
                let missing = (value == MISSING) != missing_first;
                let value = match (value, descending) {
                    (MISSING, _) => 0,
                    (value, true) => u64::MAX - value,
                    (value, false) => value,
                };
                (missing, value, rank[*slot as usize])
            });
            self.grew(order.len() * 4);
            Arc::from(order)
        }))
    }

    fn memory(&self) -> usize {
        self.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// A slot a search matches and how well.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Hit {
    pub(crate) slot: u32,
    pub(crate) tier: Tier,
    /// How many of the query's words the name or sort name holds.
    pub(crate) own_words: usize,
    /// Whether the display name reached `tier`, which ranks ahead of a sort name reaching it.
    pub(crate) by_name: bool,
}

/// What a table is built from for each entity.
pub(crate) struct Named {
    pub(crate) id: DbId,
    pub(crate) name: String,
    pub(crate) sort_name: Option<String>,
    pub(crate) aliases: Vec<String>,
}

impl Table {
    fn new(entities: Vec<Named>) -> Self {
        let len = entities.len();
        let prepared = par_map(&entities, Prepared::new);
        let ids = entities.iter().map(|entity| entity.id).collect::<Vec<_>>();
        let slots = ids
            .iter()
            .enumerate()
            .map(|(slot, id)| (*id, slot as u32))
            .collect::<HashMap<_, _>>();

        let sort_name = |slot: usize| {
            let prepared = &prepared[slot];
            prepared.sort_lower.as_deref().unwrap_or(&prepared.lower)
        };
        let words = || {
            let mut words = WordsBuilder::default();
            for (slot, prepared) in prepared.iter().enumerate() {
                let slot = slot as u32;
                words.add(slot, Field::Name, &prepared.folded);
                if let Some(folded) = &prepared.sort_folded {
                    words.add(slot, Field::SortName, folded);
                }
                for alias in &prepared.aliases {
                    words.add(slot, Field::Alias, alias);
                }
            }
            words.finish()
        };
        let ranks = || {
            let mut by_rank = (0..len).collect::<Vec<_>>();
            by_rank.sort_unstable_by(|a, b| {
                sort_name(*a)
                    .cmp(sort_name(*b))
                    .then_with(|| prepared[*a].lower.cmp(&prepared[*b].lower))
                    .then_with(|| entities[*a].name.cmp(&entities[*b].name))
                    .then_with(|| ids[*a].0.cmp(&ids[*b].0))
            });
            let mut rank = vec![0; len];
            for (place, slot) in by_rank.iter().enumerate() {
                rank[*slot] = place as u32;
            }
            let sort_name_rank = dense_rank(&by_rank, len, |a, b| sort_name(a) == sort_name(b));
            (rank, by_rank, sort_name_rank)
        };
        let name_rank = || {
            let mut by_name = (0..len).collect::<Vec<_>>();
            by_name.sort_unstable_by(|a, b| prepared[*a].lower.cmp(&prepared[*b].lower));
            dense_rank(&by_name, len, |a, b| prepared[a].lower == prepared[b].lower)
        };
        let parallel = len > PARALLEL_CHUNK;
        let (words, ((rank, by_rank, sort_name_rank), name_rank)) =
            beside(parallel, words, || beside(parallel, ranks, name_rank));

        let mut sort_names = Strings::default();
        let mut keys = Strings::default();
        let mut sort_keys = Strings::default();
        for (slot, prepared) in prepared.iter().enumerate() {
            sort_names.push(sort_name(slot));
            keys.push(&prepared.key);
            sort_keys.push(prepared.sort_key.as_deref().unwrap_or(""));
        }
        Self {
            ids,
            slots,
            sort_names,
            keys,
            sort_keys,
            by_key: OnceLock::new(),
            rank,
            by_rank: by_rank.iter().map(|slot| *slot as u32).collect(),
            name_rank,
            sort_name_rank,
            words,
            cache: Cache::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn slot(&self, id: DbId) -> Option<u32> {
        self.slots.get(&id).copied()
    }

    pub(crate) fn id(&self, slot: u32) -> DbId {
        self.ids[slot as usize]
    }

    /// The slots of the entities among `ids`.
    pub(crate) fn slots<'a>(&self, ids: impl IntoIterator<Item = &'a DbId>) -> Bits {
        Bits::of(self.len(), ids.into_iter().filter_map(|id| self.slot(*id)))
    }

    /// Every slot in name order.
    pub(crate) fn name_order(&self) -> Arc<[u32]> {
        Arc::clone(&self.by_rank)
    }

    pub(crate) fn sort_name(&self, slot: u32) -> &str {
        self.sort_names.get(slot)
    }

    /// The slots whose name is `name`, compared by search [`key`](search::key) as an exact search
    /// match compares them: ignoring case, accents, apostrophes, punctuation and spaces. A name
    /// that folds away matches nothing, as a search that does.
    pub(crate) fn named(&self, name: &str) -> &[u32] {
        let key = search::key(&search::fold(name));
        if key.is_empty() {
            return &[];
        }
        let by_key = self.by_key.get_or_init(|| {
            let mut by_key = (0..self.len() as u32).collect::<Vec<_>>();
            by_key.sort_unstable_by(|a, b| self.keys.get(*a).cmp(self.keys.get(*b)));
            by_key.into_boxed_slice()
        });
        let start = by_key.partition_point(|slot| self.keys.get(*slot) < key.as_str());
        let len = by_key[start..].partition_point(|slot| self.keys.get(*slot) == key);
        &by_key[start..start + len]
    }

    /// How each slot that every word of `needle` matches does. `related` gives the slots whose
    /// other texts match a word.
    pub(crate) fn search(
        &self,
        needle: &Needle,
        within: &Bits,
        related: impl Fn(&search::Word) -> Option<Bits>,
    ) -> Vec<Hit> {
        if needle.words().is_empty() {
            return Vec::new();
        }
        let len = self.len();
        let mut matched = within.clone();
        let mut by_name = within.clone();
        let mut by_sort_name = within.clone();
        let mut own = Vec::with_capacity(needle.words().len());
        for word in needle.words() {
            let ids = self.words.matching(word);
            let name = self.words.slots(Field::Name, &ids, len);
            let sort_name = self.words.slots(Field::SortName, &ids, len);
            let mut any = self.words.slots(Field::Alias, &ids, len);
            any.or(&name);
            any.or(&sort_name);
            if let Some(related) = related(word) {
                any.or(&related);
            }
            matched.and(&any);
            by_name.and(&name);
            by_sort_name.and(&sort_name);
            if matched.is_empty() {
                return Vec::new();
            }
            let mut named = name;
            named.or(&sort_name);
            own.push(named);
        }
        matched
            .iter()
            .map(|slot| {
                let name = by_name
                    .contains(slot)
                    .then(|| needle.tier(self.keys.get(slot)));
                let sort_name = by_sort_name
                    .contains(slot)
                    .then(|| needle.tier(self.sort_keys.get(slot)));
                let own_words = own.iter().filter(|own| own.contains(slot)).count();
                let (tier, by_name) = match name.max(sort_name) {
                    Some(best) => (best, name == Some(best)),
                    None if own_words > 0 => (Tier::Partial, false),
                    None => (Tier::Related, false),
                };
                Hit {
                    slot,
                    tier,
                    own_words,
                    by_name,
                }
            })
            .collect()
    }

    /// The slots whose own texts match `word`, for searches through related entities.
    pub(crate) fn matching(&self, word: &search::Word) -> Bits {
        let ids = self.words.matching(word);
        let len = self.len();
        let mut slots = self.words.slots(Field::Name, &ids, len);
        slots.or(&self.words.slots(Field::SortName, &ids, len));
        slots.or(&self.words.slots(Field::Alias, &ids, len));
        slots
    }

    fn memory(&self) -> usize {
        self.cache.memory()
            + self.ids.capacity() * 8
            + self.slots.capacity() * 16
            + self.sort_names.memory()
            + self.keys.memory()
            + self.sort_keys.memory()
            + (self.rank.capacity()
                + self.by_rank.len()
                + self.by_key.get().map_or(0, |by_key| by_key.len())
                + self.name_rank.capacity()
                + self.sort_name_rank.capacity())
                * 4
            + self.words.memory()
    }
}

/// An entity's names as the table compares and searches them.
struct Prepared {
    lower: String,
    folded: String,
    key: String,
    sort_lower: Option<String>,
    /// The folded sort name and its key, when it folds to other than the name.
    sort_folded: Option<String>,
    sort_key: Option<String>,
    aliases: Vec<String>,
}

impl Prepared {
    fn new(entity: &Named) -> Self {
        let folded = search::fold(&entity.name);
        let sort_folded = entity
            .sort_name
            .as_deref()
            .map(search::fold)
            .filter(|sort_folded| *sort_folded != folded);
        Self {
            lower: entity.name.to_lowercase(),
            key: search::key(&folded),
            folded,
            sort_lower: entity.sort_name.as_deref().map(str::to_lowercase),
            sort_key: sort_folded.as_deref().map(search::key),
            sort_folded,
            aliases: entity
                .aliases
                .iter()
                .map(|alias| search::fold(alias))
                .collect(),
        }
    }
}

/// The fewest items worth a thread of their own.
const PARALLEL_CHUNK: usize = 4_096;

/// `a` and `b`, with `a` on a thread of its own when `parallel`.
fn beside<A: Send, B>(
    parallel: bool,
    a: impl FnOnce() -> A + Send,
    b: impl FnOnce() -> B,
) -> (A, B) {
    if !parallel {
        return (a(), b());
    }
    std::thread::scope(|scope| {
        let a = scope.spawn(a);
        let b = b();
        (
            a.join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
            b,
        )
    })
}

/// `f` of each item, computed in chunks across threads.
fn par_map<T: Sync, U: Send>(items: &[T], f: impl Fn(&T) -> U + Sync) -> Vec<U> {
    if items.len() <= PARALLEL_CHUNK {
        return items.iter().map(f).collect();
    }
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let size = items.len().div_ceil(threads).max(PARALLEL_CHUNK);
    std::thread::scope(|scope| {
        let chunks = items
            .chunks(size)
            .map(|chunk| scope.spawn(|| chunk.iter().map(&f).collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        chunks
            .into_iter()
            .flat_map(|chunk| chunk.join().expect("a chunk panicked"))
            .collect()
    })
}

/// Each slot's dense rank, given the slots in an order where equal values are adjacent.
fn dense_rank(order: &[usize], len: usize, same: impl Fn(usize, usize) -> bool) -> Vec<u32> {
    let mut rank = vec![0; len];
    let mut current = 0;
    for (place, slot) in order.iter().enumerate() {
        if place > 0 && !same(order[place - 1], *slot) {
            current += 1;
        }
        rank[*slot] = current;
    }
    rank
}

/// The fields of tracks that browsing filters and sorts by, by slot.
#[derive(Debug)]
pub(crate) struct TrackFields {
    pub(crate) created: Vec<u64>,
    pub(crate) year: Vec<u64>,
    pub(crate) duration: Vec<u64>,
    pub(crate) disc: Vec<u64>,
    pub(crate) track: Vec<u64>,
}

#[derive(Debug)]
pub(crate) struct ReleaseFields {
    pub(crate) created: Vec<u64>,
    /// [`super::releases::date_order`] of the release date.
    pub(crate) date: Vec<u64>,
    pub(crate) year: Vec<u64>,
    pub(crate) track_count: Vec<u64>,
    pub(crate) total_duration: Vec<u64>,
}

#[derive(Debug)]
pub(crate) struct ArtistFields {
    pub(crate) created: Vec<u64>,
    pub(crate) artist_type: Vec<Option<ArtistType>>,
    /// The artists a release or track credits, the only ones browsed.
    pub(crate) credited: Bits,
}

/// One credit on a release or track.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Credit {
    pub(crate) artist: u32,
    pub(crate) credit_type: CreditType,
    pub(crate) order: u64,
}

/// The credits on each owner slot, in no particular order.
#[derive(Debug, Default)]
pub(crate) struct Credits {
    offsets: Vec<u32>,
    credits: Vec<Credit>,
}

impl Credits {
    fn new(owners: usize, mut credits: Vec<(u32, Credit)>) -> Self {
        credits.sort_unstable_by_key(|(owner, _)| *owner);
        let mut offsets = Vec::with_capacity(owners + 1);
        offsets.push(0);
        let mut credits = credits.into_iter().peekable();
        let mut stored = Vec::with_capacity(credits.len());
        for owner in 0..owners as u32 {
            while let Some((_, credit)) = credits.next_if(|(slot, _)| *slot == owner) {
                stored.push(credit);
            }
            offsets.push(stored.len() as u32);
        }
        Self {
            offsets,
            credits: stored,
        }
    }

    pub(crate) fn of(&self, owner: u32) -> &[Credit] {
        &self.credits
            [self.offsets[owner as usize] as usize..self.offsets[owner as usize + 1] as usize]
    }

    /// The artists credited on `owner`.
    fn artist_links(&self, owners: usize) -> Links {
        Links::new(
            owners,
            (0..owners as u32)
                .flat_map(|owner| {
                    self.of(owner)
                        .iter()
                        .map(move |credit| (owner, credit.artist))
                })
                .collect(),
        )
    }

    fn memory(&self) -> usize {
        self.offsets.capacity() * 4 + self.credits.capacity() * std::mem::size_of::<Credit>()
    }
}

/// What a set of libraries lets a viewer see.
#[derive(Debug)]
pub(crate) struct Scope {
    /// Whether the scope leaves anything out, unlike an admin's or the system's.
    pub(crate) restricted: bool,
    pub(crate) releases: Bits,
    /// The tracks on the releases; unrestricted, every track.
    pub(crate) tracks: Bits,
    /// The credited artists credited on the releases or their tracks.
    pub(crate) artists: Bits,
    /// The genres of the releases.
    pub(crate) genres: Bits,
    /// Columns and orders whose values depend on what the scope sees.
    pub(crate) cache: Cache,
    /// The library slots the scope may see, or `None` for every library.
    libraries: Option<Vec<u32>>,
}

impl Scope {
    fn memory(&self) -> usize {
        self.libraries
            .as_ref()
            .map_or(0, |libraries| libraries.capacity() * 4)
            + self.cache.memory()
            + self.releases.memory()
            + self.tracks.memory()
            + self.artists.memory()
            + self.genres.memory()
    }
}

/// How many scopes limited to some libraries a snapshot keeps, and how much they may hold
/// together. Past either, the least recently used goes; queries holding it keep their copy.
const SCOPES_KEPT: usize = 32;
const SCOPE_BYTES_KEPT: usize = 256 << 20;

/// The scopes built for a snapshot, by library set, with when each was last used. The scope
/// that sees everything is never evicted.
#[derive(Debug)]
struct Scopes {
    everything: Option<Arc<Scope>>,
    limited: HashMap<Vec<u32>, (Arc<Scope>, u64)>,
    uses: u64,
    kept: usize,
    bytes_kept: usize,
}

impl Default for Scopes {
    fn default() -> Self {
        Self {
            everything: None,
            limited: HashMap::new(),
            uses: 0,
            kept: SCOPES_KEPT,
            bytes_kept: SCOPE_BYTES_KEPT,
        }
    }
}

/// What the scope map spends on each entry besides the scope: its key, the slot and the table.
const SCOPE_ENTRY_BYTES: usize = 96;

impl Scopes {
    fn bytes(&self) -> usize {
        self.limited
            .iter()
            .map(|(libraries, (scope, _))| {
                SCOPE_ENTRY_BYTES + libraries.capacity() * 4 + scope.memory()
            })
            .sum()
    }

    /// Drops the least recently used scopes other than the one for `keep` until the rest fit,
    /// then that one too if it alone does not. Queries holding a dropped scope keep using it.
    fn evict(&mut self, keep: &[u32]) {
        while self.limited.len() > self.kept || self.bytes() > self.bytes_kept {
            let oldest = self
                .limited
                .iter()
                .filter(|(libraries, _)| libraries.as_slice() != keep)
                .min_by_key(|(_, (_, used))| *used)
                .map(|(libraries, _)| libraries.clone())
                .unwrap_or_else(|| keep.to_vec());
            if self.limited.remove(&oldest).is_none() {
                return;
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Snapshot {
    stamp: Option<(u64, u64)>,
    pub(crate) tracks: Table,
    pub(crate) releases: Table,
    pub(crate) artists: Table,
    pub(crate) genres: Table,
    pub(crate) track: TrackFields,
    pub(crate) release: ReleaseFields,
    pub(crate) artist: ArtistFields,
    libraries: HashMap<DbId, u32>,
    library_public_ids: HashMap<String, u32>,
    library_releases: Links,
    release_tracks: Links,
    track_releases: Links,
    release_credits: Credits,
    track_credits: Credits,
    /// The artists credited on each release and track.
    release_artists: Links,
    track_artists: Links,
    /// The releases and tracks crediting each artist.
    artist_releases: Links,
    artist_tracks: Links,
    genre_releases: Links,
    release_genres: Links,
    /// The first artist credited on each track and release: the one with the lowest credit
    /// order, and the first in name order among several.
    track_lead: Vec<Option<u32>>,
    release_lead: Vec<Option<u32>>,
    scopes: Mutex<Scopes>,
}

impl Snapshot {
    pub(crate) fn build(db: &DbAny) -> anyhow::Result<Self> {
        Ok(Self::from_catalog(lyra_db::catalog::read(db)?, None))
    }

    fn from_catalog(catalog: lyra_db::catalog::Catalog, stamp: Option<(u64, u64)>) -> Self {
        let lyra_db::catalog::Catalog {
            tracks,
            releases,
            artists,
            genres,
            libraries,
            release_tracks,
            library_releases,
            genre_releases,
            credits,
        } = catalog;

        let track = TrackFields {
            created: tracks
                .iter()
                .map(|track| or_missing(track.created))
                .collect(),
            year: tracks.iter().map(|track| or_missing(track.year)).collect(),
            duration: tracks
                .iter()
                .map(|track| or_missing(track.duration_ms))
                .collect(),
            disc: tracks.iter().map(|track| or_missing(track.disc)).collect(),
            track: tracks.iter().map(|track| or_missing(track.track)).collect(),
        };
        let tracks = tracks
            .into_iter()
            .map(|track| Named {
                id: track.id,
                name: track.title,
                sort_name: track.sort_title,
                aliases: Vec::new(),
            })
            .collect::<Vec<_>>();
        // Tracks outnumber the rest, so their table builds beside the others.
        let (
            tracks,
            releases,
            artists,
            genres,
            release_dates,
            release_created,
            artist_created,
            artist_type,
            credited_flags,
        ) = {
            let parallel = tracks.len() > PARALLEL_CHUNK;
            let (tracks, rest) = beside(
                parallel,
                || Table::new(tracks),
                || {
                    let release_dates = releases
                        .iter()
                        .map(|release| release.release_date.clone())
                        .collect::<Vec<_>>();
                    let release_created = releases
                        .iter()
                        .map(|release| or_missing(release.created))
                        .collect::<Vec<_>>();
                    let releases = Table::new(
                        releases
                            .into_iter()
                            .map(|release| Named {
                                id: release.id,
                                name: release.title,
                                sort_name: release.sort_title,
                                aliases: Vec::new(),
                            })
                            .collect(),
                    );
                    let artist_created = artists
                        .iter()
                        .map(|artist| or_missing(artist.created))
                        .collect::<Vec<_>>();
                    let artist_type = artists
                        .iter()
                        .map(|artist| artist.artist_type)
                        .collect::<Vec<_>>();
                    let credited_flags = artists
                        .iter()
                        .map(|artist| artist.credited)
                        .collect::<Vec<_>>();
                    let artists = Table::new(
                        artists
                            .into_iter()
                            .map(|artist| Named {
                                id: artist.id,
                                name: artist.name,
                                sort_name: artist.sort_name,
                                aliases: artist.scan_aliases,
                            })
                            .collect(),
                    );
                    let genres = Table::new(
                        genres
                            .into_iter()
                            .map(|genre| Named {
                                id: genre.id,
                                name: genre.name,
                                sort_name: None,
                                aliases: genre.aliases,
                            })
                            .collect(),
                    );
                    (
                        releases,
                        artists,
                        genres,
                        release_dates,
                        release_created,
                        artist_created,
                        artist_type,
                        credited_flags,
                    )
                },
            );
            let (
                releases,
                artists,
                genres,
                release_dates,
                release_created,
                artist_created,
                artist_type,
                credited_flags,
            ) = rest;
            (
                tracks,
                releases,
                artists,
                genres,
                release_dates,
                release_created,
                artist_created,
                artist_type,
                credited_flags,
            )
        };
        let pairs = |edges: Vec<(DbId, DbId)>, from: &dyn Fn(DbId) -> Option<u32>, to: &Table| {
            edges
                .into_iter()
                .filter_map(|(source, target)| Some((from(source)?, to.slot(target)?)))
                .collect::<Vec<_>>()
        };
        let library_slots = libraries
            .iter()
            .enumerate()
            .map(|(slot, library)| (library.id, slot as u32))
            .collect::<HashMap<_, _>>();
        let library_public_ids = libraries
            .into_iter()
            .enumerate()
            .map(|(slot, library)| (library.public_id, slot as u32))
            .collect();
        let library_releases = Links::new(
            library_slots.len(),
            pairs(
                library_releases,
                &|library| library_slots.get(&library).copied(),
                &releases,
            ),
        );
        let release_tracks = Links::new(
            releases.len(),
            pairs(release_tracks, &|release| releases.slot(release), &tracks),
        );
        let track_releases = release_tracks.inverted(tracks.len());
        let genre_releases = Links::new(
            genres.len(),
            pairs(genre_releases, &|genre| genres.slot(genre), &releases),
        );
        let release_genres = genre_releases.inverted(releases.len());

        let mut release_credits = Vec::new();
        let mut track_credits = Vec::new();
        for CatalogCredit {
            owner,
            artist,
            credit_type,
            artist_order,
        } in credits
        {
            let Some(artist) = artists.slot(artist) else {
                continue;
            };
            let credit = Credit {
                artist,
                credit_type,
                order: artist_order,
            };
            if let Some(release) = releases.slot(owner) {
                release_credits.push((release, credit));
            } else if let Some(track) = tracks.slot(owner) {
                track_credits.push((track, credit));
            }
        }
        let release_credits = Credits::new(releases.len(), release_credits);
        let track_credits = Credits::new(tracks.len(), track_credits);
        let release_artists = release_credits.artist_links(releases.len());
        let track_artists = track_credits.artist_links(tracks.len());
        let artist_releases = release_artists.inverted(artists.len());
        let artist_tracks = track_artists.inverted(artists.len());

        let durations = &track.duration;
        let mut track_count = Vec::with_capacity(releases.len());
        let mut total_duration = Vec::with_capacity(releases.len());
        for release in 0..releases.len() as u32 {
            let release_tracks = release_tracks.of(release);
            track_count.push(release_tracks.len() as u64);
            total_duration.push(
                release_tracks
                    .iter()
                    .map(|track| durations[*track as usize])
                    .filter(|duration| *duration != MISSING)
                    .fold(0u64, u64::saturating_add),
            );
        }
        let release = ReleaseFields {
            created: release_created,
            date: release_dates
                .iter()
                .map(|date| or_missing(date.as_deref().and_then(super::releases::date_order)))
                .collect(),
            year: release_dates
                .iter()
                .map(|date| {
                    or_missing(lyra_db::releases::release_year(date.as_deref()).map(u64::from))
                })
                .collect(),
            track_count,
            total_duration,
        };
        let artist = ArtistFields {
            created: artist_created,
            artist_type,
            credited: Bits::of(
                artists.len(),
                credited_flags
                    .iter()
                    .enumerate()
                    .filter(|(_, credited)| **credited)
                    .map(|(slot, _)| slot as u32),
            ),
        };
        let lead = |credits: &Credits, owners: usize| {
            (0..owners as u32)
                .map(|owner| {
                    let credits = credits.of(owner);
                    let first = credits.iter().map(|credit| credit.order).min()?;
                    credits
                        .iter()
                        .filter(|credit| credit.order == first)
                        .map(|credit| credit.artist)
                        .min_by_key(|artist| artists.rank[*artist as usize])
                })
                .collect::<Vec<_>>()
        };
        let track_lead = lead(&track_credits, tracks.len());
        let release_lead = lead(&release_credits, releases.len());

        Self {
            stamp,
            tracks,
            releases,
            artists,
            genres,
            track,
            release,
            artist,
            libraries: library_slots,
            library_public_ids,
            library_releases,
            release_tracks,
            track_releases,
            release_credits,
            track_credits,
            release_artists,
            track_artists,
            artist_releases,
            artist_tracks,
            genre_releases,
            release_genres,
            track_lead,
            release_lead,
            scopes: Mutex::default(),
        }
    }

    pub(crate) fn library(&self, id: DbId) -> Option<u32> {
        self.libraries.get(&id).copied()
    }

    /// What a viewer limited to the libraries with these public ids sees, or one who sees every
    /// library without any.
    pub(crate) fn scope<'a>(
        &self,
        libraries: Option<impl IntoIterator<Item = &'a String>>,
    ) -> Arc<Scope> {
        let key = libraries.map(|libraries| {
            let mut slots = libraries
                .into_iter()
                .filter_map(|id| self.library_public_ids.get(id).copied())
                .collect::<Vec<_>>();
            slots.sort_unstable();
            slots.dedup();
            slots
        });
        let mut scopes = self.scopes.lock().expect("index scopes poisoned");
        scopes.uses += 1;
        let uses = scopes.uses;
        match &key {
            None => {
                if let Some(scope) = &scopes.everything {
                    return Arc::clone(scope);
                }
            }
            Some(libraries) => {
                if let Some((scope, used)) = scopes.limited.get_mut(libraries) {
                    *used = uses;
                    let scope = Arc::clone(scope);
                    scopes.evict(libraries);
                    return scope;
                }
            }
        }
        let scope = Arc::new(match &key {
            None => Scope {
                restricted: false,
                releases: Bits::full(self.releases.len()),
                tracks: Bits::full(self.tracks.len()),
                artists: self.artist.credited.clone(),
                genres: self.genre_releases.linked(self.genres.len()),
                cache: Cache::default(),
                libraries: None,
            },
            Some(libraries) => {
                let releases = self
                    .library_releases
                    .reach_slots(libraries.iter().copied(), self.releases.len());
                let tracks = self.release_tracks.reach(&releases, self.tracks.len());
                let mut artists = self.release_artists.reach(&releases, self.artists.len());
                artists.or(&self.track_artists.reach(&tracks, self.artists.len()));
                artists.and(&self.artist.credited);
                Scope {
                    restricted: true,
                    genres: self.release_genres.reach(&releases, self.genres.len()),
                    releases,
                    tracks,
                    artists,
                    cache: Cache::default(),
                    libraries: Some(libraries.clone()),
                }
            }
        });
        match key {
            None => scopes.everything = Some(Arc::clone(&scope)),
            Some(libraries) => {
                scopes
                    .limited
                    .insert(libraries.clone(), (Arc::clone(&scope), uses));
                scopes.evict(&libraries);
            }
        }
        scope
    }

    /// Keeps the retained scopes within their budget after `scope`'s cache grew.
    pub(crate) fn trim_scopes(&self, scope: &Scope) {
        if let Some(libraries) = &scope.libraries {
            self.scopes
                .lock()
                .expect("index scopes poisoned")
                .evict(libraries);
        }
    }

    /// Limits the scopes the snapshot keeps to `kept` holding `bytes_kept` together.
    #[cfg(test)]
    pub(crate) fn limit_scopes(&self, kept: usize, bytes_kept: usize) {
        let mut scopes = self.scopes.lock().expect("index scopes poisoned");
        scopes.kept = kept;
        scopes.bytes_kept = bytes_kept;
    }

    /// What the retained scopes limited to some libraries hold.
    #[cfg(test)]
    pub(crate) fn limited_scope_bytes(&self) -> usize {
        self.scopes.lock().expect("index scopes poisoned").bytes()
    }

    /// How many scopes limited to some libraries the snapshot keeps.
    #[cfg(test)]
    pub(crate) fn limited_scopes(&self) -> usize {
        self.scopes
            .lock()
            .expect("index scopes poisoned")
            .limited
            .len()
    }

    /// The heap the snapshot holds, roughly, including the columns and orders built so far.
    pub(crate) fn memory(&self) -> usize {
        let scopes = {
            let scopes = self.scopes.lock().expect("index scopes poisoned");
            scopes
                .limited
                .values()
                .map(|(scope, _)| scope)
                .chain(&scopes.everything)
                .map(|scope| scope.memory())
                .sum::<usize>()
        };
        let fields = (self.track.created.capacity()
            + self.track.year.capacity()
            + self.track.duration.capacity()
            + self.track.disc.capacity()
            + self.track.track.capacity()
            + self.release.created.capacity()
            + self.release.date.capacity()
            + self.release.year.capacity()
            + self.release.track_count.capacity()
            + self.release.total_duration.capacity()
            + self.artist.created.capacity())
            * 8;
        scopes
            + self.tracks.memory()
            + self.releases.memory()
            + self.artists.memory()
            + self.genres.memory()
            + fields
            + self.library_releases.memory()
            + self.release_tracks.memory()
            + self.track_releases.memory()
            + self.release_credits.memory()
            + self.track_credits.memory()
            + self.release_artists.memory()
            + self.track_artists.memory()
            + self.artist_releases.memory()
            + self.artist_tracks.memory()
            + self.genre_releases.memory()
            + self.release_genres.memory()
            + (self.track_lead.capacity() + self.release_lead.capacity()) * 8
    }
}

/// The links between kinds as one scope sees them. Every hop leaves out releases and tracks the
/// scope cannot see, so nothing reached through it comes from a library the viewer lacks; the
/// raw link tables stay inside this module.
#[derive(Clone, Copy)]
pub(crate) struct Scoped<'a> {
    index: &'a Snapshot,
    scope: &'a Scope,
}

impl<'a> Scoped<'a> {
    pub(crate) fn new(index: &'a Snapshot, scope: &'a Scope) -> Self {
        Self { index, scope }
    }

    fn release_seen(&self, release: u32) -> bool {
        self.scope.releases.contains(release)
    }

    fn track_seen(&self, track: u32) -> bool {
        self.scope.tracks.contains(track)
    }

    fn seen(&self, mut slots: Bits, seen: &Bits) -> Bits {
        if self.scope.restricted {
            slots.and(seen);
        }
        slots
    }

    /// Whether the scope sees the release `id`.
    pub(crate) fn sees_release(&self, id: DbId) -> bool {
        self.index
            .releases
            .slot(id)
            .is_some_and(|release| self.release_seen(release))
    }

    /// The releases among `releases` the scope sees.
    pub(crate) fn releases_seen(&self, releases: Bits) -> Bits {
        self.seen(releases, &self.scope.releases)
    }

    /// The artists among `artists` the scope sees.
    pub(crate) fn artists_seen(&self, artists: Bits) -> Bits {
        self.seen(artists, &self.scope.artists)
    }

    /// The releases of `library`, none when the scope may not see that library, whatever it
    /// shares with libraries the scope does see.
    pub(crate) fn releases_of_library(&self, library: DbId) -> Bits {
        let index = self.index;
        let library = index.library(library).filter(|library| {
            self.scope
                .libraries
                .as_ref()
                .is_none_or(|libraries| libraries.contains(library))
        });
        self.seen(
            index
                .library_releases
                .reach_slots(library, index.releases.len()),
            &self.scope.releases,
        )
    }

    pub(crate) fn releases_of_genres(&self, genres: &Bits) -> Bits {
        self.seen(
            self.index
                .genre_releases
                .reach(genres, self.index.releases.len()),
            &self.scope.releases,
        )
    }

    pub(crate) fn releases_of_genre(&self, genre: u32) -> impl Iterator<Item = u32> + 'a {
        let scoped = *self;
        self.index
            .genre_releases
            .of(genre)
            .iter()
            .copied()
            .filter(move |release| scoped.release_seen(*release))
    }

    pub(crate) fn genres_of_releases(&self, releases: &Bits) -> Bits {
        let releases = self.seen(releases.clone(), &self.scope.releases);
        self.index
            .release_genres
            .reach(&releases, self.index.genres.len())
    }

    pub(crate) fn tracks_of_releases(&self, releases: &Bits) -> Bits {
        let releases = self.seen(releases.clone(), &self.scope.releases);
        self.index
            .release_tracks
            .reach(&releases, self.index.tracks.len())
    }

    /// The tracks of `release`, none when the scope cannot see it.
    pub(crate) fn tracks_of_release(&self, release: u32) -> &'a [u32] {
        if self.release_seen(release) {
            self.index.release_tracks.of(release)
        } else {
            &[]
        }
    }

    pub(crate) fn releases_of_tracks(&self, tracks: &Bits) -> Bits {
        self.seen(
            self.index
                .track_releases
                .reach(tracks, self.index.releases.len()),
            &self.scope.releases,
        )
    }

    pub(crate) fn releases_of_track(&self, track: u32) -> impl Iterator<Item = u32> + 'a {
        let scoped = *self;
        self.index
            .track_releases
            .of(track)
            .iter()
            .copied()
            .filter(move |release| scoped.release_seen(*release))
    }

    pub(crate) fn artists_of_releases(&self, releases: &Bits) -> Bits {
        let releases = self.seen(releases.clone(), &self.scope.releases);
        self.index
            .release_artists
            .reach(&releases, self.index.artists.len())
    }

    pub(crate) fn artists_of_tracks(&self, tracks: &Bits) -> Bits {
        let tracks = self.seen(tracks.clone(), &self.scope.tracks);
        self.index
            .track_artists
            .reach(&tracks, self.index.artists.len())
    }

    pub(crate) fn releases_of_artists(&self, artists: &Bits) -> Bits {
        self.seen(
            self.index
                .artist_releases
                .reach(artists, self.index.releases.len()),
            &self.scope.releases,
        )
    }

    pub(crate) fn releases_of_artist(&self, artist: u32) -> impl Iterator<Item = u32> + 'a {
        let scoped = *self;
        self.index
            .artist_releases
            .of(artist)
            .iter()
            .copied()
            .filter(move |release| scoped.release_seen(*release))
    }

    pub(crate) fn tracks_of_artists(&self, artists: &Bits) -> Bits {
        self.seen(
            self.index
                .artist_tracks
                .reach(artists, self.index.tracks.len()),
            &self.scope.tracks,
        )
    }

    pub(crate) fn tracks_of_artist(&self, artist: u32) -> impl Iterator<Item = u32> + 'a {
        let scoped = *self;
        self.index
            .artist_tracks
            .of(artist)
            .iter()
            .copied()
            .filter(move |track| scoped.track_seen(*track))
    }

    /// The credits on `release`, none when the scope cannot see it.
    pub(crate) fn release_credits(&self, release: u32) -> &'a [Credit] {
        if self.release_seen(release) {
            self.index.release_credits.of(release)
        } else {
            &[]
        }
    }

    /// The credits on `track`, none when the scope cannot see it.
    pub(crate) fn track_credits(&self, track: u32) -> &'a [Credit] {
        if self.track_seen(track) {
            self.index.track_credits.of(track)
        } else {
            &[]
        }
    }

    /// The first artist credited on `track` itself.
    pub(crate) fn track_lead(&self, track: u32) -> Option<u32> {
        self.track_seen(track)
            .then(|| self.index.track_lead[track as usize])
            .flatten()
    }

    /// The first artist credited on `release` itself.
    pub(crate) fn release_lead(&self, release: u32) -> Option<u32> {
        self.release_seen(release)
            .then(|| self.index.release_lead[release as usize])
            .flatten()
    }
}

fn or_missing(value: Option<u64>) -> u64 {
    value.unwrap_or(MISSING)
}

/// The snapshot a query reads, kept per generation for the server's database.
#[derive(Default)]
pub(crate) struct IndexCache {
    current: crate::services::state::SwapHandle<Option<Arc<Snapshot>>>,
    building: Mutex<()>,
}

/// Builds the server database's snapshot off the async workers, so the first query finds it.
pub(crate) fn warm() {
    tokio::spawn(async {
        let db = crate::services::STATE.db.read().await;
        let built = tokio::task::spawn_blocking(move || snapshot(&db).map(drop)).await;
        if let Ok(Err(error)) = built {
            tracing::warn!(error = %error, "building the catalog index failed");
        }
    });
}

/// The snapshot of `db`, which the caller holds a guard on. The server's database keeps its
/// snapshot until a write changes it; any other database, such as a test's, builds one per call.
pub(crate) fn snapshot(db: &DbAny) -> anyhow::Result<Arc<Snapshot>> {
    let Some(state) = crate::services::STATE.get() else {
        return Ok(Arc::new(Snapshot::build(db)?));
    };
    let shared = state.db.get();
    if !shared.holds(db) {
        return Ok(Arc::new(Snapshot::build(db)?));
    }
    let generation = state.generation();
    let cache = &generation.catalog_index;
    let (serial, changes) = shared.stamp();
    // A write guard holding mutable access is open; what it sees may change before it drops.
    if !changes.is_multiple_of(2) {
        return Ok(Arc::new(Snapshot::build(db)?));
    }
    let stamp = Some((serial, changes));
    let fresh = |cache: &IndexCache| {
        cache
            .current
            .get()
            .filter(|snapshot| snapshot.stamp == stamp)
    };
    if let Some(snapshot) = fresh(cache) {
        return Ok(snapshot);
    }
    let _building = cache.building.lock().expect("index build poisoned");
    if let Some(snapshot) = fresh(cache) {
        return Ok(snapshot);
    }
    let started = std::time::Instant::now();
    let snapshot = Arc::new(Snapshot::from_catalog(lyra_db::catalog::read(db)?, stamp));
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        tracks = snapshot.tracks.len(),
        memory_bytes = snapshot.memory(),
        "rebuilt catalog index"
    );
    cache.current.replace(Some(Arc::clone(&snapshot)));
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{
        STATE,
        catalog::{
            self,
            Viewer,
            releases::Releases,
        },
        testing::{
            init_default_test_state,
            runtime_test_lock,
            user_principal,
        },
    };
    use lyra_db::roots::{
        self,
        Root,
    };
    use lyra_db::{
        fixtures::{
            connect,
            insert_release,
        },
        libraries::AccessKind,
        test_db::{
            insert_library,
            insert_user,
        },
    };

    #[test]
    fn a_name_that_folds_away_matches_nothing_and_builds_no_key_order() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        for title in ["'", " ", "Hunter"] {
            insert_release(&mut db, title)?;
        }
        let index = Snapshot::build(&db)?;
        let releases = &index.releases;
        for name in ["", "'", "  "] {
            assert!(releases.named(name).is_empty(), "{name:?}");
        }
        assert!(releases.by_key.get().is_none());
        assert_eq!(releases.named("HUNTER").len(), 1);
        assert!(releases.by_key.get().is_some());
        Ok(())
    }

    fn titles(db: &DbAny, viewer: &Viewer) -> Vec<String> {
        let query = catalog::Query::<Releases>::new(Default::default());
        catalog::page(db, viewer, &query, 0, None)
            .expect("the query runs")
            .items
            .into_iter()
            .map(|release| release.release_title)
            .collect()
    }

    #[tokio::test]
    async fn the_server_database_keeps_its_snapshot_until_a_write() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let release = insert_release(&mut *STATE.db.write().await, "Before")?;

        let first = snapshot(&*STATE.db.read().await)?;
        let again = snapshot(&*STATE.db.read().await)?;
        assert!(Arc::ptr_eq(&first, &again));

        {
            let mut db = STATE.db.write().await;
            let mut stored = lyra_db::releases::get_by_id(&db, release)?.expect("release exists");
            stored.release_title = "After".to_string();
            lyra_db::releases::update(&mut db, &stored)?;
        }
        let db = STATE.db.read().await;
        let rebuilt = snapshot(&db)?;
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert_eq!(titles(&db, &Viewer::System), ["After"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_query_inside_a_write_sees_the_writes_before_it() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let mut db = STATE.db.write().await;
        insert_release(&mut db, "One")?;
        assert_eq!(titles(&db, &Viewer::System), ["One"]);
        insert_release(&mut db, "Two")?;
        assert_eq!(titles(&db, &Viewer::System), ["One", "Two"]);
        Ok(())
    }

    #[tokio::test]
    async fn writes_through_a_held_mutable_reference_reach_later_queries() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        {
            let mut guard = STATE.db.write().await;
            let db: &mut DbAny = &mut guard;
            insert_release(db, "One")?;
            assert_eq!(titles(db, &Viewer::System), ["One"]);
            insert_release(db, "Two")?;
            assert_eq!(titles(db, &Viewer::System), ["One", "Two"]);
        }
        let db = STATE.db.read().await;
        assert_eq!(titles(&db, &Viewer::System), ["One", "Two"]);
        let cached = snapshot(&db)?;
        assert!(
            Arc::ptr_eq(&cached, &snapshot(&db)?),
            "reads share one snapshot"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_rolled_back_write_leaves_queries_correct() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        insert_release(&mut *STATE.db.write().await, "Kept")?;
        let before = snapshot(&*STATE.db.read().await)?;
        {
            let mut db = STATE.db.write().await;
            let _ = db.transaction_mut(|t| -> anyhow::Result<()> {
                let release = lyra_db::releases::Release {
                    db_id: None,
                    id: "rolled-back".to_string(),
                    release_title: "Rolled Back".to_string(),
                    sort_title: None,
                    release_type: None,
                    release_date: None,
                    media_formats: None,
                    barcode: None,
                    locked: None,
                    created_at_ms: None,
                    file_created_at_ms: None,
                };
                let id = lyra_db::DbAccess::exec_mut(
                    t,
                    agdb::QueryBuilder::insert().element(&release).query(),
                )?
                .ids()[0];
                roots::link(t, Root::Releases, id)?;
                anyhow::bail!("rolled back")
            });
        }
        let db = STATE.db.read().await;
        assert!(!Arc::ptr_eq(&before, &snapshot(&db)?));
        assert_eq!(titles(&db, &Viewer::System), ["Kept"]);
        Ok(())
    }

    #[tokio::test]
    async fn writes_outside_the_catalog_keep_the_snapshot() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        insert_release(&mut *STATE.db.write().await, "Kept")?;
        let before = snapshot(&*STATE.db.read().await)?;
        insert_user(&mut *STATE.db.write_outside_catalog().await, "listener")?;
        assert!(Arc::ptr_eq(&before, &snapshot(&*STATE.db.read().await)?));
        Ok(())
    }

    #[tokio::test]
    async fn a_revoked_grant_hides_the_library_from_the_next_request() -> anyhow::Result<()> {
        let _guard = runtime_test_lock().await;
        init_default_test_state()?;
        let (user, library) = {
            let mut db = STATE.db.write().await;
            let user = insert_user(&mut db, "listener")?;
            let library = insert_library(&mut db, "Shared", "/index/shared")?;
            let release = insert_release(&mut db, "Shared Release")?;
            connect(&mut db, library, release)?;
            let hidden = insert_library(&mut db, "Hidden", "/index/hidden")?;
            let release = insert_release(&mut db, "Hidden Release")?;
            connect(&mut db, hidden, release)?;
            lyra_db::libraries::grant_access(&mut db, user, library, AccessKind::ReadWrite)?;
            (user, library)
        };
        let viewer = |principal| {
            let shared = STATE.db.get();
            let db = shared.try_read().expect("no writer");
            Viewer::user(&*db, principal).expect("a viewer")
        };

        let granted = viewer(user_principal(user).await?);
        assert_eq!(
            titles(&*STATE.db.read().await, &granted),
            ["Shared Release"]
        );
        lyra_db::libraries::revoke_access(&mut *STATE.db.write().await, user, library)?;
        let revoked = viewer(user_principal(user).await?);
        assert!(titles(&*STATE.db.read().await, &revoked).is_empty());
        Ok(())
    }

    #[test]
    fn scopes_past_the_cap_are_evicted_and_rebuilt_alike() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let mut libraries = Vec::new();
        for index in 0..7 {
            let library = insert_library(&mut db, &format!("L{index}"), &format!("/cap/{index}"))?;
            let release = insert_release(&mut db, &format!("R{index}"))?;
            connect(&mut db, library, release)?;
            libraries.push(
                lyra_db::libraries::get_by_id(&db, library)?
                    .expect("library exists")
                    .id,
            );
        }
        let index = Snapshot::build(&db)?;
        let set = |bits: u32| {
            libraries
                .iter()
                .enumerate()
                .filter(|(place, _)| bits & (1 << place) != 0)
                .map(|(_, id)| id)
                .collect::<Vec<_>>()
        };
        let held = index.scope(Some(set(0b101)));
        let releases = |scope: &Scope| scope.releases.iter().collect::<Vec<_>>();
        let first = releases(&held);
        for bits in 0..1 << libraries.len() {
            index.scope(Some(set(bits)));
            assert!(index.limited_scopes() <= SCOPES_KEPT);
        }
        assert_eq!(releases(&held), first, "a held scope outlives eviction");
        let rebuilt = index.scope(Some(set(0b101)));
        assert!(!Arc::ptr_eq(&held, &rebuilt), "the scope was evicted");
        assert_eq!(releases(&rebuilt), first);
        let everything = index.scope(None::<[&String; 0]>);
        assert!(Arc::ptr_eq(&everything, &index.scope(None::<[&String; 0]>)));
        Ok(())
    }

    /// A catalog of seven libraries, each with a release of three tracks, and a viewer for each
    /// set of them.
    fn libraries_fixture() -> anyhow::Result<(DbAny, Vec<Viewer>)> {
        use crate::services::auth::Principal;
        let mut db = lyra_db::test_db::new_test_db()?;
        let mut libraries = Vec::new();
        for index in 0..7 {
            let library =
                insert_library(&mut db, &format!("L{index}"), &format!("/budget/{index}"))?;
            let release = insert_release(&mut db, &format!("R{index}"))?;
            connect(&mut db, library, release)?;
            for track in 0..3 {
                let track = lyra_db::fixtures::insert_track(&mut db, &format!("T{index}{track}"))?;
                connect(&mut db, release, track)?;
            }
            libraries.push(
                lyra_db::libraries::get_by_id(&db, library)?
                    .expect("library exists")
                    .id,
            );
        }
        let user = insert_user(&mut db, "budget")?;
        let viewers = (1..1u32 << libraries.len())
            .map(|bits| {
                let set = libraries
                    .iter()
                    .enumerate()
                    .filter(|(place, _)| bits & (1 << place) != 0)
                    .map(|(_, id)| id.clone())
                    .collect();
                Viewer::User {
                    principal: Principal::for_user(&db, user, Vec::new(), set),
                    user_db_id: user,
                    playlists: Default::default(),
                }
            })
            .collect();
        Ok((db, viewers))
    }

    fn by_release_title(index: &Arc<Snapshot>, db: &DbAny, viewer: &Viewer) -> Vec<DbId> {
        let mut query = catalog::Query::<catalog::tracks::Tracks>::new(Default::default());
        query.sort = vec![(
            catalog::tracks::TrackKey::ReleaseTitle,
            catalog::Direction::Ascending,
        )];
        let cx = catalog::pipeline::Context::with(Arc::clone(index), db, viewer);
        catalog::pipeline::leading_in(&cx, &query, None).expect("the query runs")
    }

    #[test]
    fn concurrent_queries_leave_the_scopes_within_their_budget() -> anyhow::Result<()> {
        let (db, viewers) = libraries_fixture()?;
        let unlimited = Arc::new(Snapshot::build(&db)?);
        let expected = viewers
            .iter()
            .map(|viewer| by_release_title(&unlimited, &db, viewer))
            .collect::<Vec<_>>();
        let probe = Arc::new(Snapshot::build(&db)?);
        by_release_title(&probe, &db, &viewers[0]);
        let one = probe.limited_scope_bytes();

        let index = Arc::new(Snapshot::build(&db)?);
        index.limit_scopes(SCOPES_KEPT, 3 * one);
        std::thread::scope(|threads| {
            for chunk in viewers.chunks(16).zip(expected.chunks(16)) {
                let (index, db) = (&index, &db);
                threads.spawn(move || {
                    for (viewer, expected) in chunk.0.iter().zip(chunk.1) {
                        assert_eq!(&by_release_title(index, db, viewer), expected);
                    }
                });
            }
        });
        assert!(index.limited_scope_bytes() <= 3 * one);
        assert!(index.limited_scopes() >= 1);
        Ok(())
    }

    #[test]
    fn a_scope_past_the_budget_on_its_own_is_not_kept() -> anyhow::Result<()> {
        let (db, viewers) = libraries_fixture()?;
        let unlimited = Arc::new(Snapshot::build(&db)?);
        // Room for the scope itself but not for the columns and orders a sort adds to it.
        let probe = Snapshot::build(&db)?;
        let Viewer::User { principal, .. } = &viewers[0] else {
            unreachable!("every viewer is a user")
        };
        probe.scope(Some(&principal.accessible_library_ids));
        let bare = probe.limited_scope_bytes();
        let index = Arc::new(Snapshot::build(&db)?);
        index.limit_scopes(SCOPES_KEPT, bare);
        by_release_title(&index, &db, &viewers[1]);
        assert_eq!(index.limited_scopes(), 0, "a grown scope goes");
        index.scope(Some(&principal.accessible_library_ids));
        assert_eq!(index.limited_scopes(), 1, "a bare scope fits");
        for viewer in &viewers[..5] {
            assert_eq!(
                by_release_title(&index, &db, viewer),
                by_release_title(&unlimited, &db, viewer)
            );
            assert_eq!(index.limited_scopes(), 0);
        }
        Ok(())
    }

    #[test]
    fn a_viewer_sees_only_the_releases_and_tracks_of_its_libraries() -> anyhow::Result<()> {
        let mut db = lyra_db::test_db::new_test_db()?;
        let shown = insert_library(&mut db, "Shown", "/index/shown")?;
        let hidden = insert_library(&mut db, "Hidden", "/index/hidden")?;
        let shown_id = lyra_db::libraries::get_by_id(&db, shown)?
            .expect("library exists")
            .id;
        let both = insert_release(&mut db, "Both")?;
        let only_hidden = insert_release(&mut db, "Only Hidden")?;
        connect(&mut db, shown, both)?;
        connect(&mut db, hidden, both)?;
        connect(&mut db, hidden, only_hidden)?;
        let track = lyra_db::fixtures::insert_track(&mut db, "Hidden Track")?;
        connect(&mut db, only_hidden, track)?;
        let loose = lyra_db::fixtures::insert_track(&mut db, "Loose Track")?;

        let index = Snapshot::build(&db)?;
        let scope = index.scope(Some([&shown_id]));
        assert!(scope.restricted);
        let releases = scope
            .releases
            .iter()
            .map(|slot| index.releases.id(slot))
            .collect::<Vec<_>>();
        assert_eq!(releases, [both]);
        assert!(scope.tracks.is_empty());
        let everything = index.scope(None::<[&String; 0]>);
        assert!(
            everything
                .tracks
                .contains(index.tracks.slot(loose).expect("indexed"))
        );
        assert!(
            everything
                .tracks
                .contains(index.tracks.slot(track).expect("indexed"))
        );
        assert!(index.scope(Some(&[] as &[String])).releases.is_empty());
        Ok(())
    }
}
