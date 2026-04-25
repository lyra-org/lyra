# agdb Reference (Embedded Database)

This guide documents how the `agdb` crate actually behaves in this repo. It is
intended for contributors and focuses on precise semantics, edge cases, and
code-backed behavior so readers can avoid wrong assumptions.

The [official agdb documentation](https://agdb.agnesoft.com/docs) is the
authoritative source for agdb behavior and APIs. This page is supplemental
project-local guidance for using agdb in Lyra.

## Scope and Mental Model

- `agdb` is a persistent, embedded graph database with object queries (no SQL).
- The core database lives under `agdb/` (Rust crate). Server/client layers use
  the same query model but are out of scope here.

## Graph Model and IDs

- The database is a directed graph of nodes and edges. Both are "elements".
- Element IDs are signed 64-bit integers (`DbId`):
  - Positive IDs are nodes.
  - Negative IDs are edges.
  - `0` is invalid and means "no element".
- IDs are **reused** after deletions. Do not rely on monotonic IDs or ID order
  to infer insertion time.
- Nodes and edges share the same absolute index internally. You cannot have
  node `3` and edge `-3` at the same time.
- Each node maintains **dual adjacency lists** (`from`/`from_meta` for outgoing
  edges, `to`/`to_meta` for incoming edges). Both `search().from()` and
  `search().to()` traverse via these lists and are **O(neighbors)**, not
  O(all_edges). Reverse BFS with `to()` is equally efficient as forward BFS.
- Removing a node also removes all its incoming/outgoing edges and their values.
- Orphaned edges are **not possible** via normal queries:
  - Inserted edges must reference existing nodes.
  - When a node is deleted, all attached edges are deleted as part of the same operation.

## Storage and Persistence

- Storage is a **single file** database with a write-ahead log (WAL):
  - WAL filename is `.{db_filename}` (same directory).
  - WAL records the **previous bytes** before writes; on load it is applied to
    restore a consistent state after crashes.
- Storage fragments over time. `DbImpl` calls `storage.shrink_to_fit()` on drop.
  You can call `Db::optimize_storage()` explicitly to defragment.
- Storage variants:
  - `Db` (default): file + in-memory buffer; reads are in-memory, writes sync to
    both (fast reads, persistent).
  - `DbFile`: file-only (low memory, slower reads).
  - `DbMemory`: in-memory only, **no persistence** unless you `backup()` it.
    Note: `DbMemory::new()` loads from file if it exists.
- `Db::backup(path)` copies the current storage to `path`.
- `Db::copy(path)` returns a new `Db` with copied storage at `path`.
- `Db::rename(path)` renames the underlying file(s).

## Values and Types

- Keys and values are `DbValue` (type-strict).
- Supported `DbValue` variants:
  - `I64`, `U64`, `F64` (wrapped as `DbF64`), `String`, `Bytes`
  - `VecI64`, `VecU64`, `VecF64`, `VecString`
- Keys are also `DbValue` (not just strings). Type matters: `"1"` != `1`.
- Small-value optimization:
  - Values up to 15 bytes are stored inline in a 16-byte cell.
  - Larger values are stored separately with an index.
- Numeric conversions are **lossless** and may fail if out of range.
- `DbF64` ordering uses `total_cmp` (NaN is ordered, not equal).
- Bool conversions:
  - `bool` is stored as `u64` (`0` or `1`).
  - `Vec<bool>` is stored as `Vec<u8>` (`0` or `1`).
  - Conversions back to bool are permissive: non-zero numeric is `true`,
    string `"true"` or `"1"` is `true`.

## Transactions and Concurrency

- Every query runs as a transaction.
- Explicit transactions use closures:
  - `Db::transaction(|t| ...)` for read-only.
  - `Db::transaction_mut(|t| ...)` for read/write.
- No manual commit/rollback. If the closure returns `Ok`, it commits; `Err`
  rolls back.
- Nested transactions are not supported.
- Project convention: wrap related DB interactions (especially writes) in a
  single explicit transaction (`transaction` / `transaction_mut`) so any error
  rolls back the whole unit of work.
- Concurrency rule (like Rust borrowing):
  - Unlimited concurrent reads, or exactly one write.
  - Use `RwLock` for multi-threaded access.

## Query Results

- All queries return `QueryResult { result: i64, elements: Vec<DbElement> }`.
- `DbElement` has `id`, `from`, `to`, and `values`:
  - `from`/`to` are set only for edges.
- `result` meaning depends on query:
  - Inserts: count of inserted/updated elements (or key-values for insert values).
  - Removes: negative count of removed items.
  - Select/Search: number of returned elements.
- `QueryResult::ids()` extracts the element IDs for reuse in other queries.
- **`search()` vs `select().search()`**: A bare `search()` query returns
  elements with only structural fields (`id`, `from`, `to`) — the `values`
  list is **empty**. To get stored key-values (needed for type checks like
  `db_element_id` or any application data), use `select().search()` or
  `select().elements::<T>().search()`. If you need to inspect element values
  after a search, always use the `select` form; do not rely on `values` from
  a bare `search()`.

## Aliases

- Aliases are a **one-to-one** mapping between `String` and `DbId`.
- Intended for **nodes only**. Avoid aliasing edges.
- Inserting an alias replaces any existing alias for that element, and if the
  alias is already in use it is moved to the new element.
- `InsertAliasesQuery` **does not support search** IDs (it returns an error).
- `SelectAliasesQuery`:
  - With explicit IDs: all must have aliases (otherwise error).
  - With search: only elements that have aliases are returned (no error).
- Removing a node removes its alias.

## Indexes

- Indexes are per **key** (`DbValue`) and cover the entire DB.
- Creating an index scans **existing** data and indexes it.
- `insert().index(key)` errors if the index already exists.
- `remove().index(key)` is a no-op if the index does not exist.
- Index search is **exact match** (key + value). Comparators are ignored.

## Insert Queries

### Insert Nodes

- If `ids` is provided:
  - All IDs must exist and must be nodes.
  - `values` length must match `ids` (unless `Single`).
  - Updates existing nodes; inserts/replaces key-values.
  - Optional aliases are applied to the matched IDs.
- If `ids` is empty:
  - New nodes are inserted, **unless** an alias already exists, in which case
    that existing node is updated instead of inserting a new node.
  - `count` defaults to max(`count`, alias count) unless `values` is multi.
  - Aliases must be non-empty and `aliases.len() <= values.len()` if multi.
- IDs are reused. Do not infer insertion order from ID.

### Insert Edges

- `from` and `to` must resolve to **existing nodes**.
- If `ids` is provided:
  - All IDs must exist and must be edges.
  - `from`/`to` are ignored (values are updated only).
- If `ids` is empty:
  - Pairwise insert if `from.len() == to.len()` and `each == false`.
  - Cross-product insert if `each == true` or lengths differ.
  - **Empty from/to is an error** (values length mismatch), not a no-op.
- If `from`/`to` come from a search, edges are silently filtered out (nodes only).

### Insert Values

- Applies to nodes **and** edges (values live on any element).
- For `QueryId::Id(0)` or missing alias:
  - Inserts a **new node** (with alias if provided).
- For existing IDs:
  - Replaces existing key-values with the same key (upsert).
- `result` counts inserted/updated **key-value pairs**, not elements.
- `elements` contains only **newly created nodes**.

### Insert Aliases

- Requires explicit ID list (no search).
- IDs must exist; aliases must be non-empty.
- Length of IDs and aliases must match.

### Insert Index

- Creates a key index; errors if the index already exists.

## Remove Queries

### Remove Elements

- Missing IDs/aliases are **not errors**.
- Removing nodes also removes their edges and edge values.
- `result` is the negative count of elements explicitly removed.
  Cascaded edge removals are **not** included in `result`.

### Remove Values

- IDs must exist (otherwise error).
- Missing keys are **not** errors.
- `result` is the negative count of removed key-value pairs.

### Remove Aliases

- Missing aliases are **not** errors.

### Remove Index

- Missing index is **not** an error.
- `result` is negative count of values removed from the index.

## Select Queries

### Select Values / IDs

- With explicit IDs:
  - All IDs must exist.
  - If keys are specified, every ID must have **all keys** (otherwise error).
- With search:
  - Missing keys are **allowed** (values may be incomplete).
  - Use `where_().keys(...)` if you need to guarantee presence.
- Empty key list returns all values.

### Select Keys

- Returns keys only; values are default `I64(0)` placeholders.

### Select Aliases

- Explicit IDs: every ID must have an alias.
- Search: only elements with aliases are returned.

### Select Edge Count

- `edge_count` counts in+out edges.
- Self-referential edges count twice.

### Select Indexes

- Returns one element with ID `0` and properties:
  `key -> count_of_indexed_values`.

### Select Node Count

- Returns one element with ID `0` and property `node_count`.

## Search Queries

### Algorithms

- **BreadthFirst** (default): level-by-level, edges first, newest to oldest.
- **DepthFirst**: follows one path until dead end, then backtracks.
- **Elements**: full scan of all elements (slow).
- **Index**: bypasses graph, uses key/value index (exact match only).

### Order, Limit, Offset

- BFS/DFS order is by **most recently connected**, not by ID.
- Elements search returns elements in **ascending absolute index**.
  If an edge exists for index `i`, it is returned as `-i` instead of `i`.
- `order_by` sorts by key values:
  - Missing keys sort **after** present keys.
  - Sorting uses `DbValue` ordering (type strict).
- For non-path BFS/DFS search **without** `order_by`, `offset`/`limit` are
  applied during traversal and can short-circuit once enough matches are seen
  (effectively after `offset + limit`, when `limit` is set).
- For `order_by` or path search, the search runs to completion **before**
  `offset`/`limit` are applied. Avoid large scans if you can.
- When using `order_by`, ensure `offset <= result count`; slicing is not guarded.

### Origin/Destination

- `from` sets the search origin (outgoing direction).
- `to` sets reverse search (incoming direction).
- `from` + `to` uses **A*** path search.
- If `from` or `to` does not resolve to an existing element, the query errors.
- If `from == to`, path search returns an empty path.

### Conditions (Where)

- Conditions are type-strict; no automatic coercion.
- `Contains`, `StartsWith`, `EndsWith` support strings and vector types:
  - `Contains(Vec)` requires all values to be present.
  - `StartsWith(VecString)` and `EndsWith(VecString)` use concatenation.
  - Integral `Contains` is only defined for vectors (not scalars).
- `distance` counts **every element**, including edges. Neighboring nodes
  are at distance `2`.
- `beyond` / `not_beyond` can stop or continue traversal past elements.
  The origin element is also subject to these conditions: if the origin does
  not match a `beyond()` condition, traversal stops at distance 0.
- `ids()` condition **does not support search queries**; if you pass a search,
  it is silently ignored (empty list).

### Index Search

- Uses the first KeyValue condition only (exact match).
- Ignores `limit`, `offset`, `order_by`, and graph traversal settings.

## Type System and Derives

- `DbType`:
  - Field names become keys (String).
  - `Option<T>` fields with `None` are omitted on insert.
  - `db_id: Option<DbId>` (or `Option<QueryId>`) enables upserts via
    `insert().element(s)` and round-tripping from results.
- `DbElement`:
  - Same as `DbType`, but also inserts `db_element_id = "TypeName"`.
  - `select().element::<T>()` and `select().elements::<T>()` inject this
    condition automatically when using `search()`. This is an **inclusion-only**
    filter — it controls which elements appear in results but does **not** block
    traversal. BFS/DFS continues through non-matching element types normally.
  - `select().element::<T>().ids(...)` / `select().elements::<T>().ids(...)`
    do **not** inject `db_element_id` filtering.
  - If the ID set is mixed-type, `try_into::<T>()` / `try_into::<Vec<T>>()`
    can fail with missing-key errors (for example, `Key 'artist_name' not found`).
    Filter IDs by `db_element_id` first when type purity is not guaranteed.
- `DbTypeMarker`: enables vectorized custom types in `DbType`.
- `DbSerialize` and `DbValue`: enable custom value storage (e.g., enums).

## Practical Patterns

- Create a root node alias (`root`) to anchor discovery of graph structure.
- Use indexes for attributes that do not map well to graph traversal.
- Use `where_().node()` / `where_().edge()` if your search requires a specific
  element type; search returns both nodes and edges by default.
- Wrap related DB interactions in `transaction_mut` so failures rollback cleanly
  and graph/value changes stay in sync.
- Do not assume ID monotonicity or creation order; IDs are reused.

## Example Queries (Rust)

```rust
use agdb::{Db, DbId, DbType, QueryBuilder, Comparison, DbKeyOrder};

// Create or load a database file.
let mut db = Db::new("example.agdb")?;

// Insert a root node alias.
db.exec_mut(QueryBuilder::insert().nodes().aliases("root").query())?;

// Insert nodes with values (using DbType derive).
#[derive(Debug, DbType)]
struct User {
    db_id: Option<DbId>,
    username: String,
    age: u64,
}

let users = vec![
    User { db_id: None, username: "alice".into(), age: 30 },
    User { db_id: None, username: "bob".into(), age: 25 },
];

let inserted = db.exec_mut(QueryBuilder::insert().nodes().values(&users).query())?;

// Connect root -> user nodes (pairwise).
db.exec_mut(
    QueryBuilder::insert()
        .edges()
        .from("root")
        .to(inserted)
        .query(),
)?;

// Upsert values by id (replaces matching keys).
db.exec_mut(
    QueryBuilder::insert()
        .values_uniform([("active", 1_u64).into()])
        .ids(1)
        .query(),
)?;

// Select a single user by id and convert to type.
let user: User = db.exec(QueryBuilder::select().elements::<User>().ids(1).query())?.try_into()?;

// Graph search: find users connected to root with age < 30.
let young_users: Vec<User> = db.exec(
    QueryBuilder::select()
        .elements::<User>()
        .search()
        .from("root")
        .where_()
        .key("age")
        .value(Comparison::LessThan(30.into()))
        .query(),
)?.try_into()?;

// Index search: create index and then query exact match.
db.exec_mut(QueryBuilder::insert().index("username").query())?;
let bob: User = db.exec(
    QueryBuilder::select()
        .elements::<User>()
        .search()
        .index("username")
        .value("bob")
        .query(),
)?.try_into()?;

// Ordered search with limit/offset (full search then slice).
let ordered_ids = db.exec(
    QueryBuilder::search()
        .elements()
        .order_by([DbKeyOrder::Asc("age".into())])
        .offset(1)
        .limit(1)
        .query(),
)?;

// Path search (A*): from root to a specific user.
let path = db.exec(QueryBuilder::search().from("root").to(2).query())?;

// Remove values from elements.
db.exec_mut(QueryBuilder::remove().values(["active".into()]).ids([1, 2]).query())?;

// Remove nodes (also removes their edges and values).
db.exec_mut(QueryBuilder::remove().ids([1, 2]).query())?;

// Transaction: insert node + edge atomically.
db.transaction_mut(|t| {
    let node = t.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
    t.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("root")
            .to(node)
            .query(),
    )?;
    Ok(())
})?;
```

## Production Examples (Rust)

### Storage choice and initialization

```rust
use agdb::{Db, DbAny, DbError, DbFile, DbMemory};

// Default (memory mapped file).
let mut db = Db::new("app.agdb")?;

// File-only (lower memory, slower reads).
let mut db_file = DbFile::new("app.agdb")?;

// In-memory (fast, not persistent unless you backup).
let mut db_mem = DbMemory::new("cache.agdb")?;

// Runtime choice.
let mut db_any = DbAny::new_file("app.agdb")?;
```

### Root node and discovery anchors

```rust
use agdb::{DbError, QueryBuilder};

// Create a root and top-level "collections".
db.exec_mut(QueryBuilder::insert().nodes().aliases(["root", "users", "posts"]).query())?;
db.exec_mut(QueryBuilder::insert().edges().from("root").to(["users", "posts"]).query())?;
```

### Typed CRUD (nodes)

```rust
use agdb::{DbError, DbId, DbType, QueryBuilder};

#[derive(Debug, DbType)]
struct User {
    db_id: Option<DbId>,
    username: String,
    age: u64,
}

let user = User { db_id: None, username: "alice".into(), age: 30 };
let user_id = db.exec_mut(QueryBuilder::insert().element(&user).query())?.elements[0].id;

let mut user: User = db.exec(
    QueryBuilder::select().elements::<User>().ids(user_id).query(),
)?.try_into()?;

user.age += 1;
db.exec_mut(QueryBuilder::insert().element(&user).query())?;

db.exec_mut(QueryBuilder::remove().ids(user_id).query())?;
```

### Custom types, flatten, rename, and type disambiguation

```rust
use agdb::{DbElement, DbError, DbId, DbSerialize, DbType, DbTypeMarker, DbValue, QueryBuilder};

#[derive(Debug, DbType)]
struct Address {
    city: String,
    zip: String,
}

#[derive(Debug, DbType)]
struct Profile {
    db_id: Option<DbId>,
    #[agdb(rename = "display_name")]
    name: String,
    #[agdb(flatten)]
    address: Address,
    #[agdb(skip)]
    _cache: (),
}

#[derive(Debug, DbTypeMarker, DbSerialize, DbValue)]
enum Status {
    Active,
    Disabled,
}

#[derive(Debug, DbElement)]
struct Admin {
    db_id: Option<DbId>,
    name: String,
    status: Status,
}

let admin: Admin = db.exec(
    QueryBuilder::select()
        .element::<Admin>()
        .search()
        .from("users")
        .query(),
)?.try_into()?;
```

### Upserts by alias or id (insert-or-update)

```rust
use agdb::{DbError, QueryBuilder};

// If alias does not exist, inserts a new node with that alias.
// If alias exists, updates that node.
db.exec_mut(
    QueryBuilder::insert()
        .values_uniform([("email", "alice@example.com").into()])
        .ids("user:alice")
        .query(),
)?;

// Update specific ids with different values.
db.exec_mut(
    QueryBuilder::insert()
        .values([
            vec![("status", "active").into()],
            vec![("status", "inactive").into()],
        ])
        .ids([1, 2])
        .query(),
)?;
```

### Relationship modeling with edge properties

```rust
use agdb::{DbError, QueryBuilder};

// Use edge properties to represent relationship type.
db.exec_mut(
    QueryBuilder::insert()
        .edges()
        .from("users")
        .to(42)
        .values_uniform([("kind", "member").into(), ("since", 2024_u64).into()])
        .query(),
)?;
```

### Updating edges by id

```rust
use agdb::{DbError, QueryBuilder};

// Create the edge.
let edge_id = db.exec_mut(
    QueryBuilder::insert()
        .edges()
        .from(1)
        .to(2)
        .values_uniform([("role", "member").into()])
        .query(),
)?.elements[0].id;

// Update edge properties by id (from/to are ignored on update but required by builder).
db.exec_mut(
    QueryBuilder::insert()
        .edges()
        .ids(edge_id)
        .from(1)
        .to(2)
        .values_uniform([("role", "owner").into()])
        .query(),
)?;
```

### Index-backed lookup and uniqueness guard

```rust
use agdb::{DbError, DbType, QueryBuilder};

#[derive(Debug, DbType)]
struct User {
    username: String,
    email: String,
}

db.exec_mut(QueryBuilder::insert().index("username").query())?;

db.transaction_mut(|t| -> Result<(), DbError> {
    let exists = t.exec(
        QueryBuilder::search()
            .index("username")
            .value("alice")
            .query(),
    )?.result > 0;

    if exists {
        return Err(DbError::from("username already exists"));
    }

    t.exec_mut(QueryBuilder::insert().element(&User {
        username: "alice".into(),
        email: "a@b.com".into(),
    }).query())?;

    Ok(())
})?;
```

### Pagination and ordering

```rust
use agdb::{DbError, DbKeyOrder, QueryBuilder};

let page = 0_u64;
let page_size = 20_u64;

let users = db.exec(
    QueryBuilder::select()
        .search()
        .from("users")
        .order_by([DbKeyOrder::Asc("username".into())])
        .offset(page * page_size)
        .limit(page_size)
        .query(),
)?;
```

### Traversal patterns (BFS, DFS, neighbors, path)

```rust
use agdb::{DbError, QueryBuilder, CountComparison};

// BFS (default).
let bfs = db.exec(QueryBuilder::search().from("root").query())?;

// DFS for stable "edge -> node" ordering in joins.
let dfs = db.exec(QueryBuilder::search().depth_first().from("root").query())?;

// Neighbor nodes (distance == 2).
let neighbors = db.exec(
    QueryBuilder::search().from("root").where_().neighbor().query(),
)?;

// Direct outgoing edges only (distance == 1 and edge).
let direct_edges = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .edge()
        .and()
        .distance(CountComparison::Equal(1))
        .query(),
)?;

// Path search (A*).
let path = db.exec(QueryBuilder::search().from("root").to(123).query())?;
```

### Traversal-based removals (delete while walking the graph)

```rust
use agdb::{DbError, QueryBuilder, CountComparison};

// Remove only edges directly connected to "root" (distance == 1).
db.exec_mut(
    QueryBuilder::remove()
        .search()
        .from("root")
        .where_()
        .edge()
        .and()
        .distance(CountComparison::Equal(1))
        .query(),
)?;

// Remove nodes in the 2-hop neighborhood, but keep "root" itself.
db.exec_mut(
    QueryBuilder::remove()
        .search()
        .from("root")
        .where_()
        .node()
        .and()
        .distance(CountComparison::LessThanOrEqual(2))
        .and()
        .not()
        .ids("root")
        .query(),
)?;

// Remove edges along the path from A to B (leave nodes intact).
db.exec_mut(
    QueryBuilder::remove()
        .search()
        .from("A")
        .to("B")
        .where_()
        .edge()
        .query(),
)?;

// Remove all elements in a subtree, but stop traversal at "protected".
db.exec_mut(
    QueryBuilder::remove()
        .search()
        .from("root")
        .where_()
        .not()
        .ids("protected")
        .and()
        .not_beyond()
        .ids("protected")
        .query(),
)?;
```

### Condition recipes (beyond / not_beyond / where)

```rust
use agdb::{DbError, QueryBuilder, Comparison, CountComparison};

// Only include nodes (exclude edges).
let nodes = db.exec(
    QueryBuilder::search().from("root").where_().node().query(),
)?;

// Only include edges.
let edges = db.exec(
    QueryBuilder::search().from("root").where_().edge().query(),
)?;

// Keys-only presence check.
let with_keys = db.exec(
    QueryBuilder::search().from("root").where_().keys(["name", "email"]).query(),
)?;

// Key-value comparisons.
let by_value = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .key("age")
        .value(Comparison::GreaterThanOrEqual(21.into()))
        .query(),
)?;

// Contains / starts_with / ends_with.
let contains = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .key("tags")
        .value(Comparison::Contains(vec!["rust", "db"].into()))
        .query(),
)?;
let starts = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .key("name")
        .value(Comparison::StartsWith("A".into()))
        .query(),
)?;
let ends = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .key("name")
        .value(Comparison::EndsWith("son".into()))
        .query(),
)?;

// Distance: neighbors (distance == 2).
let neighbors = db.exec(
    QueryBuilder::search().from("root").where_().neighbor().query(),
)?;

// Edge counts on nodes.
let busy = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .edge_count_from(CountComparison::GreaterThan(10))
        .query(),
)?;

// Combine logic with nested where_.
let nested = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .node()
        .and()
        .where_()
        .key("status")
        .value("active")
        .or()
        .key("priority")
        .value("high")
        .end_where()
        .query(),
)?;

// beyond(): traverse only through "container" nodes; exclude them from results.
let traverse_containers = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .not()
        .keys("container")
        .and()
        .beyond()
        .keys("container")
        .query(),
)?;

// not_beyond(): stop traversal at "archived" nodes but keep scanning elsewhere.
let stop_at_archived = db.exec(
    QueryBuilder::search()
        .from("root")
        .where_()
        .not()
        .keys("archived")
        .and()
        .not_beyond()
        .keys("archived")
        .query(),
)?;
```

### Join-like read model (edge then node)

```rust
use agdb::{DbError, QueryBuilder};

let joined = db.exec(
    QueryBuilder::select()
        .search()
        .depth_first()
        .from("user")
        .where_()
        .keys("role")
        .or()
        .keys("name")
        .query(),
)?;

// Expect edges (role) followed by nodes (name).
```

### Batch insert and bulk updates

```rust
use agdb::{DbError, QueryBuilder};

// Batch insert nodes with values.
db.exec_mut(
    QueryBuilder::insert()
        .nodes()
        .values([
            [("name", "db1").into()],
            [("name", "db2").into()],
        ])
        .query(),
)?;

// Bulk update values using search ids.
db.exec_mut(
    QueryBuilder::insert()
        .values_uniform([("status", "active").into()])
        .search()
        .from("users")
        .query(),
)?;
```

### Schema migration / backfill

```rust
use agdb::{DbError, DbId, DbType, QueryBuilder};

#[derive(Debug, DbType)]
struct UserV1 {
    db_id: Option<DbId>,
    name: String,
    status: String,
    age: u64,
}

#[derive(Debug, DbType)]
struct UserV2 {
    db_id: Option<DbId>,
    name: String,
    status: u64,
}

db.transaction_mut(|t| -> Result<(), DbError> {
    // Load as V1, transform to V2, update in place.
    let users: Vec<UserV1> = t.exec(
        QueryBuilder::select()
            .elements::<UserV1>()
            .search()
            .from("users")
            .where_()
            .neighbor()
            .query(),
    )?.try_into()?;

    let upgraded: Vec<UserV2> = users.into_iter().map(|u| UserV2 {
        name: u.name,
        status: if u.status == "active" { 1 } else { 0 },
    }).collect();

    t.exec_mut(QueryBuilder::insert().elements(&upgraded).query())?;

    // Remove old field after the update.
    t.exec_mut(
        QueryBuilder::remove()
            .values(["age".into()])
            .search()
            .from("users")
            .query(),
    )?;
    Ok(())
})?;
```

### Soft delete and retention

```rust
use agdb::{DbError, QueryBuilder};

// Soft delete.
db.exec_mut(
    QueryBuilder::insert()
        .values_uniform([("deleted_at", 1700000000_u64).into()])
        .ids(5)
        .query(),
)?;

// Query only active nodes.
let active = db.exec(
    QueryBuilder::select()
        .search()
        .from("users")
        .where_()
        .not()
        .keys("deleted_at")
        .query(),
)?;
```

### Multi-tenant scoping

```rust
use agdb::{DbError, QueryBuilder};

// tenant -> resources edges
db.exec_mut(QueryBuilder::insert().nodes().aliases(["tenant:1", "resources"]).query())?;
db.exec_mut(QueryBuilder::insert().edges().from("tenant:1").to("resources").query())?;

// Insert resource and attach to tenant.
let resource = db.exec_mut(
    QueryBuilder::insert()
        .nodes()
        .values([[("name", "secret").into()]])
        .query(),
)?;
db.exec_mut(
    QueryBuilder::insert()
        .edges()
        .from("tenant:1")
        .to(resource)
        .query(),
)?;

// Tenant-scoped query.
let tenant_resources = db.exec(
    QueryBuilder::select()
        .search()
        .from("tenant:1")
        .where_()
        .neighbor()
        .query(),
)?;
```

### Observability and introspection

```rust
use agdb::{DbError, QueryBuilder};

let node_count = db.exec(QueryBuilder::select().node_count().query())?;
let indexes = db.exec(QueryBuilder::select().indexes().query())?;
let keys = db.exec(QueryBuilder::select().keys().ids(1).query())?;
let key_count = db.exec(QueryBuilder::select().key_count().ids(1).query())?;
```

### Backup, copy, rename, and storage maintenance

```rust
use agdb::DbError;

db.backup("backup.agdb")?;
let cloned = db.copy("clone.agdb")?;
db.rename("renamed.agdb")?;
db.optimize_storage()?;
```

### Concurrency with RwLock

```rust
use agdb::{Db, DbError, QueryBuilder};
use std::sync::{Arc, RwLock};

let db = Arc::new(RwLock::new(Db::new("app.agdb")?));

// Reader.
{
    let db_read = db.read().unwrap();
    let _ = db_read.exec(QueryBuilder::select().ids(1).query())?;
}

// Writer.
{
    let mut db_write = db.write().unwrap();
    let _ = db_write.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
}
```

## Pitfalls to Avoid

- Do not use `insert().aliases()` with search IDs (it errors).
- Do not expect `select().search()` to error on missing keys; add conditions.
- Do not expect `order_by` to short-circuit search; it sorts after full scan.
- Do not rely on alias presence for edges; aliases are intended for nodes.
- Do not assume `from`/`to` searches return only nodes; filter explicitly.
- Do not inspect `values` on elements returned by bare `search()` queries;
  they are empty. Use `select().search()` to get values.
