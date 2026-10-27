// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashSet;

use agdb::{
    DbAny,
    DbElement,
    DbId,
    QueryBuilder,
    QueryId,
};

#[derive(Clone, Debug)]
pub struct Candidate {
    pub db_id: DbId,
    pub id: String,
    pub text: String,
}

#[derive(Default)]
pub struct Candidates {
    pub tracks: Vec<Candidate>,
    pub artists: Vec<Candidate>,
    pub releases: Vec<Candidate>,
}

fn string_value(element: &mut DbElement, key: &str) -> anyhow::Result<String> {
    let value = element
        .values
        .iter_mut()
        .find(|value| value.key.string().is_ok_and(|stored| stored == key))
        .ok_or_else(|| anyhow::anyhow!("search candidate {} is missing {key}", element.id.0))?;
    Ok(String::try_from(std::mem::take(&mut value.value))?)
}

fn neighbor_types(db: &DbAny, from: impl Into<QueryId>) -> anyhow::Result<Vec<DbElement>> {
    Ok(db
        .exec(
            QueryBuilder::select()
                .values(["db_element_id"])
                .search()
                .from(from)
                .where_()
                .neighbor()
                .query(),
        )?
        .elements)
}

fn typed_neighbors(db: &DbAny, from: impl Into<QueryId>, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(neighbor_types(db, from)?
        .into_iter()
        .filter(|element| super::graph::is_element_type(element, kind))
        .map(|element| element.id)
        .collect())
}

fn candidates(elements: Vec<DbElement>, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    elements
        .into_iter()
        .map(|mut element| {
            Ok(Candidate {
                db_id: element.id,
                id: string_value(&mut element, "id")?,
                text: string_value(&mut element, text_key)?,
            })
        })
        .collect()
}

fn fetch(db: &DbAny, ids: HashSet<DbId>, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort_unstable();
    let elements = db
        .exec(
            QueryBuilder::select()
                .values(["id", text_key])
                .ids(ids)
                .query(),
        )?
        .elements;
    candidates(elements, text_key)
}

fn from_root(db: &DbAny, root: &str, kind: &str, text_key: &str) -> anyhow::Result<Vec<Candidate>> {
    let mut query = QueryBuilder::select()
        .values(["id", text_key])
        .search()
        .from(root)
        .where_()
        .neighbor()
        .and()
        .key("db_element_id")
        .value(kind);
    if kind == "Artist" {
        query = query.and().key(super::credits::CREDITED).value(true);
    }
    candidates(db.exec(query.query())?.elements, text_key)
}

pub fn collect_candidates(
    db: &DbAny,
    library_ids: Option<&HashSet<String>>,
) -> anyhow::Result<Candidates> {
    let Some(library_ids) = library_ids else {
        return Ok(Candidates {
            tracks: from_root(db, "tracks", "Track", "track_title")?,
            artists: from_root(db, "artists", "Artist", "artist_name")?,
            releases: from_root(db, "releases", "Release", "release_title")?,
        });
    };
    let (mut tracks, mut artists, mut releases) = (HashSet::new(), HashSet::new(), HashSet::new());
    for public_id in library_ids {
        let Some(library_id) = super::lookup::find_node_id_by_id(db, public_id)? else {
            continue;
        };
        if super::libraries::get_by_id(db, library_id)?.is_some() {
            releases.extend(typed_neighbors(db, library_id, "Release")?);
        }
    }
    // Walk each distinct release once, collecting IDs before fetching search fields.
    for release in &releases {
        for element in neighbor_types(db, *release)? {
            if super::graph::is_element_type(&element, "Track") {
                tracks.insert(element.id);
            } else if super::graph::is_element_type(&element, "Artist") {
                artists.insert(element.id);
            }
        }
    }
    Ok(Candidates {
        tracks: fetch(db, tracks, "track_title")?,
        artists: fetch(db, artists, "artist_name")?,
        releases: fetch(db, releases, "release_title")?,
    })
}
