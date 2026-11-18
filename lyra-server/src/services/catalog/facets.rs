// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What the items a match keeps hold, read without listing them: their years, and the viewer's
//! tags on them. Each reads what the filter of the same name keeps by, so filtering by a value
//! read here keeps the items it was read from.

use std::collections::BTreeSet;

use agdb::DbAny;

use super::{
    CatalogError,
    Match,
    PersonalData,
    Viewer,
    index::{
        MISSING,
        Snapshot,
    },
    pipeline::{
        Catalog,
        Context,
        matching_in,
    },
    releases::Releases,
    tracks::Tracks,
};

/// A catalog whose `years` filter keeps entities by a year of their own.
pub(crate) trait Dated: Catalog {
    /// The year the `years` filter reads for each slot, [`MISSING`] where an entity has none.
    fn years(index: &Snapshot) -> &[u64];
}

/// A track's own year, which its release's doesn't stand in for.
impl Dated for Tracks {
    fn years(index: &Snapshot) -> &[u64] {
        &index.track.year
    }
}

/// The year of a release's date.
impl Dated for Releases {
    fn years(index: &Snapshot) -> &[u64] {
        &index.release.year
    }
}

/// Every year of the items `matching` keeps, ascending; items without one add none.
pub(crate) fn years<C: Dated>(
    db: &DbAny,
    viewer: &Viewer,
    matching: &Match<C>,
) -> Result<Vec<u32>, CatalogError> {
    let cx = Context::filtered::<C>(db, viewer, &matching.filter)?;
    let values = C::years(&cx.index);
    let years = matching_in(&cx, matching)?
        .iter()
        .map(|slot| values[slot as usize])
        .filter(|year| *year != MISSING)
        .collect::<BTreeSet<_>>();
    Ok(years
        .into_iter()
        .filter_map(|year| u32::try_from(year).ok())
        .collect())
}

/// The viewer's personal data a tag facet over `matching` reads: their tags, and what the match
/// reads.
pub(crate) fn tags_read<C: Catalog>(matching: &Match<C>) -> Vec<PersonalData> {
    let mut reads = vec![PersonalData::Tags];
    reads.extend(matching.personal());
    reads
}

/// The names of the viewer's tags on the items `matching` keeps, ascending.
pub(crate) fn tags<C: Catalog>(
    db: &DbAny,
    viewer: &Viewer,
    matching: &Match<C>,
) -> Result<Vec<String>, CatalogError> {
    let user = viewer.user_db_id("the tag facet")?;
    let cx = Context::filtered::<C>(db, viewer, &matching.filter)?;
    let table = C::table(&cx.index);
    let matched = matching_in(&cx, matching)?;
    let mut names = lyra_db::tags::with_targets(db, user)?
        .into_iter()
        .filter(|(_, targets)| {
            targets.iter().any(|target| {
                table
                    .slot(*target)
                    .is_some_and(|slot| matched.contains(slot))
            })
        })
        .map(|(tag, _)| tag.tag)
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
}
