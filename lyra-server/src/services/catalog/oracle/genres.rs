// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

impl Old for Genres {
    fn old_candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &GenreFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let (releases, _) = genre_scope(db, viewer, filter)?;
        let mut genres = Candidates::default();
        match &filter.ids {
            Some(ids) => {
                let releases = releases.into_iter().collect::<HashSet<_>>();
                let ids = lyra_db::graph::existing_ids(db, ids, "Genre")?;
                genres.restrict(
                    lyra_db::genres::get_releases_many(db, &ids)?
                        .into_iter()
                        .filter(|(_, genre_releases)| {
                            genre_releases
                                .iter()
                                .any(|release| releases.contains(release))
                        })
                        .map(|(genre, _)| genre),
                );
            }
            None => genres.restrict(
                lyra_db::genres::get_for_releases_many(db, &releases)?
                    .into_values()
                    .flatten()
                    .filter_map(|genre| genre.db_id.map(DbId::from)),
            ),
        }
        Ok(genres.resolve(&filter.exclude_ids, || Ok(Vec::new()))?)
    }

    fn old_rows(
        db: &DbAny,
        viewer: &Viewer,
        filter: &GenreFilter,
        ids: Vec<DbId>,
        keys: &[GenreKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = lyra_db::graph::select_fields(db, &ids, &["name", "created_at_ms"])?;
        let needs = |wanted: &[GenreKey]| keys.iter().any(|key| wanted.contains(key));
        let mut release_counts = HashMap::new();
        let mut tracks_by_genre = HashMap::new();
        if needs(&[
            GenreKey::ReleaseCount,
            GenreKey::TrackCount,
            GenreKey::TotalDuration,
            GenreKey::ListenCount,
            GenreKey::LastPlayedAt,
        ]) {
            let (counted, counted_tracks) = genre_scope(db, viewer, filter)?;
            let counted = counted.into_iter().collect::<HashSet<_>>();
            for (genre, releases) in lyra_db::genres::get_releases_many(db, &ids)? {
                let releases = releases
                    .into_iter()
                    .filter(|release| counted.contains(release))
                    .collect::<Vec<_>>();
                release_counts.insert(genre, releases.len() as u64);
                let mut tracks = tracks_of_releases(db, releases)?;
                if let Some(counted_tracks) = &counted_tracks {
                    tracks.retain(|track| counted_tracks.contains(track));
                }
                tracks_by_genre.insert(genre, tracks);
            }
        }
        let totals = track_totals(
            db,
            viewer,
            &tracks_by_genre,
            needs(&[GenreKey::TotalDuration]),
            needs(&[GenreKey::ListenCount, GenreKey::LastPlayedAt]),
        )?;

        Ok(ids
            .into_iter()
            .filter_map(|id| {
                let genre = fields.get(&id)?;
                let mut row = Row::new(id, genre.text("name")?.to_string(), None);
                let totals = totals.get(&id).copied().unwrap_or_default();
                row.values = keys
                    .iter()
                    .map(|key| match key {
                        GenreKey::Name => Some(SortValue::Text(row.lower_name().to_string())),
                        GenreKey::DateCreated => {
                            genre.number("created_at_ms").map(SortValue::Number)
                        }
                        GenreKey::ReleaseCount => Some(SortValue::Number(
                            release_counts.get(&id).copied().unwrap_or(0),
                        )),
                        GenreKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        GenreKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        GenreKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        GenreKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        GenreKey::Relevance | GenreKey::Random | GenreKey::Id => None,
                    })
                    .collect();
                Some(row)
            })
            .collect())
    }
}

/// The releases the viewer can see that the filter's library, availability and nested queries
/// keep, and the tracks that alone count, when anything narrows them.
fn genre_scope(
    db: &DbAny,
    viewer: &Viewer,
    filter: &GenreFilter,
) -> Result<(Vec<DbId>, Option<HashSet<DbId>>), CatalogError> {
    let mut releases = scoped_releases(db, viewer, filter.library, None, &[])?;
    let available = kept(db, filter.available)?;
    if let Some(available) = &available {
        releases.restrict(available.releases.iter().copied());
    }
    if let Some(matching) = &filter.releases {
        let matching = inheriting(matching, filter.available);
        releases.restrict(matched(db, viewer, &matching, "Release")?);
    }
    let mut tracks = filter
        .tracks
        .as_ref()
        .map(|matching| matched(db, viewer, &inheriting(matching, filter.available), "Track"))
        .transpose()?;
    if let (Some(matching), Some(tracks)) = (&filter.tracks, &tracks) {
        let visible = visible_releases(viewer, db)?;
        let kept = scoped_releases(
            db,
            viewer,
            matching.filter.library,
            matching.filter.releases.as_deref(),
            &matching.filter.genres,
        )?;
        releases.restrict(
            releases_of_tracks(db, tracks.iter().copied())?
                .into_iter()
                .filter(|release| {
                    seen(&visible, *release) && kept.ids().is_none_or(|kept| kept.contains(release))
                }),
        );
    }
    if let Some(available) = available {
        tracks = Some(match tracks {
            Some(tracks) => tracks.intersection(&available.tracks).copied().collect(),
            None => available.tracks,
        });
    }
    Ok((releases.resolve(&[], || all_releases(db))?, tracks))
}

/// `matching` with the availability of the query it is nested in, which applies to the whole
/// query.
fn inheriting<C: Catalog>(matching: &Match<C>, available: Option<bool>) -> Match<C> {
    let mut matching = matching.clone();
    let nested = C::available_mut(&mut matching.filter);
    if nested.is_none() {
        *nested = available;
    }
    matching
}
