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
        let releases = scoped_genre_releases(db, viewer, filter)?;
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
            let visible = scoped_genre_releases(db, viewer, filter)?
                .into_iter()
                .collect::<HashSet<_>>();
            for (genre, releases) in lyra_db::genres::get_releases_many(db, &ids)? {
                let releases = releases
                    .into_iter()
                    .filter(|release| visible.contains(release))
                    .collect::<Vec<_>>();
                release_counts.insert(genre, releases.len() as u64);
                tracks_by_genre.insert(genre, tracks_of_releases(db, releases)?);
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

/// The releases the viewer can see that the filter scopes genres to.
fn scoped_genre_releases(
    db: &DbAny,
    viewer: &Viewer,
    filter: &GenreFilter,
) -> anyhow::Result<Vec<DbId>> {
    scoped_releases(db, viewer, filter.library, filter.releases.as_deref(), &[])?
        .resolve(&[], || all_releases(db))
}
