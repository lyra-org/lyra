// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

impl Old for Releases {
    fn old_candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &ReleaseFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let mut releases = scoped_releases(
            db,
            viewer,
            filter.library,
            filter.ids.as_deref(),
            &filter.genres,
        )?;
        let reach = |artists: &[DbId], role| match role {
            CreditRole::Track => releases_of_tracks(db, credited_owners::<Track>(db, artists)?),
            _ => credited_owners::<Release>(db, artists),
        };
        if let Some(credit) = &filter.artists {
            releases.restrict(credit.matching(db, reach)?);
        }
        if !filter.years.is_empty() {
            let mut matching = HashSet::new();
            for year in &filter.years {
                matching.extend(lyra_db::releases::ids_with_year(db, *year)?);
            }
            releases.restrict(matching);
        }
        if let Some(favorite) = filter.personal.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = lyra_db::favorites::target_ids(db, user, FavoriteKind::Release)?;
            releases.restrict_membership(favorites, favorite, || all_releases(db))?;
        }
        if let Some(listened) = filter.personal.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let listened_tracks = lyra_db::listens::listened_track_ids(db, user)?;
            let mut listened_ids = HashSet::new();
            for release in releases_of_tracks(db, listened_tracks.iter().copied())? {
                if lyra_db::graph::neighbor_ids(db, release, "Track")?
                    .iter()
                    .all(|track| listened_tracks.contains(track))
                {
                    listened_ids.insert(release);
                }
            }
            releases.restrict_membership(listened_ids, listened, || all_releases(db))?;
        }
        restrict_rating(db, viewer, filter.personal.rating, &mut releases, || {
            all_releases(db)
        })?;

        let mut excluded = filter.exclude_ids.clone();
        if !filter.exclude_artists.is_empty() {
            excluded.extend(ArtistCredit::any(filter.exclude_artists.clone()).matching(db, reach)?);
        }
        Ok(releases.resolve(&excluded, || all_releases(db))?)
    }

    fn old_rows(
        db: &DbAny,
        viewer: &Viewer,
        _filter: &ReleaseFilter,
        ids: Vec<DbId>,
        keys: &[ReleaseKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = lyra_db::graph::select_fields(db, &ids, &release_fields(keys))?;
        let needs = |wanted: &[ReleaseKey]| keys.iter().any(|key| wanted.contains(key));
        let totals = if needs(&[
            ReleaseKey::TotalDuration,
            ReleaseKey::TrackCount,
            ReleaseKey::ListenCount,
            ReleaseKey::LastPlayedAt,
        ]) {
            let mut tracks_by_release = HashMap::new();
            for id in &ids {
                tracks_by_release.insert(
                    *id,
                    lyra_db::graph::neighbor_ids(db, *id, "Track")?
                        .into_iter()
                        .collect::<HashSet<_>>(),
                );
            }
            track_totals(
                db,
                viewer,
                &tracks_by_release,
                needs(&[ReleaseKey::TotalDuration]),
                needs(&[ReleaseKey::ListenCount, ReleaseKey::LastPlayedAt]),
            )?
        } else {
            HashMap::new()
        };
        let ratings = ratings(
            db,
            viewer,
            &ids,
            keys.iter()
                .any(|key| matches!(key, ReleaseKey::Rating | ReleaseKey::RatedAt)),
        )?;
        let favorited_at = favorited_at(
            db,
            viewer,
            &ids,
            FavoriteKind::Release,
            keys.contains(&ReleaseKey::FavoritedAt),
        )?;
        let artist_names = if needs(&[ReleaseKey::ArtistName]) {
            lyra_db::artists::first_credited_names(db, &ids)?
        } else {
            HashMap::new()
        };

        Ok(fields
            .into_iter()
            .filter_map(|(id, mut release)| {
                let name = release.take_text("release_title")?;
                let mut row = Row::new(id, name, release.text("sort_title"));
                let number = |key| release.number(key).map(SortValue::Number);
                let totals = totals.get(&id).copied().unwrap_or_default();
                row.values = keys
                    .iter()
                    .map(|key| match key {
                        ReleaseKey::Name => Some(SortValue::Text(row.lower_name().to_string())),
                        ReleaseKey::SortName => Some(SortValue::Text(row.sort_name().to_string())),
                        ReleaseKey::DateCreated => number("ctime").or_else(|| number("created_at")),
                        ReleaseKey::ReleaseDate => release
                            .text("release_date")
                            .and_then(date_order)
                            .map(SortValue::Number),
                        ReleaseKey::Year => {
                            lyra_db::releases::release_year(release.text("release_date"))
                                .map(|year| SortValue::Number(year.into()))
                        }
                        ReleaseKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        ReleaseKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        ReleaseKey::ArtistName => artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.to_lowercase())),
                        ReleaseKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        ReleaseKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        ReleaseKey::Rating => ratings
                            .get(&id)
                            .map(|edge| SortValue::Number(edge.value.get().into())),
                        ReleaseKey::RatedAt => ratings
                            .get(&id)
                            .and_then(|edge| timestamp_value(edge.updated_at_ms)),
                        ReleaseKey::FavoritedAt => {
                            favorited_at.get(&id).and_then(|ms| timestamp_value(*ms))
                        }
                        ReleaseKey::Relevance | ReleaseKey::Random | ReleaseKey::Id => None,
                    })
                    .collect();
                Some(row)
            })
            .collect())
    }
}

/// The stored release fields that the names and `keys` read.
fn release_fields(keys: &[ReleaseKey]) -> Vec<&'static str> {
    let mut fields = vec!["release_title", "sort_title"];
    for key in keys {
        fields.extend_from_slice(match key {
            ReleaseKey::DateCreated => &["ctime", "created_at"],
            ReleaseKey::ReleaseDate | ReleaseKey::Year => &["release_date"],
            _ => &[],
        });
    }
    fields
}

/// `YYYY`, `YYYY-MM` or `YYYY-MM-DD` as `YYYYMMDD`, with missing parts as zero.
fn date_order(date: &str) -> Option<u64> {
    let mut parts = date.splitn(3, '-').map(str::parse::<u64>);
    let year = parts.next()?.ok()?;
    let month = parts.next().transpose().ok()?.unwrap_or(0);
    let day = parts.next().transpose().ok()?.unwrap_or(0);
    Some(year * 10_000 + month * 100 + day)
}
