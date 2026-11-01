// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;
use lyra_db::roots::{
    self,
    Root,
};

impl Old for Tracks {
    fn old_shuffle_units(
        db: &DbAny,
        viewer: &Viewer,
        ids: &[DbId],
    ) -> anyhow::Result<HashMap<DbId, ShuffleUnit>> {
        let mut units = HashMap::new();
        for (track, link) in lyra_db::chains::links_for_tracks(db, ids)? {
            if viewer.sees_entity(db, link.release_db_id)? {
                units.insert(
                    track,
                    ShuffleUnit {
                        id: link.chain_db_id,
                        place: link.position,
                    },
                );
            }
        }
        Ok(units)
    }

    fn old_candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &TrackFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let releases = scoped_releases(
            db,
            viewer,
            filter.library,
            filter.releases.as_deref(),
            &filter.genres,
        )?;
        let mut tracks = Candidates::default();
        match (&filter.ids, releases.ids()) {
            (Some(ids), Some(releases)) => {
                let mut on_releases = Vec::new();
                for track in lyra_db::graph::existing_ids(db, ids, "Track")? {
                    if lyra_db::graph::inbound_neighbor_ids(db, track, "Release")?
                        .iter()
                        .any(|release| releases.contains(release))
                    {
                        on_releases.push(track);
                    }
                }
                tracks.restrict(on_releases);
            }
            (Some(ids), None) => tracks.restrict(lyra_db::graph::existing_ids(db, ids, "Track")?),
            (None, Some(releases)) => {
                tracks.restrict(tracks_of_releases(db, releases.iter().copied())?);
            }
            (None, None) => {}
        }
        let visible = visible_releases(viewer, db)?;
        let reach = |artists: &[DbId], role| match role {
            CreditRole::Release => tracks_of_releases(
                db,
                credited_owners::<lyra_db::Release>(db, artists)?
                    .into_iter()
                    .filter(|release| seen(&visible, *release)),
            ),
            _ => credited_owners::<Track>(db, artists),
        };
        if let Some(credit) = &filter.artists {
            tracks.restrict(credit.matching(db, reach)?);
        }
        if !filter.years.is_empty() {
            let mut matching = HashSet::new();
            for year in &filter.years {
                matching.extend(lyra_db::tracks::ids_with_year(db, *year)?);
            }
            tracks.restrict(matching);
        }
        if let Some(favorite) = filter.personal.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = lyra_db::favorites::target_ids(db, user, FavoriteKind::Track)?;
            tracks.restrict_membership(favorites, favorite, || all_tracks(db))?;
        }
        if let Some(listened) = filter.personal.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let listened_ids = lyra_db::listens::listened_track_ids(db, user)?;
            tracks.restrict_membership(listened_ids, listened, || all_tracks(db))?;
        }
        restrict_rating(db, viewer, filter.personal.rating, &mut tracks, || {
            all_tracks(db)
        })?;

        let mut excluded = filter.exclude_ids.clone();
        if !filter.exclude_artists.is_empty() {
            excluded.extend(ArtistCredit::any(filter.exclude_artists.clone()).matching(db, reach)?);
        }
        Ok(tracks.resolve(&excluded, || all_tracks(db))?)
    }

    fn old_rows(
        db: &DbAny,
        viewer: &Viewer,
        _filter: &TrackFilter,
        ids: Vec<DbId>,
        keys: &[TrackKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = lyra_db::graph::select_fields(db, &ids, &track_fields(keys))?;
        let listens = if keys
            .iter()
            .any(|key| matches!(key, TrackKey::ListenCount | TrackKey::LastPlayedAt))
        {
            let user = viewer.user_db_id("listen sort keys")?;
            lyra_db::listens::get_stats(db, &ids, user)?
                .into_iter()
                .map(|stats| (stats.db_id, stats))
                .collect()
        } else {
            HashMap::new()
        };
        let ratings = ratings(
            db,
            viewer,
            &ids,
            keys.iter()
                .any(|key| matches!(key, TrackKey::Rating | TrackKey::RatedAt)),
        )?;
        let favorited_at = favorited_at(
            db,
            viewer,
            &ids,
            FavoriteKind::Track,
            keys.contains(&TrackKey::FavoritedAt),
        )?;
        let artist_names = if keys.contains(&TrackKey::ArtistName) {
            lyra_db::artists::first_credited_names(db, &ids)?
        } else {
            HashMap::new()
        };
        let releases = if keys
            .iter()
            .any(|key| matches!(key, TrackKey::ReleaseTitle | TrackKey::ReleaseArtistName))
        {
            TrackReleases::of(db, &ids, &visible_releases(viewer, db)?)?
        } else {
            TrackReleases::default()
        };
        let release_artist_names = if keys.contains(&TrackKey::ReleaseArtistName) {
            releases.least(&lyra_db::artists::first_credited_names(
                db,
                &releases.release_ids(),
            )?)
        } else {
            HashMap::new()
        };
        let release_titles = if keys.contains(&TrackKey::ReleaseTitle) {
            let fields =
                lyra_db::graph::select_fields(db, &releases.release_ids(), &["release_title"])?;
            releases.least(
                &fields
                    .iter()
                    .filter_map(|(id, release)| {
                        Some((*id, release.text("release_title")?.to_string()))
                    })
                    .collect(),
            )
        } else {
            HashMap::new()
        };

        Ok(fields
            .into_iter()
            .filter_map(|(id, mut track)| {
                let name = track.take_text("track_title")?;
                let mut row = Row::new(id, name, track.text("sort_title"));
                let number = |key| track.number(key).map(SortValue::Number);
                row.values = keys
                    .iter()
                    .map(|key| match key {
                        TrackKey::Name => Some(SortValue::Text(row.lower_name().to_string())),
                        TrackKey::SortName => Some(SortValue::Text(row.sort_name().to_string())),
                        TrackKey::DateCreated => {
                            number("file_created_at_ms").or_else(|| number("created_at_ms"))
                        }
                        TrackKey::Year => number("year"),
                        TrackKey::Duration => number("duration_ms"),
                        TrackKey::Disc => number("disc"),
                        TrackKey::Track => number("track"),
                        TrackKey::ReleaseTitle => release_titles
                            .get(&id)
                            .map(|title| SortValue::Text(title.clone())),
                        TrackKey::ArtistName => artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.to_lowercase())),
                        TrackKey::ReleaseArtistName => release_artist_names
                            .get(&id)
                            .map(|name| SortValue::Text(name.clone())),
                        TrackKey::ListenCount => Some(SortValue::Number(
                            listens.get(&id).map_or(0, |stats| stats.count),
                        )),
                        TrackKey::LastPlayedAt => listens
                            .get(&id)
                            .and_then(|stats| stats.last_played)
                            .map(SortValue::Number),
                        TrackKey::Rating => ratings
                            .get(&id)
                            .map(|edge| SortValue::Number(edge.value.get().into())),
                        TrackKey::RatedAt => ratings
                            .get(&id)
                            .and_then(|edge| timestamp_value(edge.updated_at_ms)),
                        TrackKey::FavoritedAt => {
                            favorited_at.get(&id).and_then(|ms| timestamp_value(*ms))
                        }
                        TrackKey::Relevance | TrackKey::Random | TrackKey::Id => None,
                    })
                    .collect();
                Some(row)
            })
            .collect())
    }
}

/// The stored track fields that the names and `keys` read.
fn track_fields(keys: &[TrackKey]) -> Vec<&'static str> {
    let mut fields = vec!["track_title", "sort_title"];
    for key in keys {
        fields.extend_from_slice(match key {
            TrackKey::DateCreated => &["file_created_at_ms", "created_at_ms"],
            TrackKey::Year => &["year"],
            TrackKey::Duration => &["duration_ms"],
            TrackKey::Disc => &["disc"],
            TrackKey::Track => &["track"],
            _ => &[],
        });
    }
    fields
}

/// The releases each track is on.
#[derive(Default)]
struct TrackReleases(HashMap<DbId, Vec<DbId>>);

impl TrackReleases {
    fn of(db: &DbAny, tracks: &[DbId], visible: &Option<HashSet<DbId>>) -> anyhow::Result<Self> {
        let mut releases = HashMap::new();
        for track in tracks {
            releases.insert(
                *track,
                lyra_db::graph::inbound_neighbor_ids(db, *track, "Release")?
                    .into_iter()
                    .filter(|release| seen(visible, *release))
                    .collect(),
            );
        }
        Ok(Self(releases))
    }

    fn release_ids(&self) -> Vec<DbId> {
        self.0
            .values()
            .flatten()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Each track's lowercased value from its releases; the least when a track is on several.
    fn least(&self, values: &HashMap<DbId, String>) -> HashMap<DbId, String> {
        self.0
            .iter()
            .filter_map(|(track, releases)| {
                let value = releases
                    .iter()
                    .filter_map(|release| values.get(release))
                    .map(|value| value.to_lowercase())
                    .min()?;
                Some((*track, value))
            })
            .collect()
    }
}

fn all_tracks(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    roots::member_ids(db, Root::Tracks)
}
