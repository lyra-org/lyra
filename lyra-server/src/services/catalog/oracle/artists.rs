// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use super::*;

impl Old for Artists {
    fn old_candidates(
        db: &DbAny,
        viewer: &Viewer,
        filter: &ArtistFilter,
    ) -> Result<Vec<DbId>, CatalogError> {
        let mut artists = Candidates::default();
        if let Some(ids) = &filter.ids {
            let ids = lyra_db::graph::existing_ids(db, ids, "Artist")?;
            match &visible_releases(viewer, db)? {
                Some(visible) => {
                    let mut credited_visibly = Vec::new();
                    for artist in ids {
                        if credited_on_visible(db, artist, visible)? {
                            credited_visibly.push(artist);
                        }
                    }
                    artists.restrict(credited_visibly);
                }
                None => artists.restrict(ids),
            }
        }
        if filter.narrows_credits() || (!viewer.sees_everything() && filter.ids.is_none()) {
            artists.restrict(credited_artists(db, viewer, filter)?);
        }
        if let Some(artist_type) = filter.artist_type {
            artists.restrict(match filter.ids {
                Some(_) => lyra_db::artists::ids_with_type(db, artist_type)?,
                None => lyra_db::artists::credited_ids(db, Some(artist_type))?,
            });
        }
        if let Some(favorite) = filter.personal.favorite {
            let user = viewer.user_db_id("the favorite filter")?;
            let favorites = lyra_db::favorites::target_ids(db, user, FavoriteKind::Artist)?;
            artists.restrict_membership(favorites, favorite, || all_artists(db))?;
        }
        if let Some(listened) = filter.personal.listened {
            let user = viewer.user_db_id("the listened filter")?;
            let visible = visible_releases(viewer, db)?;
            let mut tracks = lyra_db::listens::listened_track_ids(db, user)?;
            if let Some(visible) = &visible {
                let seen = tracks_of_releases(db, visible.iter().copied())?;
                tracks.retain(|track| seen.contains(track));
            }
            let mut listened_artists = HashSet::new();
            for track in &tracks {
                listened_artists.extend(credited_artist_ids(db, *track)?);
            }
            for release in releases_of_tracks(db, tracks)? {
                if seen(&visible, release) {
                    listened_artists.extend(credited_artist_ids(db, release)?);
                }
            }
            artists.restrict_membership(listened_artists, listened, || all_artists(db))?;
        }
        restrict_rating(db, viewer, filter.personal.rating, &mut artists, || {
            all_artists(db)
        })?;

        Ok(artists.resolve(&filter.exclude_ids, || all_artists(db))?)
    }

    fn old_rows(
        db: &DbAny,
        viewer: &Viewer,
        _filter: &ArtistFilter,
        ids: Vec<DbId>,
        keys: &[ArtistKey],
    ) -> Result<Vec<Row>, CatalogError> {
        let fields = lyra_db::graph::select_fields(db, &ids, &artist_fields(keys))?;
        let needs = |wanted: &[ArtistKey]| keys.iter().any(|key| wanted.contains(key));
        let visible = if needs(&[
            ArtistKey::ReleaseCount,
            ArtistKey::TrackCount,
            ArtistKey::TotalDuration,
            ArtistKey::ListenCount,
            ArtistKey::LastPlayedAt,
        ]) {
            visible_releases(viewer, db)?
        } else {
            None
        };
        let visible_tracks = match &visible {
            Some(releases) => Some(tracks_of_releases(db, releases.iter().copied())?),
            None => None,
        };

        let mut release_counts = HashMap::new();
        let mut tracks_by_artist = HashMap::new();
        if needs(&[
            ArtistKey::ReleaseCount,
            ArtistKey::TrackCount,
            ArtistKey::TotalDuration,
            ArtistKey::ListenCount,
            ArtistKey::LastPlayedAt,
        ]) {
            for id in &ids {
                let mut releases = credited_owners::<Release>(db, &[*id])?;
                if let Some(visible) = &visible {
                    releases.retain(|release| visible.contains(release));
                }
                let mut tracks = credited_owners::<Track>(db, &[*id])?;
                tracks.extend(tracks_of_releases(db, releases.iter().copied())?);
                if let Some(visible_tracks) = &visible_tracks {
                    tracks.retain(|track| visible_tracks.contains(track));
                }
                release_counts.insert(*id, releases.len() as u64);
                tracks_by_artist.insert(*id, tracks);
            }
        }
        let ratings = ratings(
            db,
            viewer,
            &ids,
            keys.iter()
                .any(|key| matches!(key, ArtistKey::Rating | ArtistKey::RatedAt)),
        )?;
        let favorited_at = favorited_at(
            db,
            viewer,
            &ids,
            FavoriteKind::Artist,
            keys.contains(&ArtistKey::FavoritedAt),
        )?;
        let totals = track_totals(
            db,
            viewer,
            &tracks_by_artist,
            needs(&[ArtistKey::TotalDuration]),
            needs(&[ArtistKey::ListenCount, ArtistKey::LastPlayedAt]),
        )?;

        Ok(fields
            .into_iter()
            .filter_map(|(id, mut artist)| {
                let name = artist.take_text("artist_name")?;
                let mut row = Row::new(id, name, artist.text("sort_name"));
                let totals = totals.get(&id).copied().unwrap_or_default();
                row.values = keys
                    .iter()
                    .map(|key| match key {
                        ArtistKey::Name => Some(SortValue::Text(row.lower_name().to_string())),
                        ArtistKey::SortName => Some(SortValue::Text(row.sort_name().to_string())),
                        ArtistKey::DateCreated => artist
                            .number("file_created_at_ms")
                            .or_else(|| artist.number("created_at_ms"))
                            .map(SortValue::Number),
                        ArtistKey::ReleaseCount => Some(SortValue::Number(
                            release_counts.get(&id).copied().unwrap_or(0),
                        )),
                        ArtistKey::TrackCount => Some(SortValue::Number(totals.track_count)),
                        ArtistKey::TotalDuration => Some(SortValue::Number(totals.total_duration)),
                        ArtistKey::ListenCount => Some(SortValue::Number(totals.listen_count)),
                        ArtistKey::LastPlayedAt => totals.last_played_at.map(SortValue::Number),
                        ArtistKey::Rating => ratings
                            .get(&id)
                            .map(|edge| SortValue::Number(edge.value.get().into())),
                        ArtistKey::RatedAt => ratings
                            .get(&id)
                            .and_then(|edge| timestamp_value(edge.updated_at_ms)),
                        ArtistKey::FavoritedAt => {
                            favorited_at.get(&id).and_then(|ms| timestamp_value(*ms))
                        }
                        ArtistKey::Relevance | ArtistKey::Random | ArtistKey::Id => None,
                    })
                    .collect();
                Some(row)
            })
            .collect())
    }
}

/// The artists holding a credit the filter allows on an owner the viewer can see.
fn credited_artists(
    db: &DbAny,
    viewer: &Viewer,
    filter: &ArtistFilter,
) -> anyhow::Result<HashSet<DbId>> {
    let releases = scoped_releases(
        db,
        viewer,
        filter.library,
        filter.releases.as_deref(),
        &filter.genres,
    )?;
    let releases = releases.resolve(&[], || all_releases(db))?;
    if filter.credits_anywhere() {
        return credited_anywhere(db, releases);
    }
    let mut tracks = Candidates::default();
    tracks.restrict(tracks_of_releases(db, releases.iter().copied())?);
    if let Some(ids) = &filter.tracks {
        tracks.restrict(lyra_db::graph::existing_ids(db, ids, "Track")?);
    }
    let tracks = tracks.resolve(&[], || Ok(Vec::new()))?;

    let allowed = |credit_type: CreditType| {
        filter
            .credit_types
            .as_ref()
            .is_none_or(|types| types.contains(&credit_type))
            && !filter.exclude_credit_types.contains(&credit_type)
    };
    let credited = |owners: &[DbId]| -> anyhow::Result<(HashSet<DbId>, Vec<DbId>)> {
        let mut artists = HashSet::new();
        let mut uncredited = Vec::new();
        for owner in owners {
            let links = lyra_db::credits::links_for_owner(db, *owner)?;
            if links.is_empty() {
                uncredited.push(*owner);
            }
            artists.extend(
                links
                    .into_iter()
                    .filter(|link| allowed(link.credit.credit_type))
                    .map(|link| link.artist_id),
            );
        }
        Ok((artists, uncredited))
    };
    let (by_track, uncredited_tracks) = credited(&tracks)?;
    // A track without credits of its own takes its release's, so a track scope reads the
    // credits of the releases those tracks sit on.
    let releases = if filter.tracks.is_some() {
        let visible_releases = releases.into_iter().collect::<HashSet<_>>();
        releases_of_tracks(db, uncredited_tracks)?
            .into_iter()
            .filter(|release| visible_releases.contains(release))
            .collect()
    } else {
        releases
    };
    let (by_release, _) = credited(&releases)?;
    let in_role = |role| match role {
        CreditRole::Track => by_track.clone(),
        CreditRole::Release => by_release.clone(),
        CreditRole::Any => by_track.union(&by_release).copied().collect(),
    };
    let mut artists = in_role(filter.credit_role.unwrap_or(CreditRole::Any));
    if let Some(excluding) = filter.exclude_credit_role {
        let excluded = in_role(excluding);
        artists.retain(|artist| !excluded.contains(artist));
    }
    Ok(artists)
}

/// The browsable artists credited on `releases` or on their tracks, in any role.
///
/// Reads whichever side has fewer edges: each browsable artist's credits, or every release and
/// track the releases hold. Both sides keep the same artists, so the choice changes only the cost.
fn credited_anywhere(db: &DbAny, releases: Vec<DbId>) -> anyhow::Result<HashSet<DbId>> {
    if releases.is_empty() {
        return Ok(HashSet::new());
    }
    let artists = all_artists(db)?;
    if reads_by_artist(db, &artists, &releases)? {
        credited_by_artist(db, artists, &releases.into_iter().collect())
    } else {
        credited_by_owner(db, &artists.into_iter().collect(), releases)
    }
}

/// Whether the artists' incoming edges, which hold their credits, are fewer than the releases'
/// outgoing ones, which lead to their tracks and credits.
fn reads_by_artist(db: &DbAny, artists: &[DbId], releases: &[DbId]) -> anyhow::Result<bool> {
    let artist_edges = db
        .exec(QueryBuilder::select().edge_count_to().ids(artists).query())?
        .result;
    let release_edges = db
        .exec(
            QueryBuilder::select()
                .edge_count_from()
                .ids(releases)
                .query(),
        )?
        .result;
    Ok(artist_edges < release_edges)
}

/// The `artists` credited on `releases` or on their tracks, read from each artist's credits.
fn credited_by_artist(
    db: &DbAny,
    artists: Vec<DbId>,
    releases: &HashSet<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut credited = HashSet::new();
    'artists: for artist in artists {
        if lyra_db::credits::crediting_owner_ids(db, artist)?
            .iter()
            .any(|owner| releases.contains(owner))
        {
            credited.insert(artist);
            continue;
        }
        for track in credited_owners::<Track>(db, &[artist])? {
            if lyra_db::graph::inbound_neighbor_ids(db, track, "Release")?
                .iter()
                .any(|release| releases.contains(release))
            {
                credited.insert(artist);
                continue 'artists;
            }
        }
    }
    Ok(credited)
}

/// The `artists` credited on `releases` or on their tracks, read from each release and track.
fn credited_by_owner(
    db: &DbAny,
    artists: &HashSet<DbId>,
    releases: Vec<DbId>,
) -> anyhow::Result<HashSet<DbId>> {
    let mut credited = HashSet::new();
    for owner in tracks_of_releases(db, releases.iter().copied())?
        .into_iter()
        .chain(releases)
    {
        credited.extend(
            credited_artist_ids(db, owner)?
                .into_iter()
                .filter(|artist| artists.contains(artist)),
        );
    }
    Ok(credited)
}

/// Whether the artist holds a credit on a visible release or on a track of one.
fn credited_on_visible(db: &DbAny, artist: DbId, visible: &HashSet<DbId>) -> anyhow::Result<bool> {
    for owner in lyra_db::credits::crediting_owner_ids(db, artist)? {
        if visible.contains(&owner)
            || lyra_db::graph::inbound_neighbor_ids(db, owner, "Release")?
                .iter()
                .any(|release| visible.contains(release))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn credited_artist_ids(db: &DbAny, owner: DbId) -> anyhow::Result<Vec<DbId>> {
    lyra_db::credits::credited_artist_ids(db, &[owner])
}

/// The stored artist fields that the names and `keys` read.
fn artist_fields(keys: &[ArtistKey]) -> Vec<&'static str> {
    let mut fields = vec!["artist_name", "sort_name"];
    if keys.contains(&ArtistKey::DateCreated) {
        fields.extend(["file_created_at_ms", "created_at_ms"]);
    }
    fields
}

/// The artists browsing reaches: those a release or track credits.
fn all_artists(db: &DbAny) -> anyhow::Result<Vec<DbId>> {
    lyra_db::artists::credited_ids(db, None)
}
