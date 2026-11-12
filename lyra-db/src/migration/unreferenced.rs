// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! What nothing references used to wait for a scan's end to walk the whole catalog for it, so a
//! scan that failed or was cut short could leave it stored. Nothing walks the catalog any more:
//! a write collects what it leaves, and artists are collected from the `artist_candidates` root.
//!
//! So the tracks with no file, releases with no track, track sources with no track, cue tracks
//! with no source and cue sheets with no cue track left by then go here, with what was attached
//! to them, and every artist no walk from a release or track reaches becomes a candidate; the
//! next collection keeps the ones a metadata layer still references.
//!
//! A cover only removed entities owned loses its record, and its file is listed in
//! `cover_discards` for the server to discard when it starts.

use std::collections::HashSet;

use agdb::{
    CountComparison,
    DbAnyTransactionMut,
    DbId,
    QueryBuilder,
};

type Db<'a, 'b> = &'a mut DbAnyTransactionMut<'b>;

pub(super) fn run(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
    let mut tracks = Vec::new();
    for track in members(t, "tracks", "Track")? {
        if !reaches_entry(t, track)? {
            tracks.push(track);
        }
    }
    remove_entities(t, &tracks)?;

    let mut releases = Vec::new();
    for release in members(t, "releases", "Release")? {
        if !reaches(t, release, "Track")? {
            releases.push(release);
        }
    }
    remove_entities(t, &releases)?;

    let mut sources = Vec::new();
    for source in members(t, "track_sources", "TrackSource")? {
        if kinds_to(t, source, "Track")?.is_empty() {
            sources.push(source);
        }
    }
    remove(t, &sources)?;

    let mut cue_tracks = Vec::new();
    for cue_track in members(t, "cue_tracks", "CueTrack")? {
        if kinds_to(t, cue_track, "TrackSource")?.is_empty() {
            cue_tracks.push(cue_track);
        }
    }
    remove(t, &cue_tracks)?;

    let mut cue_sheets = Vec::new();
    for sheet in members(t, "cue_sheets", "CueSheet")? {
        if kinds_from(t, sheet, "CueTrack")?.is_empty() {
            cue_sheets.push(sheet);
        }
    }
    remove(t, &cue_sheets)?;

    seed_artist_candidates(t)
}

fn members(t: Db<'_, '_>, root: &str, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(t.exec(
        QueryBuilder::search()
            .from(root)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value(kind)
            .query(),
    )?
    .ids())
}

/// The `kind` nodes one outgoing edge from `from`.
fn kinds_from(t: Db<'_, '_>, from: DbId, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(t.exec(
        QueryBuilder::search()
            .from(from)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value(kind)
            .query(),
    )?
    .ids())
}

/// The `kind` nodes one incoming edge from `to`.
fn kinds_to(t: Db<'_, '_>, to: DbId, kind: &str) -> anyhow::Result<Vec<DbId>> {
    Ok(t.exec(
        QueryBuilder::search()
            .to(to)
            .where_()
            .distance(CountComparison::Equal(2))
            .and()
            .key("db_element_id")
            .value(kind)
            .query(),
    )?
    .ids())
}

/// Whether any path of outgoing edges from `from` reaches a `kind` node.
fn reaches(t: Db<'_, '_>, from: DbId, kind: &str) -> anyhow::Result<bool> {
    Ok(t.exec(
        QueryBuilder::search()
            .from(from)
            .limit(1)
            .where_()
            .key("db_element_id")
            .value(kind)
            .query(),
    )?
    .result
        > 0)
}

fn reaches_entry(t: Db<'_, '_>, track: DbId) -> anyhow::Result<bool> {
    Ok(t.exec(
        QueryBuilder::search()
            .from(track)
            .limit(1)
            .where_()
            .key("db_element_id")
            .value("Entry")
            .and()
            .beyond()
            .where_()
            .not()
            .key("db_element_id")
            .value("Entry")
            .end_where()
            .query(),
    )?
    .result
        > 0)
}

fn remove(t: Db<'_, '_>, ids: &[DbId]) -> anyhow::Result<()> {
    if !ids.is_empty() {
        t.exec_mut(QueryBuilder::remove().ids(ids).query())?;
    }
    Ok(())
}

/// Removes tracks or releases with what belongs to them alone: their layers, custom fields,
/// external ids, lyrics and chains, then the labels and covers nothing else holds, listing those
/// covers' files for discarding. The artists
/// they credited lose their credited mark when it was the last credit, and the playlist covers
/// drawn from them are marked for another draw.
fn remove_entities(t: Db<'_, '_>, ids: &[DbId]) -> anyhow::Result<()> {
    let mut labels = HashSet::new();
    let mut covers = HashSet::new();
    let mut artists = HashSet::new();
    let mut profiles = HashSet::new();
    for &id in ids {
        let mut owned = Vec::new();
        for kind in ["ReleaseLayer", "TrackLayer"] {
            for layer in kinds_from(t, id, kind)? {
                labels.extend(kinds_to(t, layer, "Label")?);
                owned.push(layer);
            }
        }
        for kind in ["ProviderCustomFields", "ExternalId", "ExternalIdAlias"] {
            owned.extend(kinds_from(t, id, kind)?);
        }
        for lyrics in kinds_from(t, id, "Lyrics")? {
            for line in kinds_from(t, lyrics, "LyricLine")? {
                owned.extend(kinds_from(t, line, "LyricWord")?);
                owned.push(line);
            }
            owned.push(lyrics);
        }
        owned.extend(kinds_from(t, id, "Chain")?);
        owned.extend(kinds_to(t, id, "Chain")?);
        labels.extend(kinds_from(t, id, "Label")?);
        covers.extend(kinds_from(t, id, "Cover")?);
        artists.extend(kinds_from(t, id, "Artist")?);
        profiles.extend(kinds_to(t, id, "DisplayCoverProfile")?);
        for playlist in kinds_to(t, id, "Playlist")? {
            profiles.extend(kinds_from(t, playlist, "DisplayCoverProfile")?);
        }
        remove(t, &owned)?;
    }
    remove(t, ids)?;

    for label in labels {
        if !kinds_to(t, label, "Release")?.is_empty()
            || !kinds_from(t, label, "ReleaseLayer")?.is_empty()
        {
            continue;
        }
        let mut owned = Vec::new();
        for kind in ["ExternalId", "ExternalIdAlias", "LabelAlias"] {
            owned.extend(kinds_from(t, label, kind)?);
        }
        owned.push(label);
        remove(t, &owned)?;
    }
    for cover in covers {
        let owners = t
            .exec(
                QueryBuilder::search()
                    .to(cover)
                    .limit(1)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .and()
                    .key("owned")
                    .value(1)
                    .query(),
            )?
            .result;
        if owners == 0 {
            discard_cover(t, cover)?;
        }
    }
    for artist in artists {
        let credited = t
            .exec(
                QueryBuilder::search()
                    .to(artist)
                    .limit(1)
                    .where_()
                    .edge()
                    .and()
                    .distance(CountComparison::Equal(1))
                    .and()
                    .key("db_element_id")
                    .value("Credit")
                    .query(),
            )?
            .result
            > 0;
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("credited", credited).into()])
                .ids(artist)
                .query(),
        )?;
    }
    for profile in profiles {
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("dirty", true).into()])
                .ids(profile)
                .query(),
        )?;
    }
    Ok(())
}

fn discard_cover(t: Db<'_, '_>, cover: DbId) -> anyhow::Result<()> {
    let mut values = t
        .exec(QueryBuilder::select().ids(cover).query())?
        .elements
        .pop()
        .map(|element| element.values)
        .unwrap_or_default();
    values.retain(|kv| kv.key == "path".into() || kv.key == "hash".into());
    remove(t, &[cover])?;
    if values.len() != 2 {
        return Ok(());
    }
    values.push(("db_element_id", "DiscardedCover").into());
    let discarded = t
        .exec_mut(QueryBuilder::insert().nodes().values([values]).query())?
        .ids();
    t.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from("cover_discards")
            .to(discarded)
            .query(),
    )?;
    Ok(())
}

fn seed_artist_candidates(t: Db<'_, '_>) -> anyhow::Result<()> {
    let mut reached = HashSet::new();
    for root in ["releases", "tracks"] {
        reached.extend(
            t.exec(
                QueryBuilder::search()
                    .from(root)
                    .where_()
                    .key("db_element_id")
                    .value("Artist")
                    .query(),
            )?
            .ids(),
        );
    }
    let unreached: Vec<DbId> = members(t, "artists", "Artist")?
        .into_iter()
        .filter(|artist| !reached.contains(artist))
        .collect();
    if !unreached.is_empty() {
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("artist_candidates")
                .to(unreached)
                .query(),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use agdb::{
        DbAny,
        DbId,
        QueryBuilder,
    };

    use crate::{
        graph::is_kind,
        orphans::{
            is_artist_candidate,
            testing::{
                assert_collected,
                assert_drained,
                drain_artists,
            },
        },
        test_db::new_test_db,
    };

    /// A `kind` node, a member of `root` when given.
    fn node(db: &mut DbAny, kind: &str, root: Option<&str>) -> anyhow::Result<DbId> {
        let node = db
            .exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[
                        ("db_element_id", kind).into(),
                        ("id", format!("{kind}-{}", crate::nanoid())).into(),
                    ]])
                    .query(),
            )?
            .ids()[0];
        if let Some(root) = root {
            connect(db, root, node)?;
        }
        Ok(node)
    }

    fn connect(
        db: &mut DbAny,
        from: impl Into<agdb::QueryId>,
        to: impl Into<agdb::QueryId>,
    ) -> anyhow::Result<()> {
        db.exec_mut(QueryBuilder::insert().edges().from(from).to(to).query())?;
        Ok(())
    }

    fn connect_with(
        db: &mut DbAny,
        from: DbId,
        to: DbId,
        values: Vec<agdb::DbKeyValue>,
    ) -> anyhow::Result<()> {
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from(from)
                .to(to)
                .values_uniform(values)
                .query(),
        )?;
        Ok(())
    }

    fn credit(db: &mut DbAny, owner: DbId, artist: DbId) -> anyhow::Result<()> {
        connect_with(
            db,
            owner,
            artist,
            vec![
                ("db_element_id", "Credit").into(),
                ("credit_type", "artist").into(),
                ("artist_order", 0_u64).into(),
            ],
        )?;
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("credited", true).into()])
                .ids(artist)
                .query(),
        )?;
        Ok(())
    }

    /// A cover record of the image at `path`.
    fn cover(db: &mut DbAny, path: &str) -> anyhow::Result<DbId> {
        let cover = node(db, "Cover", None)?;
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    ("path", path).into(),
                    ("hash", format!("{path}-hash")).into(),
                ])
                .ids(cover)
                .query(),
        )?;
        Ok(cover)
    }

    fn own_cover(db: &mut DbAny, owner: DbId, cover: DbId) -> anyhow::Result<()> {
        connect_with(
            db,
            owner,
            cover,
            vec![("owned", 1).into(), ("cover_source", "local").into()],
        )
    }

    fn exists(db: &DbAny, id: DbId) -> bool {
        db.exec(QueryBuilder::select().ids(id).query()).is_ok()
    }

    fn flag(db: &DbAny, id: DbId, key: &str) -> anyhow::Result<bool> {
        Ok(db
            .exec(QueryBuilder::select().values([key]).ids(id).query())?
            .elements[0]
            .values[0]
            .value
            .to_bool()?)
    }

    /// A release holding a track that plays a file. Returns the release and the track.
    fn playable(db: &mut DbAny) -> anyhow::Result<(DbId, DbId)> {
        let release = crate::fixtures::insert_release(db, "Kept")?;
        let track = crate::fixtures::insert_track(db, "Kept")?;
        let source = node(db, "TrackSource", Some("track_sources"))?;
        let library = node(db, "Library", None)?;
        let entry = crate::fixtures::insert_file_entry(db, library, "kept.flac")?;
        connect(db, release, track)?;
        connect(db, track, source)?;
        connect(db, source, entry)?;
        Ok((release, track))
    }

    #[test]
    fn what_a_cut_short_scan_left_goes_and_unreached_artists_become_candidates()
    -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let (kept_release, kept_track) = playable(&mut db)?;
        let kept_artist = node(&mut db, "Artist", Some("artists"))?;
        let related = node(&mut db, "Artist", Some("artists"))?;
        credit(&mut db, kept_track, kept_artist)?;
        connect(&mut db, kept_artist, related)?;
        let shared_label = node(&mut db, "Label", Some("labels"))?;
        connect(&mut db, kept_release, shared_label)?;
        let shared_cover = cover(&mut db, "/covers/1/provider.jpg")?;
        own_cover(&mut db, kept_release, shared_cover)?;
        let kept_sheet = node(&mut db, "CueSheet", Some("cue_sheets"))?;
        let kept_cue_track = node(&mut db, "CueTrack", Some("cue_tracks"))?;
        let kept_source = crate::graph::neighbor_ids(&db, kept_track, "TrackSource")?[0];
        connect(&mut db, kept_sheet, kept_cue_track)?;
        connect(&mut db, kept_source, kept_cue_track)?;

        // A track whose file went, alone on its release, with everything a track and a release
        // can own.
        let left_release = node(&mut db, "Release", Some("releases"))?;
        let left_track = node(&mut db, "Track", Some("tracks"))?;
        connect(&mut db, left_release, left_track)?;
        let both_credit = node(&mut db, "Artist", Some("artists"))?;
        let left_artist = node(&mut db, "Artist", Some("artists"))?;
        credit(&mut db, left_track, left_artist)?;
        credit(&mut db, left_release, both_credit)?;
        credit(&mut db, kept_release, both_credit)?;
        let mut owned = Vec::new();
        for (owner, kind) in [
            (left_track, "TrackLayer"),
            (left_track, "ProviderCustomFields"),
            (left_track, "ExternalId"),
            (left_track, "Lyrics"),
            (left_release, "ReleaseLayer"),
            (left_release, "ExternalId"),
            (left_release, "ExternalIdAlias"),
        ] {
            let node = node(&mut db, kind, None)?;
            connect(&mut db, owner, node)?;
            owned.push(node);
        }
        let lyrics = owned[3];
        let line = node(&mut db, "LyricLine", None)?;
        let word = node(&mut db, "LyricWord", None)?;
        connect(&mut db, lyrics, line)?;
        connect(&mut db, line, word)?;
        let chain = node(&mut db, "Chain", Some("chains"))?;
        connect(&mut db, chain, left_track)?;
        let own_label = node(&mut db, "Label", Some("labels"))?;
        let own_label_alias = node(&mut db, "LabelAlias", None)?;
        connect(&mut db, left_release, own_label)?;
        connect(&mut db, own_label, own_label_alias)?;
        let layer_label = node(&mut db, "Label", Some("labels"))?;
        connect(&mut db, layer_label, owned[4])?;
        connect(&mut db, left_release, shared_label)?;
        let own_cover_record = cover(&mut db, "/covers/2/provider.jpg")?;
        own_cover(&mut db, left_release, own_cover_record)?;
        own_cover(&mut db, left_release, shared_cover)?;
        let playlist = node(&mut db, "Playlist", Some("playlists"))?;
        let profile = node(&mut db, "DisplayCoverProfile", None)?;
        connect(&mut db, playlist, left_track)?;
        connect(&mut db, playlist, profile)?;
        connect(&mut db, profile, left_release)?;

        // Sources and cue records nothing plays.
        let left_source = node(&mut db, "TrackSource", Some("track_sources"))?;
        let left_cue_track = node(&mut db, "CueTrack", Some("cue_tracks"))?;
        let left_sheet = node(&mut db, "CueSheet", Some("cue_sheets"))?;
        let emptied_sheet = node(&mut db, "CueSheet", Some("cue_sheets"))?;
        let chained_cue_track = node(&mut db, "CueTrack", Some("cue_tracks"))?;
        connect(&mut db, left_source, left_cue_track)?;
        connect(&mut db, left_sheet, left_cue_track)?;
        connect(&mut db, emptied_sheet, chained_cue_track)?;

        db.transaction_mut(super::run)?;

        let mut gone = vec![
            left_release,
            left_track,
            line,
            word,
            chain,
            own_label,
            own_label_alias,
            layer_label,
            own_cover_record,
            left_source,
            left_cue_track,
            left_sheet,
            emptied_sheet,
            chained_cue_track,
        ];
        gone.extend(owned);
        for id in gone {
            // A freed id may be reissued to the file a removed cover listed.
            assert!(
                !exists(&db, id) || is_kind(&db, id, "DiscardedCover")?,
                "node {} survived",
                id.0
            );
        }
        for id in [
            kept_release,
            kept_track,
            kept_source,
            kept_sheet,
            kept_cue_track,
            shared_label,
            shared_cover,
            playlist,
            profile,
        ] {
            assert!(exists(&db, id), "node {} went", id.0);
        }
        assert_eq!(
            crate::covers::discards::listed(&db)?
                .into_iter()
                .map(|listed| (listed.path, listed.hash))
                .collect::<Vec<_>>(),
            [(
                "/covers/2/provider.jpg".to_string(),
                "/covers/2/provider.jpg-hash".to_string()
            )]
        );
        assert!(flag(&db, profile, "dirty")?);
        assert!(!flag(&db, left_artist, "credited")?);
        assert!(flag(&db, both_credit, "credited")?);
        for (artist, candidate) in [
            (kept_artist, false),
            (related, false),
            (both_credit, false),
            (left_artist, true),
        ] {
            assert_eq!(is_artist_candidate(&db, artist)?, candidate);
        }
        assert_collected(&db)?;

        assert_eq!(drain_artists(&mut db)?, 1);
        assert!(!is_kind(&db, left_artist, "Artist")?);
        assert_drained(&db)
    }
}
