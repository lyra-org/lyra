// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Reported playbacks kept their session key inside a JSON object under
//! `reported`; it becomes the plain string `reported_session_key`.

use agdb::{
    DbAnyTransactionMut,
    QueryBuilder,
};

pub(super) fn run(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
    let playbacks = t.exec(
        QueryBuilder::select()
            .values(["reported"])
            .search()
            .from("playbacks")
            .where_()
            .neighbor()
            .and()
            .key("db_element_id")
            .value("Playback")
            .and()
            .keys("reported")
            .query(),
    )?;
    for playback in playbacks.elements {
        let source: serde_json::Value = serde_json::from_str(playback.values[0].value.string()?)?;
        let Some(session_key) = source["session_key"].as_str() else {
            anyhow::bail!("playback {} has no reported session key", playback.id.0);
        };
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("reported_session_key", session_key).into()])
                .ids(playback.id)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::remove()
                .values(["reported"])
                .ids(playback.id)
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
        DbKeyValue,
        QueryBuilder,
    };

    use crate::{
        playbacks::{
            PlaybackSource,
            get_by_id,
        },
        test_db::{
            new_test_db,
            stored_keys,
        },
    };

    fn seed(db: &mut DbAny, id: &str, source: Vec<DbKeyValue>) -> anyhow::Result<DbId> {
        let mut values: Vec<DbKeyValue> = vec![
            ("db_element_id", "Playback").into(),
            ("id", id).into(),
            ("created_at_ms", 1_u64).into(),
            ("updated_at_ms", 1_u64).into(),
        ];
        values.extend(source);
        let playback = db
            .exec_mut(QueryBuilder::insert().nodes().values([values]).query())?
            .ids()[0];
        db.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("playbacks")
                .to(playback)
                .query(),
        )?;
        Ok(playback)
    }

    #[test]
    fn a_reported_playback_keeps_its_session_key() -> anyhow::Result<()> {
        let mut db = new_test_db()?;
        let reported = seed(
            &mut db,
            "reported",
            vec![("reported", r#"{"session_key":"device:1"}"#).into()],
        )?;
        let queued = seed(
            &mut db,
            "queued",
            vec![
                ("queue_revision", 1_u64).into(),
                ("queue_track_ids", Vec::<String>::new()).into(),
                ("queue_current_index", 0_u64).into(),
                ("queue_repeat_mode", "none").into(),
                ("queue_shuffle_enabled", false).into(),
            ],
        )?;

        db.transaction_mut(super::run)?;

        let playback = get_by_id(&db, reported)?.expect("reported playback");
        assert!(matches!(
            &playback.source,
            PlaybackSource::Reported(source) if source.session_key() == "device:1"
        ));
        assert!(!stored_keys(&db, reported)?.contains("reported"));
        let playback = get_by_id(&db, queued)?.expect("queued playback");
        assert!(matches!(playback.source, PlaybackSource::Queue(_)));
        Ok(())
    }
}
