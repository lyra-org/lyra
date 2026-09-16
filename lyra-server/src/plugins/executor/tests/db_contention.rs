use super::*;
use std::{
    future::Future,
    process::Command,
    task::Context as TaskContext,
    time::{
        Duration,
        Instant,
    },
};

// A regressed synchronous binding can deadlock the entire executor thread. Isolate
// these tests so failure terminates the child rather than hanging the test suite.
fn bounded(test: fn() -> Result<()>) -> Result<()> {
    let thread = std::thread::current();
    let name = thread.name().context("test has a name")?;
    if std::env::var("LYRA_DB_CONTENTION_TEST").as_deref() == Ok(name) {
        return test();
    }
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", name, "--nocapture"])
        .env("LYRA_DB_CONTENTION_TEST", name)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait()? {
            anyhow::ensure!(status.success(), "contention test failed: {status}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("plugin executor stalled under database contention");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn fixture() -> Result<(PluginExecutor, crate::plugins::db::DbAsync)> {
    let db = Arc::new(tokio::sync::RwLock::new(
        crate::plugins::db::test_db::new_test_db()?,
    ));
    let runtime = PluginExecutor::with_database(
        Arc::from(vec![manifest(
            "demo",
            &[
                "harmony.task",
                "lyra.datastore",
                "lyra.tracks",
                "lyra.genres",
            ],
        )]),
        default_server_info(),
        db.clone(),
    )?;
    runtime.run_plugin_source(
        "demo",
        "init.luau",
        &br#"
        task = require("@harmony/task")
        store = require("@lyra/datastore").get_or_create("contention")
        tracks = require("@lyra/tracks")
        genres = require("@lyra/genres")
    "#[..],
    )?;
    Ok((runtime, db))
}

#[test]
fn database_waits_yield_to_other_plugin_tasks() -> Result<()> {
    bounded(|| {
        let (runtime, db) = fixture()?;
        let held_writer = futures::executor::block_on(db.write());
        runtime.run_plugin_source(
            "demo",
            "reader.luau",
            &br#"
            task.spawn(function()
                tracks.list()
                reader_done = true
            end)
        "#[..],
        )?;
        runtime.poll_background_tasks();
        runtime.run_plugin_source(
            "demo",
            "writer.luau",
            &br#"
            task.spawn(function()
                store:set_many({first = 1, second = {value = 2}})
                writer_done = true
            end)
        "#[..],
        )?;
        runtime.poll_background_tasks();
        runtime.run_plugin_source(
            "demo",
            "following-readers.luau",
            &br#"
            task.spawn(function()
                local values = store:get_many({"first", "missing", "second"})
                assert(values[1] == 1 and values[2] == nil and values[3].value == 2)
                datastore_reader_done = true
            end)
            task.spawn(function()
                genres.find_by_name("missing")
                genre_reader_done = true
            end)
        "#[..],
        )?;
        runtime.poll_background_tasks();
        assert_eq!(runtime.eval_plugin_source("demo", "progress.luau",
            &b"return 42, reader_done, writer_done, datastore_reader_done, genre_reader_done"[..])?,
            vec![luau::Value::Number(42.0), luau::Value::Nil, luau::Value::Nil, luau::Value::Nil, luau::Value::Nil]);

        let mut api_reader = Box::pin(db.read());
        let waker = futures::task::noop_waker();
        let mut context = TaskContext::from_waker(&waker);
        assert!(api_reader.as_mut().poll(&mut context).is_pending());
        drop(held_writer);
        for _ in 0..8 {
            runtime.poll_background_tasks();
        }
        assert!(api_reader.as_mut().poll(&mut context).is_ready());
        assert_eq!(
            runtime.eval_plugin_source(
                "demo",
                "done.luau",
                &b"return reader_done, writer_done, datastore_reader_done, genre_reader_done"[..]
            )?,
            vec![luau::Value::Boolean(true); 4]
        );
        Ok(())
    })
}

#[test]
fn cancelling_database_waits_releases_reserved_permits() -> Result<()> {
    bounded(|| {
        for source in [
            "return task.spawn(function() store:set_many({cancelled = true}); resumed = true end)",
            "return task.spawn(function() store:get('cancelled'); resumed = true end)",
        ] {
            let (runtime, db) = fixture()?;
            let held_writer = futures::executor::block_on(db.write());
            let values = runtime.eval_plugin_source("demo", "cancelled.luau", source.as_bytes())?;
            let luau::Value::Thread(thread) = &values[0] else {
                anyhow::bail!("task.spawn did not return a thread");
            };
            runtime.poll_background_tasks();
            drop(held_writer);
            assert!(
                db.try_write().is_err(),
                "pending coroutine must have reserved permits"
            );
            assert!(
                runtime
                    .vm
                    .data()
                    .get::<LocalScheduler>()?
                    .cancel_luau_thread(thread)
            );
            let guard = db
                .try_write()
                .context("cancelled future must return its reserved permits")?;
            drop(guard);
            runtime.poll_background_tasks();
            assert_eq!(
                runtime.eval_plugin_source(
                    "demo",
                    "check.luau",
                    &b"return resumed, store:get('cancelled'), 42"[..]
                )?,
                vec![
                    luau::Value::Nil,
                    luau::Value::Nil,
                    luau::Value::Number(42.0)
                ]
            );
        }
        Ok(())
    })
}
