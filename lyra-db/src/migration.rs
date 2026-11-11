// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Databases open on the build that stamped them, on later builds of the same
//! cycle, and on the release after `PREVIOUS_RELEASE`'s; `STEPS` brings them
//! to the current shape.

use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::Read,
    path::{
        Path,
        PathBuf,
    },
};

use agdb::{
    DbAny,
    DbAnyTransactionMut,
    DbError,
    DbValue,
    QueryBuilder,
};
use semver::Version;

use super::bootstrap::{
    DbConfig,
    DbKind,
};

mod listen_totals;
mod reported_session_key;

/// The release before this version; its databases run every step in `STEPS`.
pub(crate) const PREVIOUS_RELEASE: Option<&str> = Some("0.0.1");

pub(crate) type Step = fn(&mut DbAnyTransactionMut<'_>) -> anyhow::Result<()>;

/// Rewrites stored data from the shape `PREVIOUS_RELEASE` left it in, one
/// change at a time. Append only: databases record how many steps they ran.
pub(crate) const STEPS: &[Step] = &[reported_session_key::run, listen_totals::run];

const STAMP_ALIAS: &str = "server";
const VERSION_KEY: &str = "lyra_version";
const STEPS_KEY: &str = "lyra_migration_steps";

#[derive(Clone, Copy)]
pub(crate) struct Migration {
    current: &'static str,
    previous: Option<&'static str>,
    steps: &'static [Step],
}

pub(crate) const TO_CURRENT: Migration = Migration {
    current: env!("CARGO_PKG_VERSION"),
    previous: PREVIOUS_RELEASE,
    steps: STEPS,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Plan {
    /// Already current: no step runs and the stamp is not rewritten.
    Open,
    /// Empty, or every step already ran: only the stamp is rewritten.
    Stamp { from: Option<String> },
    /// `steps` is the recorded count, absent for a previous release without one.
    Migrate {
        from: String,
        steps: Option<u64>,
        first_step: usize,
    },
}

enum Found {
    Empty,
    Unstamped,
    Stamp {
        version: String,
        steps: Result<Option<u64>, DbError>,
    },
}

impl Migration {
    pub(crate) fn plan(&self, db: &DbAny) -> anyhow::Result<Plan> {
        let current = Version::parse(self.current)?;
        let previous = self.previous.map(Version::parse).transpose()?;
        let (found, steps) = match read_stamp(db)? {
            Found::Empty => return Ok(Plan::Stamp { from: None }),
            Found::Unstamped => anyhow::bail!("database has no Lyra version; {}", self.accepted()),
            Found::Stamp { version, steps } => (version, steps),
        };
        let Ok(version) = Version::parse(&found) else {
            anyhow::bail!("database is from Lyra {found}; {}", self.accepted());
        };
        let recorded = steps.as_ref().ok().copied().flatten();
        let first_step = if previous.as_ref() == Some(&version) {
            0
        } else if version <= current && previous.as_ref().is_none_or(|previous| *previous < version)
        {
            let steps = steps.map_err(|_| {
                anyhow::anyhow!("database from Lyra {found} has an unreadable migration step count")
            })?;
            let Some(steps) = steps else {
                anyhow::bail!("database from Lyra {found} has no migration step count");
            };
            if steps > self.steps.len() as u64 {
                anyhow::bail!(
                    "database from Lyra {found} ran {steps} migration steps; Lyra {} has {}",
                    self.current,
                    self.steps.len()
                );
            }
            steps as usize
        } else {
            anyhow::bail!("database is from Lyra {found}; {}", self.accepted());
        };
        Ok(if first_step < self.steps.len() {
            Plan::Migrate {
                from: found,
                steps: recorded,
                first_step,
            }
        } else if version == current {
            Plan::Open
        } else {
            Plan::Stamp { from: Some(found) }
        })
    }

    /// Fails unless the database opens as it is, with no stamp to write.
    pub(crate) fn ensure_current(&self, db: &DbAny) -> anyhow::Result<()> {
        match self.plan(db)? {
            Plan::Open => Ok(()),
            Plan::Stamp { from: None } => anyhow::bail!("database is empty"),
            Plan::Stamp { from: Some(from) } => {
                anyhow::bail!("database is from Lyra {from}, not {}", self.current)
            }
            Plan::Migrate {
                from, first_step, ..
            } => anyhow::bail!(
                "database from Lyra {from} has {} pending migration steps",
                self.steps.len() - first_step
            ),
        }
    }

    fn accepted(&self) -> String {
        match self.previous {
            Some(previous) => format!(
                "Lyra {} opens databases from {previous} through {}",
                self.current, self.current
            ),
            None => format!(
                "Lyra {} opens databases up to {}",
                self.current, self.current
            ),
        }
    }

    pub(crate) fn apply(&self, t: &mut DbAnyTransactionMut<'_>, plan: &Plan) -> anyhow::Result<()> {
        match plan {
            Plan::Open => return Ok(()),
            Plan::Stamp { .. } => {}
            Plan::Migrate {
                from, first_step, ..
            } => {
                tracing::info!(
                    from = from.as_str(),
                    to = self.current,
                    steps = self.steps.len() - first_step,
                    "migrating db"
                );
                for step in &self.steps[*first_step..] {
                    step(t)?;
                }
            }
        }
        self.write_stamp(t)
    }

    fn write_stamp(&self, t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        t.exec_mut(QueryBuilder::insert().nodes().aliases(STAMP_ALIAS).query())?;
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([
                    (VERSION_KEY, self.current).into(),
                    (STEPS_KEY, self.steps.len() as u64).into(),
                ])
                .ids(STAMP_ALIAS)
                .query(),
        )?;
        Ok(())
    }
}

/// Copies the database file and its write-ahead log beside them, named after
/// the stamp they hold: `<name>.from-<version>.steps-<count>`. An existing
/// backup of that name with the same contents is kept as it is; one with other
/// contents gets a numbered sibling. Call only while the database is closed.
pub(crate) fn back_up(config: &DbConfig, version: &str, steps: Option<u64>) -> anyhow::Result<()> {
    if matches!(config.kind, DbKind::Memory) {
        return Ok(());
    }
    let mut label = format!("from-{version}");
    if let Some(steps) = steps {
        label.push_str(&format!(".steps-{steps}"));
    }
    let sources = [config.path.clone(), write_ahead_log(&config.path)];
    let mut attempt = 0;
    loop {
        let suffix = match attempt {
            0 => label.clone(),
            attempt => format!("{label}.{attempt}"),
        };
        let targets = sources.clone().map(|source| backup_path(&source, &suffix));
        let staged = targets
            .clone()
            .map(|target| backup_path(&target, STAGED_SUFFIX));
        for staged in &staged {
            let _ = std::fs::remove_file(staged);
        }
        let Some(missing) = missing_from_backup(&sources, &targets)? else {
            attempt += 1;
            continue;
        };
        if missing.is_empty() {
            tracing::info!(path = %targets[0].display(), "db already backed up");
            return Ok(());
        }
        let copies = missing
            .into_iter()
            .map(|file| Copy {
                source: &sources[file],
                staged: &staged[file],
                target: &targets[file],
            })
            .collect::<Vec<_>>();
        let copied = copy_all(&copies);
        if copied.is_err() {
            for copy in &copies {
                let _ = std::fs::remove_file(copy.staged);
                let _ = std::fs::remove_file(copy.target);
            }
        }
        copied?;
        sync_directory(&config.path);
        return Ok(());
    }
}

/// A copy is written under this suffix and renamed once complete, so a file
/// named as a backup is always a whole one. A copy cut short leaves the staged
/// file behind; the next attempt removes it.
const STAGED_SUFFIX: &str = "tmp";

struct Copy<'a> {
    source: &'a Path,
    staged: &'a Path,
    target: &'a Path,
}

fn backup_path(source: &Path, suffix: &str) -> PathBuf {
    let mut name = source.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(suffix);
    source.with_file_name(name)
}

/// Names no copy as a backup until every one is complete.
fn copy_all(copies: &[Copy<'_>]) -> anyhow::Result<()> {
    let failed = |copy: &Copy<'_>, error: std::io::Error| {
        anyhow::anyhow!(
            "failed to back up '{}' to '{}': {error}",
            copy.source.display(),
            copy.target.display()
        )
    };
    for copy in copies {
        tracing::info!(
            from = %copy.source.display(),
            to = %copy.target.display(),
            "backing up db before migration"
        );
        copy_durably(copy.source, copy.staged).map_err(|error| failed(copy, error))?;
    }
    for copy in copies {
        std::fs::rename(copy.staged, copy.target).map_err(|error| failed(copy, error))?;
    }
    Ok(())
}

/// The sources `targets` still lack to be their backup, none when it is
/// complete. `None` when the targets hold something other than the sources. A
/// backup lacks a file when its process died between renaming the copies.
fn missing_from_backup(
    sources: &[PathBuf; 2],
    targets: &[PathBuf; 2],
) -> anyhow::Result<Option<Vec<usize>>> {
    let mut missing = Vec::new();
    for (file, (source, target)) in sources.iter().zip(targets).enumerate() {
        match (source.is_file(), target.exists()) {
            (false, false) => {}
            (true, false) => missing.push(file),
            (true, true) => {
                let same = same_contents(source, target).map_err(|error| {
                    anyhow::anyhow!(
                        "failed to compare '{}' with '{}': {error}",
                        source.display(),
                        target.display()
                    )
                })?;
                if !same {
                    return Ok(None);
                }
            }
            (false, true) => return Ok(None),
        }
    }
    Ok(Some(missing))
}

fn same_contents(a: &Path, b: &Path) -> std::io::Result<bool> {
    let mut remaining = a.metadata()?.len();
    if remaining != b.metadata()?.len() {
        return Ok(false);
    }
    let (mut a, mut b) = (File::open(a)?, File::open(b)?);
    let (mut chunk_a, mut chunk_b) = (vec![0; 1 << 16], vec![0; 1 << 16]);
    while remaining > 0 {
        let len = remaining.min(chunk_a.len() as u64) as usize;
        a.read_exact(&mut chunk_a[..len])?;
        b.read_exact(&mut chunk_b[..len])?;
        if chunk_a[..len] != chunk_b[..len] {
            return Ok(false);
        }
        remaining -= len as u64;
    }
    Ok(true)
}

fn copy_durably(source: &Path, target: &Path) -> std::io::Result<()> {
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new();
    output.write(true).create_new(true);
    // The copy gets the source's mode as it is created: some mounts let the
    // server write files it may not change the mode of afterwards.
    #[cfg(unix)]
    {
        use std::os::unix::fs::{
            OpenOptionsExt,
            PermissionsExt,
        };
        output.mode(input.metadata()?.permissions().mode() & 0o7777);
    }
    let mut output = output.open(target)?;
    std::io::copy(&mut input, &mut output)?;
    output.sync_all()
}

/// Makes the backups' names durable where the filesystem can. The copies are
/// already complete and synced, so a filesystem that cannot is no reason to
/// refuse the migration.
#[cfg(unix)]
fn sync_directory(path: &Path) {
    let directory = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    if let Err(error) = File::open(directory).and_then(|directory| directory.sync_all()) {
        tracing::warn!(
            path = %directory.display(),
            %error,
            "failed to sync db backups to disk"
        );
    }
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) {}

fn read_stamp(db: &DbAny) -> anyhow::Result<Found> {
    if db.exec(QueryBuilder::select().node_count().query())?.result == 0 {
        return Ok(Found::Empty);
    }
    let aliases = db.exec(QueryBuilder::select().aliases().query())?;
    let has_stamp_alias = aliases.elements.iter().any(|element| {
        element
            .values
            .iter()
            .any(|value| value.value.to_string() == STAMP_ALIAS)
    });
    if !has_stamp_alias {
        return Ok(Found::Unstamped);
    }
    let result = db.exec(QueryBuilder::select().ids(STAMP_ALIAS).query())?;
    let value = |key: &str| {
        result.elements.first().and_then(|element| {
            element
                .values
                .iter()
                .find(|value| value.key == key.into())
                .map(|value| &value.value)
        })
    };
    let Some(version) = value(VERSION_KEY) else {
        return Ok(Found::Unstamped);
    };
    let Ok(version) = version.string() else {
        anyhow::bail!("database has an unreadable Lyra version");
    };
    Ok(Found::Stamp {
        version: version.clone(),
        steps: value(STEPS_KEY).map(DbValue::to_u64).transpose(),
    })
}

/// agdb keeps its write-ahead log beside the database as `.<name>`.
fn write_ahead_log(path: &Path) -> PathBuf {
    let mut name = std::ffi::OsString::from(".");
    name.push(path.file_name().unwrap_or_default());
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roots::{
        self,
        Root,
    };
    use crate::{
        bootstrap,
        test_db::TestDb,
    };

    const PREVIOUS: &str = "0.0.2";
    const CURRENT: &str = "0.0.3-dev.2";

    const NONE: &[Step] = &[];
    const A: &[Step] = &[step_a];
    const AB: &[Step] = &[step_a, step_b];
    const ABC: &[Step] = &[step_a, step_b, step_c];
    const FAILING: &[Step] = &[step_a, fail];
    const NEEDS_SCHEMA: &[Step] = &[needs_schema];

    /// Marks the stamp node with `key`, failing if an earlier run already did.
    fn mark(t: &mut DbAnyTransactionMut<'_>, key: &str) -> anyhow::Result<()> {
        let result = t.exec(QueryBuilder::select().ids(STAMP_ALIAS).query())?;
        anyhow::ensure!(
            result.elements[0]
                .values
                .iter()
                .all(|value| value.key != key.into()),
            "{key} ran twice"
        );
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(key, true).into()])
                .ids(STAMP_ALIAS)
                .query(),
        )?;
        Ok(())
    }

    fn step_a(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        mark(t, "step_a")
    }

    fn step_b(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        mark(t, "step_b")
    }

    fn step_c(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        mark(t, "step_c")
    }

    fn fail(_t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        anyhow::bail!("step failed")
    }

    fn needs_schema(t: &mut DbAnyTransactionMut<'_>) -> anyhow::Result<()> {
        t.exec_mut(
            QueryBuilder::insert()
                .edges()
                .from("users")
                .to(STAMP_ALIAS)
                .query(),
        )?;
        t.exec_mut(
            QueryBuilder::insert()
                .values_uniform([("id", "stamp").into()])
                .ids(STAMP_ALIAS)
                .query(),
        )?;
        let found = t.exec(QueryBuilder::search().index("id").value("stamp").query())?;
        anyhow::ensure!(found.result == 1, "id index is missing");
        step_a(t)
    }

    fn build(current: &'static str, steps: &'static [Step]) -> Migration {
        Migration {
            current,
            previous: Some(PREVIOUS),
            steps,
        }
    }

    fn set_stamp(db: &mut DbAny, version: &str, steps: Option<u64>) -> anyhow::Result<()> {
        db.transaction_mut(|t| -> anyhow::Result<()> {
            t.exec_mut(QueryBuilder::insert().nodes().aliases(STAMP_ALIAS).query())?;
            t.exec_mut(
                QueryBuilder::insert()
                    .values_uniform([(VERSION_KEY, version).into()])
                    .ids(STAMP_ALIAS)
                    .query(),
            )?;
            match steps {
                Some(steps) => t.exec_mut(
                    QueryBuilder::insert()
                        .values_uniform([(STEPS_KEY, steps).into()])
                        .ids(STAMP_ALIAS)
                        .query(),
                )?,
                None => t.exec_mut(
                    QueryBuilder::remove()
                        .values([STEPS_KEY])
                        .ids(STAMP_ALIAS)
                        .query(),
                )?,
            };
            Ok(())
        })
    }

    /// A database on the current schema stamped with `version` and `steps`.
    fn seeded(version: &str, steps: Option<u64>) -> anyhow::Result<DbAny> {
        let mut db = TestDb::new()?.into_inner();
        bootstrap::initialize_with(&mut db, &build(CURRENT, NONE))?;
        set_stamp(&mut db, version, steps)?;
        Ok(db)
    }

    /// A database holding only a stamp, without the current root aliases and indexes.
    fn bare(version: &str, steps: Option<u64>) -> anyhow::Result<DbAny> {
        let mut db = TestDb::new()?.into_inner();
        set_stamp(&mut db, version, steps)?;
        Ok(db)
    }

    fn stamp_of(db: &DbAny) -> anyhow::Result<Option<(String, Option<u64>)>> {
        Ok(match read_stamp(db)? {
            Found::Stamp { version, steps } => Some((version, steps?)),
            Found::Empty | Found::Unstamped => None,
        })
    }

    fn stamp(version: &str, steps: u64) -> Option<(String, Option<u64>)> {
        Some((version.to_string(), Some(steps)))
    }

    fn has_alias(db: &DbAny, alias: &str) -> anyhow::Result<bool> {
        let aliases = db.exec(QueryBuilder::select().aliases().query())?;
        Ok(aliases.elements.iter().any(|element| {
            element
                .values
                .iter()
                .any(|value| value.value.to_string() == alias)
        }))
    }

    fn has_index(db: &DbAny, name: &str) -> anyhow::Result<bool> {
        let result = db.exec(QueryBuilder::select().indexes().query())?;
        Ok(result.elements.iter().any(|element| {
            element
                .values
                .iter()
                .any(|value| value.key.to_string() == name)
        }))
    }

    fn ran(db: &DbAny) -> anyhow::Result<Vec<String>> {
        let result = db.exec(QueryBuilder::select().ids(STAMP_ALIAS).query())?;
        Ok(result.elements[0]
            .values
            .iter()
            .map(|value| value.key.to_string())
            .filter(|key| key.starts_with("step_"))
            .collect())
    }

    fn refusal(db: &mut DbAny, migration: &Migration) -> String {
        bootstrap::initialize_with(db, migration)
            .expect_err("database should be refused")
            .to_string()
    }

    #[test]
    fn fresh_database_is_stamped_with_the_step_count_without_running_steps() -> anyhow::Result<()> {
        let db = TestDb::initialized()?.into_inner();
        assert_eq!(
            stamp_of(&db)?,
            stamp(env!("CARGO_PKG_VERSION"), STEPS.len() as u64)
        );

        let mut db = TestDb::new()?.into_inner();
        bootstrap::initialize_with(&mut db, &build(CURRENT, AB))?;
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        assert!(ran(&db)?.is_empty());
        Ok(())
    }

    #[test]
    fn database_with_the_current_stamp_opens() -> anyhow::Result<()> {
        let mut db = TestDb::initialized()?.into_inner();
        bootstrap::initialize(&mut db)?;
        assert_eq!(TO_CURRENT.plan(&db)?, Plan::Open);

        let db = seeded(CURRENT, Some(2))?;
        assert_eq!(build(CURRENT, AB).plan(&db)?, Plan::Open);
        Ok(())
    }

    #[test]
    fn later_build_of_the_cycle_runs_only_the_remaining_steps() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        bootstrap::initialize_with(&mut db, &build("0.0.3-dev.0", NONE))?;
        bootstrap::initialize_with(&mut db, &build("0.0.3-dev.1", A))?;
        assert_eq!(ran(&db)?, ["step_a"]);
        assert_eq!(stamp_of(&db)?, stamp("0.0.3-dev.1", 1));

        bootstrap::initialize_with(&mut db, &build(CURRENT, ABC))?;
        assert_eq!(ran(&db)?, ["step_a", "step_b", "step_c"]);
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 3));
        Ok(())
    }

    #[test]
    fn release_of_the_cycle_upgrades_its_development_builds() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        bootstrap::initialize_with(&mut db, &build(PREVIOUS, NONE))?;
        bootstrap::initialize_with(&mut db, &build(CURRENT, A))?;
        bootstrap::initialize_with(&mut db, &build("0.0.3", AB))?;
        assert_eq!(ran(&db)?, ["step_a", "step_b"]);
        assert_eq!(stamp_of(&db)?, stamp("0.0.3", 2));
        Ok(())
    }

    #[test]
    fn previous_release_runs_every_step_whatever_its_count() -> anyhow::Result<()> {
        for steps in [None, Some(0), Some(2), Some(9)] {
            let mut db = seeded(PREVIOUS, steps)?;
            bootstrap::initialize_with(&mut db, &build(CURRENT, AB))?;
            assert_eq!(ran(&db)?, ["step_a", "step_b"]);
            assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        }
        Ok(())
    }

    #[test]
    fn previous_release_without_steps_is_restamped() -> anyhow::Result<()> {
        let mut db = seeded(PREVIOUS, Some(0))?;
        assert_eq!(
            build(CURRENT, NONE).plan(&db)?,
            Plan::Stamp {
                from: Some(PREVIOUS.to_string())
            }
        );
        bootstrap::initialize_with(&mut db, &build(CURRENT, NONE))?;
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 0));
        Ok(())
    }

    #[test]
    fn same_version_with_fewer_steps_runs_the_rest() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        bootstrap::initialize_with(&mut db, &build(CURRENT, A))?;
        bootstrap::initialize_with(&mut db, &build(CURRENT, AB))?;
        assert_eq!(ran(&db)?, ["step_b"]);
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        Ok(())
    }

    #[test]
    fn earlier_build_with_every_step_is_only_restamped() -> anyhow::Result<()> {
        let mut db = seeded("0.0.3-dev.0", Some(2))?;
        assert_eq!(
            build(CURRENT, AB).plan(&db)?,
            Plan::Stamp {
                from: Some("0.0.3-dev.0".to_string())
            }
        );
        bootstrap::initialize_with(&mut db, &build(CURRENT, AB))?;
        assert!(ran(&db)?.is_empty());
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        Ok(())
    }

    #[test]
    fn databases_outside_the_upgrade_path_are_refused() -> anyhow::Result<()> {
        for version in ["0.0.3-dev.3", "0.0.3", "0.0.2-dev.3", "0.0.1", "nonsense"] {
            let mut db = bare(version, Some(0))?;
            assert_eq!(
                refusal(&mut db, &build(CURRENT, AB)),
                format!(
                    "database is from Lyra {version}; Lyra 0.0.3-dev.2 opens databases from 0.0.2 through 0.0.3-dev.2"
                )
            );
            assert_eq!(stamp_of(&db)?, stamp(version, 0));
            assert!(!has_alias(&db, "users")?);
        }
        Ok(())
    }

    #[test]
    fn database_that_ran_more_steps_is_refused() -> anyhow::Result<()> {
        let mut db = seeded("0.0.3-dev.1", Some(3))?;
        assert_eq!(
            refusal(&mut db, &build(CURRENT, AB)),
            "database from Lyra 0.0.3-dev.1 ran 3 migration steps; Lyra 0.0.3-dev.2 has 2"
        );
        assert_eq!(stamp_of(&db)?, stamp("0.0.3-dev.1", 3));
        Ok(())
    }

    #[test]
    fn cycle_database_without_a_step_count_is_refused() -> anyhow::Result<()> {
        let mut db = seeded("0.0.3-dev.0", None)?;
        assert_eq!(
            refusal(&mut db, &build(CURRENT, AB)),
            "database from Lyra 0.0.3-dev.0 has no migration step count"
        );
        Ok(())
    }

    #[test]
    fn without_a_previous_release_every_earlier_version_upgrades() -> anyhow::Result<()> {
        let migration = Migration {
            previous: None,
            ..build(CURRENT, AB)
        };
        let mut db = seeded("0.0.1", Some(1))?;
        bootstrap::initialize_with(&mut db, &migration)?;
        assert_eq!(ran(&db)?, ["step_b"]);

        let mut db = seeded("0.0.4", Some(0))?;
        assert_eq!(
            refusal(&mut db, &migration),
            "database is from Lyra 0.0.4; Lyra 0.0.3-dev.2 opens databases up to 0.0.3-dev.2"
        );
        Ok(())
    }

    #[test]
    fn unstamped_database_with_data_is_refused() -> anyhow::Result<()> {
        let mut db = TestDb::with_root_aliases(&["tracks"])?.into_inner();
        let error = bootstrap::initialize(&mut db).expect_err("unstamped data should be refused");
        assert!(
            error
                .to_string()
                .starts_with("database has no Lyra version; ")
        );
        Ok(())
    }

    #[test]
    fn unaliased_data_is_refused() -> anyhow::Result<()> {
        let mut db = TestDb::new()?.into_inner();
        db.exec_mut(
            QueryBuilder::insert()
                .nodes()
                .values([[("title", "song").into()]])
                .query(),
        )?;
        let error = bootstrap::initialize(&mut db).expect_err("unaliased data should be refused");
        assert!(
            error
                .to_string()
                .starts_with("database has no Lyra version; ")
        );
        Ok(())
    }

    #[test]
    fn steps_run_on_the_current_schema() -> anyhow::Result<()> {
        let mut db = bare(PREVIOUS, None)?;
        assert!(!has_alias(&db, "users")?);
        assert!(!has_index(&db, "id")?);
        bootstrap::initialize_with(&mut db, &build(CURRENT, NEEDS_SCHEMA))?;
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 1));
        assert_eq!(ran(&db)?, ["step_a"]);
        Ok(())
    }

    #[test]
    fn failed_migration_changes_nothing() -> anyhow::Result<()> {
        let mut db = bare(PREVIOUS, Some(0))?;
        let error = refusal(&mut db, &build(CURRENT, FAILING));
        assert_eq!(error, "step failed");
        assert_eq!(stamp_of(&db)?, stamp(PREVIOUS, 0));
        assert!(ran(&db)?.is_empty());
        assert!(!has_alias(&db, "root")?);
        assert!(!has_alias(&db, "users")?);
        assert!(!has_index(&db, "id")?);
        Ok(())
    }

    fn file_config(dir: &Path, kind: DbKind) -> DbConfig {
        DbConfig {
            kind,
            path: dir.join("lyra.db"),
        }
    }

    fn backups(dir: &Path) -> anyhow::Result<Vec<String>> {
        let mut names = std::fs::read_dir(dir)?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.retain(|name| name.contains(".from-"));
        names.sort();
        Ok(names)
    }

    #[test]
    fn migration_backs_up_file_databases_first() -> anyhow::Result<()> {
        for kind in [DbKind::File, DbKind::Mmap] {
            let dir = tempfile::tempdir()?;
            let config = file_config(dir.path(), kind);
            drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
            let original = std::fs::read(&config.path)?;

            let created = bootstrap::create_with(&config, &build(CURRENT, A))?;
            let db = created.db.try_read()?;
            assert_eq!(stamp_of(&db)?, stamp(CURRENT, 1));
            assert_eq!(ran(&db)?, ["step_a"]);

            let backup = dir.path().join("lyra.db.from-0.0.2.steps-0");
            assert_eq!(std::fs::read(&backup)?, original);
            let backup_wal = dir.path().join(".lyra.db.from-0.0.2.steps-0");
            assert_eq!(
                backup_wal.is_file(),
                write_ahead_log(&config.path).is_file()
            );

            let restored = bootstrap::open(kind, backup.to_string_lossy().as_ref())?;
            assert_eq!(stamp_of(&restored)?, stamp(PREVIOUS, 0));
            assert!(ran(&restored)?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn backups_are_made_only_when_steps_run() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(
            &config,
            &build("0.0.3-dev.0", NONE),
        )?);
        drop(bootstrap::create_with(
            &config,
            &build("0.0.3-dev.0", NONE),
        )?);
        drop(bootstrap::create_with(
            &config,
            &build("0.0.3-dev.1", NONE),
        )?);
        assert!(backups(dir.path())?.is_empty());

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        let names = backups(dir.path())?;
        assert!(
            names.contains(&"lyra.db.from-0.0.3-dev.1.steps-0".to_string()),
            "{names:?}"
        );
        assert!(
            names
                .iter()
                .all(|name| name.ends_with(".from-0.0.3-dev.1.steps-0")),
            "{names:?}"
        );
        Ok(())
    }

    #[test]
    fn remigrating_the_same_version_keeps_the_earlier_backup() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let previous = std::fs::read(&config.path)?;

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        let current = std::fs::read(&config.path)?;
        drop(bootstrap::create_with(&config, &build(CURRENT, AB))?);

        let names = backups(dir.path())?;
        assert!(
            names.contains(&"lyra.db.from-0.0.2.steps-0".to_string()),
            "{names:?}"
        );
        assert!(
            names.contains(&"lyra.db.from-0.0.3-dev.2.steps-1".to_string()),
            "{names:?}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("lyra.db.from-0.0.2.steps-0"))?,
            previous
        );
        assert_eq!(
            std::fs::read(dir.path().join("lyra.db.from-0.0.3-dev.2.steps-1"))?,
            current
        );
        Ok(())
    }

    #[test]
    fn retried_migration_keeps_one_backup_per_source_state() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        let backup = |n: &str| dir.path().join(format!("lyra.db.from-0.0.2.steps-0{n}"));
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let first = std::fs::read(&config.path)?;
        assert!(bootstrap::create_with(&config, &build(CURRENT, FAILING)).is_err());
        // agdb's rollback leaves the file's bytes changed, though not its data.
        let rolled_back = std::fs::read(&config.path)?;
        for _ in 0..3 {
            assert!(bootstrap::create_with(&config, &build(CURRENT, FAILING)).is_err());
        }
        assert_eq!(std::fs::read(backup(""))?, first);
        assert_eq!(std::fs::read(backup(".1"))?, rolled_back);
        assert!(!backup(".2").exists());

        let mut db = bootstrap::open(DbKind::File, config.path.to_string_lossy().as_ref())?;
        db.exec_mut(QueryBuilder::insert().nodes().count(1).query())?;
        drop(db);
        let used_again = std::fs::read(&config.path)?;
        assert!(bootstrap::create_with(&config, &build(CURRENT, FAILING)).is_err());
        assert_eq!(std::fs::read(backup(""))?, first);
        assert_eq!(std::fs::read(backup(".2"))?, used_again);
        Ok(())
    }

    #[test]
    fn failed_backup_names_nothing_as_one() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let original = std::fs::read(&config.path)?;
        assert!(write_ahead_log(&config.path).is_file());

        // The database copies; its write-ahead log, second, cannot.
        let blocker = dir.path().join(".lyra.db.from-0.0.2.steps-0.tmp");
        std::fs::create_dir(&blocker)?;
        let error = bootstrap::create_with(&config, &build(CURRENT, A))
            .err()
            .expect("the backup should fail");
        assert!(
            error.to_string().starts_with("failed to back up "),
            "{error}"
        );
        assert_eq!(backups(dir.path())?, [".lyra.db.from-0.0.2.steps-0.tmp"]);
        assert_eq!(std::fs::read(&config.path)?, original);

        std::fs::remove_dir(&blocker)?;
        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        assert_eq!(
            backups(dir.path())?,
            [".lyra.db.from-0.0.2.steps-0", "lyra.db.from-0.0.2.steps-0"]
        );
        assert_eq!(
            std::fs::read(dir.path().join("lyra.db.from-0.0.2.steps-0"))?,
            original
        );
        Ok(())
    }

    #[test]
    fn backup_replaces_a_copy_cut_short() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let original = std::fs::read(&config.path)?;
        std::fs::write(dir.path().join("lyra.db.from-0.0.2.steps-0.tmp"), "partial")?;

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        let names = backups(dir.path())?;
        assert!(
            names.iter().all(|name| !name.ends_with(".tmp")),
            "{names:?}"
        );
        assert_eq!(
            std::fs::read(dir.path().join("lyra.db.from-0.0.2.steps-0"))?,
            original
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn backup_is_as_private_as_the_database() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        std::fs::set_permissions(&config.path, std::fs::Permissions::from_mode(0o600))?;
        let cut_short = dir.path().join("lyra.db.from-0.0.2.steps-0.tmp");
        std::fs::write(&cut_short, "partial")?;
        std::fs::set_permissions(&cut_short, std::fs::Permissions::from_mode(0o644))?;

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        let backup = dir.path().join("lyra.db.from-0.0.2.steps-0");
        assert_eq!(backup.metadata()?.permissions().mode() & 0o777, 0o600);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn backup_does_not_write_through_a_link() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let original = std::fs::read(&config.path)?;
        let bystander = dir.path().join("bystander");
        std::fs::write(&bystander, "kept")?;
        std::os::unix::fs::symlink(
            &bystander,
            dir.path().join("lyra.db.from-0.0.2.steps-0.tmp"),
        )?;
        std::fs::hard_link(
            &bystander,
            dir.path().join(".lyra.db.from-0.0.2.steps-0.tmp"),
        )?;

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        assert_eq!(std::fs::read_to_string(&bystander)?, "kept");
        let backup = dir.path().join("lyra.db.from-0.0.2.steps-0");
        assert!(!backup.is_symlink());
        assert_eq!(std::fs::read(&backup)?, original);
        Ok(())
    }

    #[test]
    fn backup_cut_short_between_its_files_is_completed() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build(PREVIOUS, NONE))?);
        let original = std::fs::read(&config.path)?;
        let log = std::fs::read(write_ahead_log(&config.path))?;
        std::fs::write(dir.path().join("lyra.db.from-0.0.2.steps-0"), &original)?;
        std::fs::write(
            dir.path().join(".lyra.db.from-0.0.2.steps-0.tmp"),
            "partial",
        )?;

        drop(bootstrap::create_with(&config, &build(CURRENT, A))?);
        assert_eq!(
            backups(dir.path())?,
            [".lyra.db.from-0.0.2.steps-0", "lyra.db.from-0.0.2.steps-0"]
        );
        assert_eq!(
            std::fs::read(dir.path().join("lyra.db.from-0.0.2.steps-0"))?,
            original
        );
        assert_eq!(
            std::fs::read(dir.path().join(".lyra.db.from-0.0.2.steps-0"))?,
            log
        );
        Ok(())
    }

    #[test]
    fn refused_database_is_not_backed_up() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let config = file_config(dir.path(), DbKind::File);
        drop(bootstrap::create_with(&config, &build("0.0.4", NONE))?);
        let before = std::fs::read(&config.path)?;
        assert!(bootstrap::create_with(&config, &build(CURRENT, A)).is_err());
        assert!(backups(dir.path())?.is_empty());
        assert_eq!(std::fs::read(&config.path)?, before);
        Ok(())
    }

    /// Every node with its values, except the stamp.
    fn user_data(db: &DbAny) -> anyhow::Result<Vec<agdb::DbElement>> {
        let stamp = db.exec(QueryBuilder::select().ids(STAMP_ALIAS).query())?;
        let mut nodes = db
            .exec(
                QueryBuilder::select()
                    .search()
                    .from("root")
                    .where_()
                    .node()
                    .query(),
            )?
            .elements;
        nodes.retain(|node| node.id != stamp.elements[0].id);
        Ok(nodes)
    }

    fn with_user_data(version: &str, steps: Option<u64>) -> anyhow::Result<DbAny> {
        let mut db = seeded(version, steps)?;
        db.transaction_mut(|t| -> anyhow::Result<()> {
            let track = t.exec_mut(
                QueryBuilder::insert()
                    .nodes()
                    .values([[("title", "Song").into(), ("id", "track-1").into()]])
                    .query(),
            )?;
            roots::link(t, Root::Tracks, track.elements[0].id)?;
            Ok(())
        })?;
        Ok(db)
    }

    #[test]
    fn opening_or_restamping_leaves_user_data_alone() -> anyhow::Result<()> {
        for (version, plan) in [
            (CURRENT, Plan::Open),
            (
                "0.0.3-dev.0",
                Plan::Stamp {
                    from: Some("0.0.3-dev.0".to_string()),
                },
            ),
        ] {
            let mut db = with_user_data(version, Some(2))?;
            let migration = build(CURRENT, AB);
            assert_eq!(migration.plan(&db)?, plan);
            let before = user_data(&db)?;
            bootstrap::initialize_with(&mut db, &migration)?;
            assert_eq!(user_data(&db)?, before);
            assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        }
        Ok(())
    }

    fn set_value(db: &mut DbAny, key: &str, value: DbValue) -> anyhow::Result<()> {
        db.exec_mut(
            QueryBuilder::insert()
                .values_uniform([(key, value).into()])
                .ids(STAMP_ALIAS)
                .query(),
        )?;
        Ok(())
    }

    #[test]
    fn unreadable_stamp_values_are_refused() -> anyhow::Result<()> {
        let mut db = seeded(CURRENT, Some(0))?;
        set_value(&mut db, VERSION_KEY, 3_u64.into())?;
        assert_eq!(
            refusal(&mut db, &build(CURRENT, AB)),
            "database has an unreadable Lyra version"
        );

        let mut db = seeded("0.0.3-dev.0", Some(0))?;
        set_value(&mut db, STEPS_KEY, "two".into())?;
        assert_eq!(
            refusal(&mut db, &build(CURRENT, AB)),
            "database from Lyra 0.0.3-dev.0 has an unreadable migration step count"
        );
        Ok(())
    }

    #[test]
    fn previous_release_ignores_an_unreadable_step_count() -> anyhow::Result<()> {
        let mut db = seeded(PREVIOUS, Some(0))?;
        set_value(&mut db, STEPS_KEY, "two".into())?;
        let migration = build(CURRENT, AB);
        assert_eq!(
            migration.plan(&db)?,
            Plan::Migrate {
                from: PREVIOUS.to_string(),
                steps: None,
                first_step: 0,
            }
        );
        bootstrap::initialize_with(&mut db, &migration)?;
        assert_eq!(ran(&db)?, ["step_a", "step_b"]);
        assert_eq!(stamp_of(&db)?, stamp(CURRENT, 2));
        Ok(())
    }

    #[test]
    fn only_current_databases_pass_the_current_check() -> anyhow::Result<()> {
        let migration = build(CURRENT, AB);
        migration.ensure_current(&seeded(CURRENT, Some(2))?)?;

        let refusal = |db: DbAny| {
            migration
                .ensure_current(&db)
                .expect_err("database should not be current")
                .to_string()
        };
        assert_eq!(refusal(TestDb::new()?.into_inner()), "database is empty");
        assert_eq!(
            refusal(seeded("0.0.3-dev.0", Some(2))?),
            "database is from Lyra 0.0.3-dev.0, not 0.0.3-dev.2"
        );
        assert_eq!(
            refusal(seeded(CURRENT, Some(1))?),
            "database from Lyra 0.0.3-dev.2 has 1 pending migration steps"
        );
        assert!(refusal(seeded("0.0.4", Some(0))?).starts_with("database is from Lyra 0.0.4; "));
        Ok(())
    }

    #[test]
    fn write_ahead_log_sits_beside_the_database() {
        assert_eq!(
            write_ahead_log(Path::new("/data/lyra.db")),
            PathBuf::from("/data/.lyra.db")
        );
        assert_eq!(
            write_ahead_log(Path::new("lyra.db")),
            PathBuf::from(".lyra.db")
        );
    }
}
