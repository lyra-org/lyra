// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::path::{
    Component,
    Path,
    PathBuf,
};

use anyhow::{
    Context,
    bail,
};
use lyra_db::{
    entries::classify_file_kind,
    libraries::lexical_normalize_path,
};

use crate::{
    services::STATE,
    services::{
        auth::Principal,
        catalog::Viewer,
        covers::is_cover_file,
    },
};

/// Where `root` leads now. A root can be retargeted while a batch of files is being judged, so no
/// answer outlives one decision.
fn canonical_root(root: &Path) -> Option<PathBuf> {
    #[cfg(test)]
    tests::ROOT_READS.with(|reads| reads.set(reads.get() + 1));
    std::fs::canonicalize(root).ok()
}

struct LibraryRoot {
    path: PathBuf,
    /// The root's path normalized without touching the disk, since a root may itself be a link.
    lexical: PathBuf,
    visible: bool,
    follows_links: bool,
}

impl LibraryRoot {
    /// Whether `lexical`, a path under this root, is `target` by name: no link lies between them
    /// but the root itself.
    fn names(&self, lexical: &Path, target: &Path) -> bool {
        lexical == target
            || lexical.strip_prefix(&self.lexical).is_ok_and(|rest| {
                canonical_root(&self.path).is_some_and(|root| root.join(rest) == target)
            })
    }
}

/// Where a viewer may be served files from: read under the database guard, then checked against
/// the disk once per file. A link can be retargeted after a scan, so the server checks a file each
/// time it serves or registers it.
pub(crate) struct MediaRoots {
    libraries: Vec<LibraryRoot>,
    covers: Option<PathBuf>,
    /// What a followed link may lead to outside every library, by the target's real name.
    outside_rule: fn(&Path) -> bool,
}

impl MediaRoots {
    /// The roots of every library, those `viewer` can't see among them only to refuse.
    pub(crate) fn libraries(db: &impl lyra_db::DbAccess, viewer: &Viewer) -> anyhow::Result<Self> {
        let visible = viewer.visible_libraries(db)?;
        let libraries = lyra_db::libraries::get(db)?
            .into_iter()
            .map(|library| LibraryRoot {
                lexical: lexical_normalize_path(&library.path),
                visible: visible
                    .as_ref()
                    .is_none_or(|ids| library.db_id.is_some_and(|id| ids.contains(&id))),
                follows_links: library.follows_links(),
                path: library.path,
            })
            .collect();
        Ok(Self {
            libraries,
            covers: None,
            outside_rule: is_media_file,
        })
    }

    /// [`Self::libraries`] as `principal` sees them, or as the system does without one.
    pub(crate) fn for_principal(
        db: &impl lyra_db::DbAccess,
        principal: Option<&Principal>,
    ) -> anyhow::Result<Self> {
        Self::libraries(db, &Viewer::of(db, principal.cloned())?)
    }

    /// Also allows the files the server wrote under `covers`, whose links lead nowhere else.
    pub(crate) fn and_covers(mut self, covers: PathBuf) -> Self {
        self.covers = Some(covers);
        self
    }

    /// Narrows what a followed link may lead to outside every library to cover images, for files
    /// served as covers.
    pub(crate) fn for_covers(mut self) -> Self {
        self.outside_rule = is_cover_file;
        self
    }

    /// The file to open for `path`, if these roots allow it, for a caller that has others to try
    /// or takes a refusal as nothing being there. Blocks on the disk.
    pub(crate) fn admits_file(&self, path: &Path) -> Option<PathBuf> {
        let target = regular_target(path).ok()?;
        self.admits(path, &target).then_some(target)
    }

    /// The file to open for `path`, or why it may not be served, logging a refusal. Blocks on the
    /// disk.
    pub(crate) fn resolve(&self, path: &Path) -> anyhow::Result<PathBuf> {
        let target = regular_target(path)?;
        if self.admits(path, &target) {
            return Ok(target);
        }
        tracing::warn!(
            path = %path.display(),
            target = %target.display(),
            "refused a file outside the roots it may come from"
        );
        bail!(
            "path '{}' is outside library and cover roots",
            path.display()
        );
    }

    /// [`Self::resolve`] off the async threads.
    pub(crate) async fn resolve_off_thread(self, path: PathBuf) -> anyhow::Result<PathBuf> {
        tokio::task::spawn_blocking(move || self.resolve(&path))
            .await
            .context("path resolution task panicked")?
    }

    /// The deepest visible library holding `lexical` by name. It alone speaks for the path, so a
    /// library nested in another keeps its own setting.
    fn owner(&self, lexical: &Path) -> Option<&LibraryRoot> {
        self.libraries
            .iter()
            .filter(|library| library.visible && lexical.starts_with(&library.lexical))
            .max_by_key(|library| library.lexical.components().count())
    }

    /// Whether the deepest library root holding `target` is visible, if any holds it. A visible
    /// library doesn't open what a hidden one nested in it holds to links; the two compare by
    /// depth, a tie going to the visible one.
    fn target_visible(&self, target: &Path) -> Option<bool> {
        self.libraries
            .iter()
            .filter_map(|library| {
                let root = canonical_root(&library.path)?;
                target
                    .starts_with(&root)
                    .then(|| (root.components().count(), library.visible))
            })
            .max()
            .map(|(_, visible)| visible)
    }

    fn admits(&self, path: &Path, target: &Path) -> bool {
        let lexical = lexical_normalize_path(path);
        let owner = self.owner(&lexical);
        // `..` can climb out of a link and land back on the file's own name.
        let climbs = path
            .components()
            .any(|component| component == Component::ParentDir);
        // An ordinary file of a visible library, which needs no other root to be read.
        if !climbs && owner.is_some_and(|owner| owner.names(&lexical, target)) {
            return true;
        }
        let covers = self.covers.as_deref().and_then(canonical_root);
        if covers.is_some_and(|covers| target.starts_with(covers)) {
            return true;
        }
        match self.target_visible(target) {
            Some(visible) => visible,
            None => {
                owner.is_some_and(|owner| owner.follows_links)
                    && (self.outside_rule)(target)
                    // `..` after a link leaves the link's target, which normalizing can't tell.
                    && std::fs::canonicalize(&lexical).is_ok_and(|named| named == target)
            }
        }
    }
}

#[cfg(test)]
impl MediaRoots {
    /// Visible libraries at `roots`, for tests of what reads through them without a database.
    pub(crate) fn around(roots: impl IntoIterator<Item = PathBuf>) -> Self {
        Self {
            libraries: roots
                .into_iter()
                .map(|path| LibraryRoot {
                    lexical: lexical_normalize_path(&path),
                    visible: true,
                    follows_links: false,
                    path,
                })
                .collect(),
            covers: None,
            outside_rule: is_media_file,
        }
    }

    /// Has the library at `root` follow links.
    pub(crate) fn following(mut self, root: &Path) -> Self {
        for library in &mut self.libraries {
            library.follows_links |= library.path == root;
        }
        self
    }
}

/// The regular file `path` leads to.
fn regular_target(path: &Path) -> anyhow::Result<PathBuf> {
    let target = std::fs::canonicalize(path)
        .with_context(|| format!("failed to resolve path '{}'", path.display()))?;
    let metadata = std::fs::metadata(&target)
        .with_context(|| format!("failed to read path '{}'", path.display()))?;
    if !metadata.is_file() {
        bail!("path '{}' is not a regular file", path.display());
    }
    Ok(target)
}

/// Whether the file's name is one a followed link may lead to: audio, a cue sheet or a cover.
pub(crate) fn is_media_file(path: &Path) -> bool {
    classify_file_kind(path).is_some() || is_cover_file(path)
}

/// Resolves `path` to the file it leads to, refusing anything [`MediaRoots`] of the libraries
/// `principal` can see and the configured cover storage root wouldn't serve. Plugins may name any
/// path, so every file they have the server read goes through here.
pub(crate) async fn confine_to_media_roots(
    path: &str,
    principal: Option<&Principal>,
) -> anyhow::Result<PathBuf> {
    let roots = {
        let db = STATE.db.read().await;
        MediaRoots::for_principal(&*db, principal)?
    }
    .and_covers(crate::services::covers::configured_covers_root());
    roots.resolve_off_thread(PathBuf::from(path)).await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        os::unix::fs::symlink,
        process::Command,
    };

    use agdb::DbAny;
    use tempfile::TempDir;

    use super::*;

    thread_local! {
        /// How many times this thread resolved a root.
        pub(super) static ROOT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// A library that follows links or doesn't, a second one, and a folder outside both.
    struct Layout {
        db: DbAny,
        _dir: TempDir,
        library: PathBuf,
        library_id: String,
        listener: (agdb::DbId, String),
        other: PathBuf,
        outside: PathBuf,
    }

    fn insert_library(db: &mut DbAny, name: &str, path: &Path, follow: bool) -> String {
        let mut library = lyra_db::test_db::insert_test_library_node(db, name, path.to_path_buf())
            .expect("library inserted");
        library.follow_links = follow.then_some(true);
        let id = library.db_id.expect("inserted library has an id");
        db.transaction_mut(|t| {
            t.exec_mut(
                agdb::QueryBuilder::insert()
                    .edges()
                    .from("libraries")
                    .to(id)
                    .query(),
            )?;
            lyra_db::libraries::update(t, &library).map_err(anyhow::Error::from)
        })
        .expect("library connected");
        library.id
    }

    fn layout(follow: bool) -> anyhow::Result<Layout> {
        let dir = tempfile::tempdir()?;
        let root = fs::canonicalize(dir.path())?;
        let (library, other, outside) = (
            root.join("library"),
            root.join("other"),
            root.join("outside"),
        );
        for path in [&library, &other, &outside] {
            fs::create_dir_all(path)?;
        }
        fs::write(outside.join("config.json"), "secret")?;
        fs::write(outside.join("song.flac"), "audio")?;
        fs::write(outside.join("cover.JPG"), "image")?;
        let mut db = lyra_db::test_db::new_test_db()?;
        let library_id = insert_library(&mut db, "Library", &library, follow);
        insert_library(&mut db, "Other", &other, false);
        let user = lyra_db::fixtures::test_user("listener")?;
        let listener = (lyra_db::users::create(&mut db, &user)?, user.id);
        Ok(Layout {
            db,
            listener,
            _dir: dir,
            library,
            library_id,
            other,
            outside,
        })
    }

    impl Layout {
        fn system(&self) -> MediaRoots {
            MediaRoots::for_principal(&self.db, None).expect("roots read")
        }

        /// As a user who sees only the first library.
        fn listener(&self) -> MediaRoots {
            let principal = Principal::from_parts(
                self.listener.0,
                self.listener.1.clone(),
                "listener".to_string(),
                Vec::new(),
                None,
                HashSet::from([self.library_id.clone()]),
            );
            MediaRoots::for_principal(&self.db, Some(&principal)).expect("roots read")
        }
    }

    fn refused(roots: &MediaRoots, path: &Path) -> bool {
        roots
            .resolve(path)
            .is_err_and(|err| err.to_string().contains("outside library"))
    }

    #[test]
    fn a_file_in_a_library_resolves_with_links_inside_it() -> anyhow::Result<()> {
        let layout = layout(false)?;
        let song = layout.library.join("song.flac");
        fs::write(&song, "audio")?;
        let link = layout.library.join("link.flac");
        symlink(&song, &link)?;

        assert_eq!(layout.system().resolve(&song)?, song);
        assert_eq!(layout.system().resolve(&link)?, song);
        Ok(())
    }

    #[test]
    fn a_link_out_of_a_library_needs_the_setting_and_a_media_target() -> anyhow::Result<()> {
        for name in ["song.flac", "cover.JPG"] {
            let off = layout(false)?;
            let link = off.library.join("link.flac");
            symlink(off.outside.join(name), &link)?;
            assert!(refused(&off.system(), &link));

            let on = layout(true)?;
            let link = on.library.join("link.flac");
            symlink(on.outside.join(name), &link)?;
            assert_eq!(on.system().resolve(&link)?, on.outside.join(name));
        }

        let on = layout(true)?;
        let link = on.library.join("x.flac");
        symlink(on.outside.join("config.json"), &link)?;
        assert!(refused(&on.system(), &link));
        Ok(())
    }

    #[test]
    fn a_retargeted_link_is_judged_by_its_new_target() -> anyhow::Result<()> {
        for follow in [false, true] {
            let layout = layout(follow)?;
            let song = layout.library.join("song.flac");
            fs::write(&song, "audio")?;
            assert!(layout.system().resolve(&song).is_ok());

            fs::remove_file(&song)?;
            symlink(layout.outside.join("config.json"), &song)?;
            assert!(refused(&layout.system(), &song), "follow_links {follow}");
        }
        Ok(())
    }

    #[test]
    fn a_followed_folder_serves_only_media_by_its_real_name() -> anyhow::Result<()> {
        let layout = layout(true)?;
        let folder = layout.library.join("linked");
        symlink(&layout.outside, &folder)?;

        assert_eq!(
            layout.system().resolve(&folder.join("song.flac"))?,
            layout.outside.join("song.flac")
        );
        assert!(refused(&layout.system(), &folder.join("config.json")));
        Ok(())
    }

    #[test]
    fn a_link_into_a_library_the_viewer_cant_see_is_refused() -> anyhow::Result<()> {
        for follow in [false, true] {
            let layout = layout(follow)?;
            let target = layout.other.join("song.flac");
            fs::write(&target, "audio")?;
            let link = layout.library.join("link.flac");
            symlink(&target, &link)?;

            assert!(refused(&layout.listener(), &link), "follow_links {follow}");
            assert!(refused(&layout.listener(), &target));
            assert_eq!(layout.system().resolve(&link)?, target);
        }
        Ok(())
    }

    #[test]
    fn a_library_the_viewer_cant_see_lends_no_following() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        let hidden = layout.other.join("nested");
        fs::create_dir_all(&hidden)?;
        insert_library(&mut layout.db, "Hidden", &hidden, true);
        let link = hidden.join("link.flac");
        symlink(layout.outside.join("song.flac"), &link)?;

        assert!(refused(&layout.listener(), &link));
        Ok(())
    }

    #[test]
    fn a_link_into_a_hidden_library_nested_in_a_visible_one_is_refused() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        let hidden = layout.library.join("private");
        fs::create_dir_all(&hidden)?;
        insert_library(&mut layout.db, "Private", &hidden, false);
        let secret = hidden.join("song.flac");
        fs::write(&secret, "audio")?;
        let link = layout.library.join("link.flac");
        symlink(&secret, &link)?;
        let folder = layout.library.join("folder");
        symlink(&hidden, &folder)?;

        let roots = layout.listener();
        assert!(refused(&roots, &link));
        assert!(refused(&roots, &folder.join("song.flac")));
        assert_eq!(roots.resolve(&secret)?, secret, "the parent holds the file");
        Ok(())
    }

    #[test]
    fn a_visible_library_nested_in_a_hidden_one_keeps_its_own_links() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        let nested = layout.other.join("shared");
        fs::create_dir_all(&nested)?;
        let shared = insert_library(&mut layout.db, "Shared", &nested, false);
        layout.library_id = shared;
        let song = nested.join("song.flac");
        fs::write(&song, "audio")?;
        let inside = nested.join("inside.flac");
        symlink(&song, &inside)?;
        let secret = layout.other.join("secret.flac");
        fs::write(&secret, "audio")?;
        let escape = nested.join("escape.flac");
        symlink(&secret, &escape)?;

        let roots = layout.listener();
        assert_eq!(roots.resolve(&song)?, song);
        assert_eq!(roots.resolve(&inside)?, song);
        assert!(refused(&roots, &escape));
        assert!(refused(&roots, &secret));
        Ok(())
    }

    #[test]
    fn a_nested_library_follows_links_by_its_own_setting() -> anyhow::Result<()> {
        for (parent, nested) in [(true, false), (false, true)] {
            let mut layout = layout(parent)?;
            let inner = layout.library.join("nested");
            fs::create_dir_all(&inner)?;
            insert_library(&mut layout.db, "Nested", &inner, nested);
            let target = layout.outside.join("song.flac");
            let in_parent = layout.library.join("link.flac");
            let in_nested = inner.join("link.flac");
            symlink(&target, &in_parent)?;
            symlink(&target, &in_nested)?;

            let roots = layout.system();
            assert_eq!(roots.resolve(&in_parent).is_ok(), parent);
            assert_eq!(roots.resolve(&in_nested).is_ok(), nested);
        }
        Ok(())
    }

    #[test]
    fn an_ordinary_file_reads_no_root_but_an_aliased_one_of_its_own() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        insert_library(
            &mut layout.db,
            "Gone",
            &layout.outside.join("unmounted"),
            false,
        );
        let song = layout.library.join("song.flac");
        fs::write(&song, "audio")?;
        let alias = layout.outside.join("alias");
        symlink(&layout.library, &alias)?;
        insert_library(&mut layout.db, "Alias", &alias, false);

        let roots = layout.system();
        let root_reads = |path: &Path| {
            ROOT_READS.set(0);
            roots.resolve(path).map(|_| ROOT_READS.get())
        };
        assert_eq!(root_reads(&song)?, 0);
        assert_eq!(root_reads(&alias.join("song.flac"))?, 1);
        Ok(())
    }

    #[test]
    fn a_cover_outside_every_library_must_be_named_as_one() -> anyhow::Result<()> {
        let layout = layout(true)?;
        let (audio, image) = (
            layout.library.join("cover.jpg"),
            layout.library.join("cover.png"),
        );
        symlink(layout.outside.join("song.flac"), &audio)?;
        symlink(layout.outside.join("cover.JPG"), &image)?;

        let roots = layout.system().for_covers();
        assert!(refused(&roots, &audio));
        assert_eq!(roots.resolve(&image)?, layout.outside.join("cover.JPG"));
        assert!(
            layout.system().resolve(&audio).is_ok(),
            "plugin files keep the media rule"
        );
        Ok(())
    }

    #[test]
    fn a_retargeted_hidden_root_is_seen_by_the_next_decision() -> anyhow::Result<()> {
        let mut layout = layout(true)?;
        let base = layout.library.parent().expect("library has a parent");
        let (old, secret, alias) = (base.join("old"), base.join("secret"), base.join("hidden"));
        fs::create_dir_all(&old)?;
        fs::create_dir_all(&secret)?;
        symlink(&old, &alias)?;
        insert_library(&mut layout.db, "Hidden", &alias, false);
        fs::write(secret.join("song.flac"), "audio")?;
        let warm = layout.library.join("warm.flac");
        symlink(layout.outside.join("song.flac"), &warm)?;
        let link = layout.library.join("link.flac");
        symlink(secret.join("song.flac"), &link)?;

        let roots = layout.listener();
        roots.resolve(&warm)?;
        assert!(roots.resolve(&link).is_ok(), "outside every library so far");

        fs::remove_file(&alias)?;
        symlink(&secret, &alias)?;
        assert!(refused(&roots, &link));
        Ok(())
    }

    #[test]
    fn a_retargeted_visible_root_is_seen_by_the_next_decision() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        let base = layout.library.parent().expect("library has a parent");
        let (first, second, alias) = (base.join("first"), base.join("second"), base.join("alias"));
        let hidden = first.join("private");
        fs::create_dir_all(&hidden)?;
        fs::create_dir_all(&second)?;
        symlink(&first, &alias)?;
        fs::write(first.join("song.flac"), "audio")?;
        fs::write(second.join("song.flac"), "audio")?;
        fs::write(hidden.join("secret.flac"), "audio")?;
        symlink(&hidden, second.join("private"))?;
        let shared = insert_library(&mut layout.db, "Alias", &alias, false);
        insert_library(&mut layout.db, "Private", &hidden, false);
        layout.library_id = shared;

        let roots = layout.listener();
        roots.resolve(&alias.join("song.flac"))?;

        fs::remove_file(&alias)?;
        symlink(&second, &alias)?;
        assert!(refused(&roots, &alias.join("private/secret.flac")));
        Ok(())
    }

    #[test]
    fn dot_dot_after_a_link_doesnt_name_a_file_in_a_hidden_nested_root() -> anyhow::Result<()> {
        let mut layout = layout(false)?;
        let (hidden, other) = (layout.library.join("private"), layout.library.join("other"));
        fs::create_dir_all(&hidden)?;
        fs::create_dir_all(&other)?;
        insert_library(&mut layout.db, "Private", &hidden, false);
        let secret = hidden.join("song.flac");
        fs::write(&secret, "audio")?;
        let link = layout.library.join("link");
        symlink(&other, &link)?;

        let roots = layout.listener();
        assert!(refused(&roots, &link.join("../private/song.flac")));
        assert_eq!(roots.resolve(&secret)?, secret, "the parent holds the file");
        Ok(())
    }

    #[test]
    fn a_fifo_is_never_a_file_to_serve() -> anyhow::Result<()> {
        let layout = layout(true)?;
        let inside = layout.library.join("fifo.flac");
        let outside = layout.outside.join("fifo.flac");
        for fifo in [&inside, &outside] {
            assert!(Command::new("mkfifo").arg(fifo).status()?.success());
        }
        let link = layout.library.join("link.flac");
        symlink(&outside, &link)?;

        for path in [&inside, &link] {
            let err = layout.system().resolve(path).expect_err("refused");
            assert!(err.to_string().contains("not a regular file"), "{err}");
        }
        Ok(())
    }

    #[test]
    fn dot_dot_cant_leave_the_library() -> anyhow::Result<()> {
        let layout = layout(true)?;
        assert!(refused(
            &layout.system(),
            &layout.library.join("../outside/song.flac")
        ));

        // Beside the link's target, which the normalized path takes to be in the library.
        let target = layout.outside.join("albums");
        fs::create_dir_all(&target)?;
        let link = layout.library.join("linked");
        symlink(&target, &link)?;
        assert!(refused(&layout.system(), &link.join("../song.flac"),));
        fs::write(layout.library.join("song.flac"), "audio")?;
        assert!(refused(&layout.system(), &link.join("../song.flac"),));
        Ok(())
    }

    #[test]
    fn a_root_that_is_itself_a_link_compares_as_named() -> anyhow::Result<()> {
        let layout = layout(false)?;
        let parent = layout.library.parent().expect("library has a parent");
        let real = parent.join("real");
        fs::create_dir_all(&real)?;
        let alias = parent.join("alias");
        symlink(&real, &alias)?;
        fs::write(real.join("song.flac"), "audio")?;
        symlink(layout.outside.join("song.flac"), real.join("link.flac"))?;
        symlink(layout.outside.join("config.json"), real.join("x.flac"))?;

        let mut db = lyra_db::test_db::new_test_db()?;
        insert_library(&mut db, "Alias", &alias, true);
        let roots = MediaRoots::for_principal(&db, None)?;

        assert_eq!(
            roots.resolve(&alias.join("song.flac"))?,
            real.join("song.flac")
        );
        assert_eq!(
            roots.resolve(&alias.join("link.flac"))?,
            layout.outside.join("song.flac")
        );
        assert!(refused(&roots, &alias.join("x.flac")));
        assert!(refused(&roots, &real.join("link.flac")));
        Ok(())
    }

    #[test]
    fn the_covers_root_holds_what_is_in_it_and_nothing_a_link_reaches() -> anyhow::Result<()> {
        let layout = layout(true)?;
        let covers = layout.outside.join("covers");
        fs::create_dir_all(&covers)?;
        let stored = covers.join("image.jpg");
        fs::write(&stored, "image")?;
        let link = covers.join("link.flac");
        symlink(layout.outside.join("song.flac"), &link)?;

        let roots = layout.system().and_covers(covers);
        assert_eq!(roots.resolve(&stored)?, stored);
        assert!(refused(&roots, &link));
        assert!(refused(&layout.system(), &stored));
        Ok(())
    }
}
