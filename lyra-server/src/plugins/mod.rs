// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub(crate) mod api;
pub(crate) mod args;
pub(crate) mod artists;
pub(crate) mod auth;
pub(crate) mod bootstrap;
pub(crate) mod callback_table;
pub(crate) mod catalog;
pub(crate) mod chromaprint;
pub(crate) mod covers;
pub(crate) mod datastore;
pub(crate) mod db;
pub(crate) mod editions;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod executor;
pub(crate) mod favorites;
pub(crate) mod genres;
pub(crate) mod ids;
pub(crate) mod labels;
pub(crate) mod libraries;
pub(crate) mod lifecycle;
pub(crate) mod limits;
pub(crate) mod listens;
pub(crate) mod locale;
pub(crate) mod lyrics;
pub(crate) mod manifests;
pub(crate) mod metadata;
pub(crate) mod mix;
pub(crate) mod options;
pub(crate) mod playback_sessions;
pub(crate) mod playback_sources;
pub(crate) mod playlists;
pub(crate) mod ratings;
pub(crate) mod releases;
pub(crate) mod repositories;
pub(crate) mod scopes;
pub(crate) mod server;
pub(crate) mod settings;
pub(crate) mod state;
#[cfg(feature = "docgen")]
mod surfaces;
pub(crate) mod tags;
pub(crate) mod testing;
pub(crate) mod track_sources;
pub(crate) mod tracks;
pub(crate) mod types;
pub(crate) mod update_callbacks;
pub(crate) mod users;
pub(crate) use scopes::plugin_host;
pub(crate) use state::STATE;
#[cfg(feature = "docgen")]
pub(crate) use surfaces::{
    lyra_doc_source_ids,
    render_lyra_doc_source,
};

pub(crate) fn runtime_error(error: impl std::fmt::Display) -> harmony_luau::Error {
    harmony_luau::Error::Runtime(error.to_string())
}

/// The plugin whose code makes the call, which must be its current run. Outside plugin code
/// the call fails, named as `function_name`.
pub(crate) fn current_plugin(
    vm: &harmony_luau::Vm,
    context: &harmony_luau::CallContext,
    function_name: &str,
) -> harmony_luau::runtime::Result<crate::services::plugin_scope::PluginId> {
    let Some(plugin_id) = context.origin.plugin.clone() else {
        return Err(runtime_error(format!(
            "{function_name} must be called from plugin Luau code"
        )));
    };
    let plugin_id = crate::services::plugin_scope::PluginId::new(plugin_id)?;
    harmony_core::ensure_current_plugin_run(vm, context)?;
    Ok(plugin_id)
}

#[cfg(test)]
mod tests {
    use std::path::{
        Path,
        PathBuf,
    };

    #[test]
    fn plugin_sources_do_not_import_route_owned_access_policy() {
        fn visit(path: &Path, files: &mut Vec<PathBuf>) {
            if path.is_dir() {
                for entry in std::fs::read_dir(path).expect("read plugin source directory") {
                    visit(&entry.expect("read plugin source entry").path(), files);
                }
                return;
            }

            if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path.to_path_buf());
            }
        }

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plugins");
        let mut files = Vec::new();
        visit(&root, &mut files);

        let route_prefix = format!("{}::", ["crate", "routes"].join("::"));
        for path in files {
            let source = std::fs::read_to_string(&path).expect("read plugin source");
            for helper_name in [
                "entity_accessible",
                "require_entity_accessible",
                "playlist_accessible",
                "resolve_accessible_library",
            ] {
                let route_owned_helper = format!("{route_prefix}{helper_name}");
                assert!(
                    !source.contains(&route_owned_helper),
                    "{} imports route-owned access policy; use crate::services::auth::access",
                    path.strip_prefix(&root).unwrap_or(&path).display()
                );
            }
        }
    }

    #[test]
    fn plugin_modules_do_not_use_context_labels() {
        fn visit(path: &Path, files: &mut Vec<PathBuf>) {
            if path.is_dir() {
                for entry in std::fs::read_dir(path).expect("read plugin source directory") {
                    visit(&entry.expect("read plugin source entry").path(), files);
                }
                return;
            }

            if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path.to_path_buf());
            }
        }

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plugins");
        let mut files = Vec::new();
        visit(&root, &mut files);

        for path in files {
            let source = std::fs::read_to_string(&path).expect("read plugin source");
            assert!(
                !source.contains("context = \""),
                "{} uses module context labels; use explicit #[harmony_context] parameters",
                path.strip_prefix(&root).unwrap_or(&path).display()
            );
        }
    }
}
