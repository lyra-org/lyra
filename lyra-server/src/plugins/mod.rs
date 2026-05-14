// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};

pub(crate) mod api;
pub(crate) mod artists;
pub(crate) mod auth;
pub(crate) mod bootstrap;
pub(crate) mod chromaprint;
pub(crate) mod covers;
pub(crate) mod datastore;
pub(crate) mod db;
#[cfg(feature = "docgen")]
pub(crate) mod docs;
pub(crate) mod entities;
pub(crate) mod entries;
pub(crate) mod executor;
pub(crate) mod favorites;
pub(crate) mod genres;
pub(crate) mod ids;
pub(crate) mod images;
pub(crate) mod labels;
pub(crate) mod libraries;
pub(crate) mod lifecycle;
pub(crate) mod listens;
pub(crate) mod lyrics;
pub(crate) mod manifests;
pub(crate) mod metadata;
pub(crate) mod mix;
pub(crate) mod playback_sessions;
pub(crate) mod playback_sources;
pub(crate) mod playlists;
pub(crate) mod releases;
pub(crate) mod server;
pub(crate) mod settings;
mod surfaces;
pub(crate) mod tags;
pub(crate) mod track_sources;
pub(crate) mod tracks;
pub(crate) mod users;
#[cfg(feature = "docgen")]
pub(crate) use surfaces::{
    lyra_doc_source_ids,
    render_lyra_doc_source,
};
pub(crate) use surfaces::{
    module_scope_ids,
    module_specs,
};

pub(crate) fn set_owned_db_id_key(
    table: &mut harmony_luau::OwnedTable,
    key: agdb::DbId,
    value: harmony_luau::Value,
) {
    table.set_key(harmony_luau::Value::Integer(key.0), value.clone());
    table.set_key(harmony_luau::Value::Number(key.0 as f64), value.clone());
    if i32::try_from(key.0).is_err() {
        table.set_field(key.0.to_string(), value);
    }
}

pub(crate) fn runtime_error(error: impl std::fmt::Display) -> harmony_luau::Error {
    harmony_luau::Error::Runtime(error.to_string())
}

pub(crate) struct OptionConfig;

impl LuauTypeInfo for OptionConfig {
    fn luau_type() -> LuauType {
        LuauType::literal("OptionConfig")
    }
}

impl DescribeInterface for OptionConfig {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new("OptionConfig", None);
        descriptor.fields.extend([
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "label",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "type",
                ty: String::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "requires_settings",
                ty: Option::<Vec<String>>::luau_type(),
                description: None,
            },
        ]);
        descriptor
    }
}

#[cfg(test)]
mod tests {
    use std::path::{
        Path,
        PathBuf,
    };

    #[test]
    fn plugin_sources_do_not_import_raw_db_directly() {
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
        let allowed = root.join("db.rs");
        let mut files = Vec::new();
        visit(&root, &mut files);

        for path in files {
            if path == allowed {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read plugin source");
            let compact = source
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>();
            let direct_path = ["crate", "db"].join("::");
            let grouped_start = ["usecrate::{", "db"].join("");
            let grouped_module = format!(",{}::{{", "db");
            assert!(
                !source.contains(&direct_path)
                    && !compact.contains(&grouped_start)
                    && !compact.contains(&grouped_module),
                "{} imports raw db directly; use crate::plugins::db",
                path.strip_prefix(&root).unwrap_or(&path).display()
            );
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
