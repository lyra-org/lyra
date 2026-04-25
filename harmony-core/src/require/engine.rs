// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::{
    BTreeSet,
    HashMap,
    HashSet,
    VecDeque,
};
use std::io::Result as IoResult;
use std::path::{
    Component,
    Path,
    PathBuf,
};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::RwLock;

use mlua::{
    Error,
    Function,
    Lua,
    NavigateError,
    Require,
    Result,
};

use crate::{
    LuaurcConfig,
    Module,
};

const LUA_FILE_EXTENSIONS: [&str; 2] = ["luau", "lua"];
const LUA_CONFIG_FILENAMES: [&str; 2] = [".luaurc", ".config.luau"];

#[derive(Clone, Debug)]
enum Scope {
    Global,
    Plugin { root: PathBuf },
}

#[derive(Clone, Debug)]
enum Location {
    Root,
    Rust { key: String },
    File { module_path: PathBuf, scope: Scope },
}

#[derive(Clone, Debug)]
enum ResolvedModule {
    File(PathBuf),
    Directory,
}

pub(super) struct ModuleRequirer {
    modules: HashMap<String, Module>,
    prefixes: HashSet<String>,
    alias_config: Vec<u8>,
    cwd: PathBuf,
    current: Location,
    cache: RequireCache,
}

#[derive(Clone)]
pub(crate) struct RequireCache {
    plugin_generations: Arc<RwLock<HashMap<PathBuf, u64>>>,
    cwd: PathBuf,
}

impl Default for RequireCache {
    fn default() -> Self {
        Self::new()
    }
}

impl RequireCache {
    pub(crate) fn new() -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        Self {
            plugin_generations: Arc::default(),
            cwd: ModuleRequirer::normalize_path(&cwd),
        }
    }

    fn normalize_plugin_root(&self, root: &Path) -> PathBuf {
        let root = if root.is_absolute() {
            root.to_path_buf()
        } else {
            self.cwd.join(root)
        };
        ModuleRequirer::normalize_path(&root)
    }

    pub(crate) fn invalidate_plugin_root(&self, root: &Path) {
        let root = self.normalize_plugin_root(root);
        let mut generations = self
            .plugin_generations
            .write()
            .expect("require cache generations poisoned");
        let generation = generations.entry(root).or_default();
        *generation = generation.saturating_add(1);
    }

    fn plugin_generation(&self, root: &Path) -> u64 {
        let root = self.normalize_plugin_root(root);
        self.plugin_generations
            .read()
            .expect("require cache generations poisoned")
            .get(&root)
            .copied()
            .unwrap_or_default()
    }
}

impl ModuleRequirer {
    pub(super) fn new(modules: &Arc<[Module]>, cache: RequireCache) -> Self {
        let mut modules_map = HashMap::new();
        let mut prefixes = HashSet::new();
        let mut aliases = BTreeSet::new();

        for module in modules.iter() {
            let path = module.path.as_ref();
            modules_map.insert(path.to_string(), module.clone());

            let mut prefix = String::new();
            for (idx, part) in path.split('/').enumerate() {
                if idx == 0 {
                    aliases.insert(part.to_ascii_lowercase());
                } else {
                    prefix.push('/');
                }
                prefix.push_str(part);
                prefixes.insert(prefix.clone());
            }
        }

        let alias_config = build_alias_config(&aliases);
        let cwd = cache.cwd.clone();

        Self {
            modules: modules_map,
            prefixes,
            alias_config,
            cwd,
            current: Location::Root,
            cache,
        }
    }

    fn normalize_chunk_name(chunk_name: &str) -> &str {
        if let Some((path, line)) = chunk_name.rsplit_once(':') {
            if line.parse::<u32>().is_ok() {
                return path;
            }
        }
        chunk_name
    }

    fn normalize_path(path: &Path) -> PathBuf {
        let mut components = VecDeque::new();

        for comp in path.components() {
            match comp {
                Component::Prefix(..) | Component::RootDir => components.push_back(comp),
                Component::CurDir => {}
                Component::ParentDir => {
                    if matches!(components.back(), None | Some(Component::ParentDir)) {
                        components.push_back(Component::ParentDir);
                    } else if matches!(components.back(), Some(Component::Normal(..))) {
                        components.pop_back();
                    }
                }
                Component::Normal(..) => components.push_back(comp),
            }
        }

        if matches!(components.front(), None | Some(Component::Normal(..))) {
            components.push_front(Component::CurDir);
        }

        #[cfg(windows)]
        {
            let path: PathBuf = components.into_iter().collect();
            PathBuf::from(path.to_string_lossy().replace('\\', "/"))
        }

        #[cfg(not(windows))]
        {
            components.into_iter().collect()
        }
    }

    fn parse_rust_key(path: &str) -> Option<String> {
        let parts: Vec<&str> = path
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("/"))
        }
    }

    fn strip_module_suffix(path: &Path) -> PathBuf {
        let mut module_path = path.to_path_buf();

        if let Some(ext) = module_path.extension().and_then(|e| e.to_str())
            && LUA_FILE_EXTENSIONS.contains(&ext)
        {
            module_path.set_extension("");
        }

        module_path
    }

    fn module_path_for_chunk(path: &Path) -> PathBuf {
        let module_path = Self::strip_module_suffix(path);

        if module_path.file_name().and_then(|name| name.to_str()) == Some("init")
            && let Some(parent) = module_path.parent()
        {
            return parent.to_path_buf();
        }

        module_path
    }

    fn detect_plugin_root(path: &Path) -> Option<PathBuf> {
        let components: Vec<Component<'_>> = path.components().collect();
        if components.len() < 2 {
            return None;
        }

        let mut found = None;
        for idx in 0..(components.len() - 1) {
            let is_plugins = match components[idx] {
                Component::Normal(name) => name == "plugins",
                _ => false,
            };
            if !is_plugins {
                continue;
            }

            let plugin_component = match components[idx + 1] {
                Component::Normal(name) => Some(name),
                _ => None,
            };
            if plugin_component.is_none() {
                continue;
            }

            let mut root = PathBuf::new();
            for component in &components[..=idx + 1] {
                root.push(component.as_os_str());
            }
            found = Some(Self::normalize_path(&root));
        }

        found
    }

    fn scope_for_module_path(path: &Path) -> Scope {
        if let Some(root) = Self::detect_plugin_root(path) {
            Scope::Plugin { root }
        } else {
            Scope::Global
        }
    }

    fn in_scope(path: &Path, scope: &Scope) -> bool {
        match scope {
            Scope::Global => true,
            Scope::Plugin { root } => Self::normalize_path(path).starts_with(root),
        }
    }

    fn parse_chunk_file_location(&self, chunk_name: &str) -> Option<(PathBuf, Scope)> {
        let chunk_name = Self::normalize_chunk_name(chunk_name);
        if chunk_name.is_empty() || chunk_name == "=repl" {
            return None;
        }

        let chunk_name = if let Some(stripped) = chunk_name.strip_prefix('@') {
            if let Some(key) = Self::parse_rust_key(stripped)
                && self.prefixes.contains(&key)
            {
                return None;
            }
            stripped
        } else {
            chunk_name
        };

        let raw_path = PathBuf::from(chunk_name.replace('\\', "/"));
        let abs_path = if raw_path.is_absolute() {
            raw_path
        } else {
            self.cwd.join(raw_path)
        };
        let abs_path = Self::normalize_path(&abs_path);
        let module_path = Self::module_path_for_chunk(&abs_path);
        let scope = Self::scope_for_module_path(&module_path);

        Some((module_path, scope))
    }

    fn resolve_module(path: &Path) -> StdResult<ResolvedModule, NavigateError> {
        let mut found: Option<PathBuf> = None;

        let last_component = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if last_component != "init" {
            for ext in LUA_FILE_EXTENSIONS {
                let candidate = path.with_extension(ext);
                if candidate.is_file() {
                    if found.replace(candidate).is_some() {
                        return Err(NavigateError::Ambiguous);
                    }
                }
            }
        }

        if path.is_dir() {
            if found.is_some() {
                return Err(NavigateError::Ambiguous);
            }

            for ext in LUA_FILE_EXTENSIONS {
                let candidate = path.join(format!("init.{ext}"));
                if candidate.is_file() {
                    if found.replace(candidate).is_some() {
                        return Err(NavigateError::Ambiguous);
                    }
                }
            }

            if let Some(file_path) = found {
                return Ok(ResolvedModule::File(file_path));
            }

            return Ok(ResolvedModule::Directory);
        }

        if let Some(file_path) = found {
            return Ok(ResolvedModule::File(file_path));
        }

        Err(NavigateError::NotFound)
    }

    fn set_file_location(
        &mut self,
        module_path: PathBuf,
        scope: Scope,
    ) -> StdResult<(), NavigateError> {
        let module_path = Self::normalize_path(&module_path);
        if !Self::in_scope(&module_path, &scope) {
            return Err(NavigateError::NotFound);
        }

        match Self::resolve_module(&module_path) {
            Ok(ResolvedModule::File(_)) | Ok(ResolvedModule::Directory) => {
                self.current = Location::File { module_path, scope };
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn current_file_ref(&self) -> Option<(&PathBuf, &Scope)> {
        match &self.current {
            Location::File { module_path, scope } => Some((module_path, scope)),
            _ => None,
        }
    }

    fn config_path_for(module_path: &Path) -> Option<PathBuf> {
        if !module_path.is_dir() {
            return None;
        }

        for filename in LUA_CONFIG_FILENAMES {
            let path = module_path.join(filename);
            if path.is_file() {
                return Some(path);
            }
        }

        None
    }

    fn merge_alias_config_bytes(primary: &[u8], fallback: &[u8]) -> Vec<u8> {
        let mut primary_config = match LuaurcConfig::from_slice(primary) {
            Some(config) => config,
            None => return primary.to_vec(),
        };
        let fallback_config = match LuaurcConfig::from_slice(fallback) {
            Some(config) => config,
            None => return primary.to_vec(),
        };

        primary_config.merge_missing_aliases(fallback_config.aliases);

        primary_config
            .to_json5_bytes()
            .unwrap_or_else(|_| primary.to_vec())
    }

    fn chunk_name_for_file(file_path: &Path, scope: &Scope) -> String {
        let Scope::Plugin { root } = scope else {
            return file_path.display().to_string();
        };
        let Some(plugin_id) = root.file_name().and_then(|name| name.to_str()) else {
            return file_path.display().to_string();
        };
        let Ok(relative_path) = file_path.strip_prefix(root) else {
            return file_path.display().to_string();
        };
        let relative_path = relative_path.to_string_lossy().replace('\\', "/");
        format!("plugins/{plugin_id}/{relative_path}")
    }
}

impl Require for ModuleRequirer {
    fn is_require_allowed(&self, _chunk_name: &str) -> bool {
        true
    }

    fn reset(&mut self, chunk_name: &str) -> StdResult<(), NavigateError> {
        let normalized = Self::normalize_chunk_name(chunk_name);
        if let Some(path) = normalized.strip_prefix('@')
            && let Some(key) = Self::parse_rust_key(path)
            && self.prefixes.contains(&key)
        {
            self.current = Location::Rust { key };
            return Ok(());
        }

        if let Some((module_path, scope)) = self.parse_chunk_file_location(normalized) {
            if Self::in_scope(&module_path, &scope) {
                self.current = Location::File { module_path, scope };
                return Ok(());
            }
        }

        self.current = Location::Root;
        Ok(())
    }

    fn jump_to_alias(&mut self, path: &str) -> StdResult<(), NavigateError> {
        if path == "self" || path.starts_with("self/") {
            let Location::File { module_path, scope } = &self.current else {
                return Err(NavigateError::NotFound);
            };

            let suffix = path.strip_prefix("self").expect("self prefix should exist");
            let suffix = suffix.trim_start_matches('/');
            let resolved_path = if suffix.is_empty() {
                module_path.clone()
            } else {
                Self::normalize_path(&module_path.join(suffix))
            };

            return self.set_file_location(resolved_path, scope.clone());
        }

        if let Some(key) = Self::parse_rust_key(path)
            && self.prefixes.contains(&key)
        {
            self.current = Location::Rust { key };
            return Ok(());
        }

        let alias_path = PathBuf::from(path.replace('\\', "/"));
        let (base_path, scope) = match &self.current {
            Location::File { module_path, scope } => (module_path.clone(), scope.clone()),
            _ => (self.cwd.clone(), Scope::Global),
        };

        let resolved_path = if alias_path.is_absolute() {
            Self::normalize_path(&alias_path)
        } else {
            Self::normalize_path(&base_path.join(alias_path))
        };

        self.set_file_location(resolved_path, scope)
    }

    fn to_parent(&mut self) -> StdResult<(), NavigateError> {
        match &self.current {
            Location::Rust { key } => {
                let mut parts: Vec<&str> = key.split('/').collect();
                if parts.is_empty() {
                    return Err(NavigateError::NotFound);
                }
                parts.pop();
                if parts.is_empty() {
                    return Err(NavigateError::NotFound);
                }

                let next = parts.join("/");
                if !self.prefixes.contains(&next) {
                    return Err(NavigateError::NotFound);
                }

                self.current = Location::Rust { key: next };
                Ok(())
            }
            Location::File { module_path, scope } => {
                if let Scope::Plugin { root } = scope
                    && module_path == root
                {
                    return Err(NavigateError::NotFound);
                }

                let mut parent = module_path.clone();
                if !parent.pop() {
                    if matches!(scope, Scope::Global) {
                        self.current = Location::Root;
                        return Ok(());
                    }
                    return Err(NavigateError::NotFound);
                }

                self.set_file_location(parent, scope.clone())
            }
            Location::Root => Err(NavigateError::NotFound),
        }
    }

    fn to_child(&mut self, name: &str) -> StdResult<(), NavigateError> {
        if name.is_empty() {
            return Err(NavigateError::NotFound);
        }

        match &self.current {
            Location::Rust { key } => {
                let next = format!("{key}/{name}");
                if !self.prefixes.contains(&next) {
                    return Err(NavigateError::NotFound);
                }

                self.current = Location::Rust { key: next };
                Ok(())
            }
            Location::File { module_path, scope } => {
                let next = Self::normalize_path(&module_path.join(name));
                self.set_file_location(next, scope.clone())
            }
            Location::Root => {
                if self.prefixes.contains(name) {
                    self.current = Location::Rust {
                        key: name.to_string(),
                    };
                    return Ok(());
                }

                let next = Self::normalize_path(&self.cwd.join(name));
                let scope = Self::scope_for_module_path(&next);
                self.set_file_location(next, scope)
            }
        }
    }

    fn has_module(&self) -> bool {
        match &self.current {
            Location::Rust { key } => self.modules.contains_key(key),
            Location::File { module_path, .. } => {
                matches!(
                    Self::resolve_module(module_path),
                    Ok(ResolvedModule::File(_))
                )
            }
            Location::Root => false,
        }
    }

    fn cache_key(&self) -> String {
        match &self.current {
            Location::Rust { key } => format!("rust:@{key}"),
            Location::File { module_path, .. } => {
                if let Ok(ResolvedModule::File(file_path)) = Self::resolve_module(module_path) {
                    let normalized = Self::normalize_path(&file_path);
                    let generation = match &self.current {
                        Location::File {
                            scope: Scope::Plugin { root },
                            ..
                        } => self.cache.plugin_generation(root),
                        _ => 0,
                    };
                    return format!("file:v{generation}:@{}", normalized.display());
                }

                format!("file:@{}", module_path.display())
            }
            Location::Root => "root:@".to_string(),
        }
    }

    fn has_config(&self) -> bool {
        match &self.current {
            Location::File { .. } => true,
            Location::Root => !self.alias_config.is_empty(),
            Location::Rust { .. } => false,
        }
    }

    fn config(&self) -> IoResult<Vec<u8>> {
        if let Some((module_path, _scope)) = self.current_file_ref()
            && let Some(config_path) = Self::config_path_for(module_path)
        {
            let config_data = std::fs::read(config_path)?;
            let merged = Self::merge_alias_config_bytes(&config_data, &self.alias_config);
            return Ok(merged);
        }

        if self.current_file_ref().is_some() || matches!(self.current, Location::Root) {
            return Ok(self.alias_config.clone());
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "config not found",
        ))
    }

    fn loader(&self, lua: &Lua) -> Result<Function> {
        match &self.current {
            Location::Rust { key } => {
                if let Some(module) = self.modules.get(key) {
                    let setup = module.setup.clone();
                    return lua.create_function(move |lua, ()| setup(lua).map_err(Into::into));
                }

                Err(Error::runtime(format!("unknown rust module: {key}")))
            }
            Location::File { module_path, scope } => {
                let file_path = match Self::resolve_module(module_path) {
                    Ok(ResolvedModule::File(path)) => path,
                    Ok(ResolvedModule::Directory) => {
                        return Err(Error::runtime(format!(
                            "path is a directory without init: {}",
                            module_path.display()
                        )));
                    }
                    Err(NavigateError::Ambiguous) => {
                        return Err(Error::runtime(format!(
                            "ambiguous module path: {}",
                            module_path.display()
                        )));
                    }
                    Err(NavigateError::NotFound) => {
                        return Err(Error::runtime(format!(
                            "module not found: {}",
                            module_path.display()
                        )));
                    }
                    Err(NavigateError::Other(err)) => return Err(err),
                };

                let source = std::fs::read(&file_path).map_err(|e| {
                    Error::runtime(format!("failed to read {}: {}", file_path.display(), e))
                })?;
                let chunk_name = Self::chunk_name_for_file(&file_path, scope);

                lua.load(&source).set_name(&chunk_name).into_function()
            }
            Location::Root => Err(Error::runtime("require context is not initialized")),
        }
    }
}

fn build_alias_config(aliases: &BTreeSet<String>) -> Vec<u8> {
    let config = LuaurcConfig::from_aliases(aliases.iter().cloned().map(|alias| {
        let path = alias.clone();
        (alias, path)
    }));
    let mut encoded = config
        .to_pretty_json5_string()
        .expect("serialize generated alias config");
    encoded.push('\n');
    encoded.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::{
        ModuleRequirer,
        build_alias_config,
    };
    use serde_json::Value;
    use std::collections::BTreeSet;

    #[test]
    fn merge_alias_config_bytes_accepts_json5_primary_config() {
        let primary = br#"{
            // keep unrelated settings
            typeErrors: true,
            aliases: {
                project: "./project",
            },
        }"#;
        let fallback =
            build_alias_config(&BTreeSet::from(["harmony".to_string(), "lyra".to_string()]));

        let merged = ModuleRequirer::merge_alias_config_bytes(primary, &fallback);
        let merged_value: Value =
            json5::from_str(std::str::from_utf8(&merged).expect("merged bytes should be utf-8"))
                .expect("parse merged config");

        assert_eq!(merged_value["typeErrors"], Value::Bool(true));
        assert_eq!(
            merged_value["aliases"]["project"],
            Value::String("./project".to_string())
        );
        assert_eq!(
            merged_value["aliases"]["harmony"],
            Value::String("harmony".to_string())
        );
        assert_eq!(
            merged_value["aliases"]["lyra"],
            Value::String("lyra".to_string())
        );
    }

    #[test]
    fn merge_alias_config_bytes_preserves_primary_alias_overrides() {
        let primary = br#"{
            "aliases": {
                "harmony": "./custom/harmony"
            }
        }"#;
        let fallback =
            build_alias_config(&BTreeSet::from(["harmony".to_string(), "lyra".to_string()]));

        let merged = ModuleRequirer::merge_alias_config_bytes(primary, &fallback);
        let merged_value: Value =
            json5::from_str(std::str::from_utf8(&merged).expect("merged bytes should be utf-8"))
                .expect("parse merged config");

        assert_eq!(
            merged_value["aliases"]["harmony"],
            Value::String("./custom/harmony".to_string())
        );
        assert_eq!(
            merged_value["aliases"]["lyra"],
            Value::String("lyra".to_string())
        );
    }

    #[test]
    fn build_alias_config_uses_double_quoted_property_names() {
        let encoded =
            build_alias_config(&BTreeSet::from(["harmony".to_string(), "lyra".to_string()]));
        let text = std::str::from_utf8(&encoded).expect("alias config should be utf-8");

        assert!(text.contains("\"aliases\""));
        assert!(text.contains("\"harmony\": \"harmony\""));
        assert!(text.contains("\"lyra\": \"lyra\""));
        assert!(!text.contains("\naliases:"));
        assert!(!text.contains("\nharmony:"));
        assert!(!text.contains("\nlyra:"));
    }
}
