// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

#[cfg(feature = "docgen")]
mod types;

use harmony_core::ModuleSpec;
use harmony_luau as luau;
use harmony_luau::FromCallFrame;
#[cfg(feature = "docgen")]
use harmony_luau::render_definition_file_with_support;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeInterface,
    DescribeModule,
    DescribeTypeAlias,
    FieldDescriptor,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFieldDescriptor,
    TypeAliasDescriptor,
};
#[cfg(feature = "docgen")]
use std::collections::HashMap;

use crate::plugins::{
    args,
    ids,
};
use crate::{
    plugins::db::ViewerDb,
    services::{
        entities::{
            EntityInclude,
            EntityProjectionInfo,
            IdIncludes,
            apply_id_links,
            id_link_targets,
            project_visible_entities,
        },
        providers::resolve_id_links,
    },
};
use agdb::QueryId;
use lyra_db::DbAsync;

/// The `options` argument of `get`.
#[derive(Default)]
struct GetOptions {
    includes: Vec<EntityInclude>,
    library_id: Option<String>,
}

impl<'vm> luau::FromLuau<'vm> for GetOptions {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let options: luau::Table = reader.read()?;
        args::reject_unknown_fields(&options, "options", &["include", "library_id"])?;
        Ok(Self {
            includes: parse_includes(options.get("include")?)?,
            library_id: options.optional_string_field("library_id")?,
        })
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for GetOptions {
    fn luau_type() -> LuauType {
        LuauType::named("EntityGetOptions")
    }
}

/// The plugin whose VM is projecting: what it sees of the providers, and whose calls it makes.
fn link_caller(
    context: &luau::CallContext,
) -> luau::runtime::Result<Option<crate::services::plugin_scope::PluginId>> {
    Ok(context
        .origin
        .plugin
        .clone()
        .map(crate::services::plugin_scope::PluginId::new)
        .transpose()?)
}

/// Whether the projection runs link generators: only for the `links` include. Generators can't
/// request links themselves, even from tasks they spawn, since those inherit the caller context.
fn runs_link_generators(
    context: &luau::CallContext,
    includes: &[EntityInclude],
) -> luau::runtime::Result<bool> {
    if !includes.contains(&EntityInclude::Links) {
        return Ok(false);
    }
    if context
        .caller
        .get::<crate::plugins::executor::IdLinkDispatchContext>()
        .is_ok()
    {
        return Err(crate::plugins::runtime_error(
            "the links include cannot be requested from an id link generator",
        ));
    }
    Ok(true)
}

#[cfg(feature = "docgen")]
struct EntityProjection;

#[cfg(feature = "docgen")]
impl LuauTypeInfo for EntityProjection {
    fn luau_type() -> LuauType {
        LuauType::named("EntityProjectionInfo")
    }
}

pub(crate) struct EntitiesModule;

#[harmony_macros::module(
    id = "lyra/entities",
    docs_feature = "docgen",
    extend = extend_module,
    describe = describe_module
)]
impl EntitiesModule {
    #[harmony(
        description = "The kind of node `id` names, such as `\"Track\"`, or nil when it names nothing the caller can see."
    )]
    async fn get_type(
        id: String,
        #[context] db: ViewerDb,
    ) -> luau::runtime::Result<Option<String>> {
        let (db, viewer) = db.read().await?;
        match ids::visible_db_id(&db, &viewer, &id)? {
            Some(id) => ids::kind(&db, id),
            None => Ok(None),
        }
    }

    #[harmony(
        description = "Projections of the releases, tracks and artists with these ids that the caller can see, keyed by id. Ids naming anything else, or nothing, are left out.",
        returns(HashMap<String, EntityProjection>)
    )]
    async fn get(
        ids: args::UniqueIds,
        options: Option<GetOptions>,
        #[context] vm: luau::Vm,
        #[context] context: luau::CallContext,
    ) -> luau::runtime::Result<luau::OwnedTable> {
        let GetOptions {
            includes,
            library_id,
        } = options.unwrap_or_default();
        let run_generators = runs_link_generators(&context, &includes)?;
        let scoped = ViewerDb::from_call_frame(&vm, &context)?;
        let mut keys = Vec::new();
        let projections = project_with_links(
            scoped.handle(),
            &vm,
            &context,
            run_generators,
            &includes,
            library_id,
            |db, library_id, id_includes| {
                let mut resolved_keys = Vec::new();
                let mut query_ids = Vec::new();
                for key in ids.0 {
                    let Some(db_id) = ids::db_id(db, &key)? else {
                        continue;
                    };
                    resolved_keys.push(key);
                    query_ids.push(QueryId::Id(db_id));
                }
                let viewer = scoped.viewer(db)?;
                let projections = project_visible_entities(
                    db,
                    &viewer,
                    query_ids,
                    &includes,
                    library_id,
                    id_includes,
                )?;
                let mut visible = Vec::with_capacity(projections.len());
                for (key, projection) in resolved_keys.into_iter().zip(projections) {
                    if let Some(projection) = projection {
                        keys.push(key);
                        visible.push(projection);
                    }
                }
                Ok(visible)
            },
        )
        .await?;

        let mut table = luau::OwnedTable::with_capacity(0, keys.len());
        for (key, projection) in keys.into_iter().zip(projections) {
            table.set_field(key, projection_to_luau_owned(projection)?);
        }
        Ok(table)
    }
}

fn extend_module(spec: ModuleSpec) -> ModuleSpec {
    spec.initializer(install_entity_constants)
}

/// Projects under one DB read, then resolves the `links` include after the
/// lock is released, since generator functions may use the database.
async fn project_with_links(
    db: DbAsync,
    vm: &luau::Vm,
    context: &luau::CallContext,
    run_generators: bool,
    includes: &[EntityInclude],
    library_id: Option<String>,
    project: impl FnOnce(
        &agdb::DbAny,
        Option<agdb::DbId>,
        &mut IdIncludes<'_>,
    ) -> luau::runtime::Result<Vec<EntityProjectionInfo>>,
) -> luau::runtime::Result<Vec<EntityProjectionInfo>> {
    // The calling VM's scope, not the published one: during a reload that's another VM's, and
    // generators can only run on this one.
    let vm_scope = crate::plugins::state::VmScope::of(vm)?;
    let caller = link_caller(context)?;
    let chain =
        crate::plugins::executor::CallChain::of(context, crate::plugins::executor::now(vm)?);
    let (schemes, generators) = {
        let registry = vm_scope.provider_view(caller.as_ref()).await;
        let registry = registry.read().await;
        (
            registry.id_schemes(),
            run_generators.then(|| registry.id_link_generators()),
        )
    };
    let (mut projections, links) = {
        let db = db.read().await;
        let library_id = match library_id {
            Some(library_id) => ids::db_id(&*db, &library_id)?,
            None => None,
        };
        let mut id_includes = IdIncludes::new(&schemes, includes);
        let projections = project(&db, library_id, &mut id_includes)?;
        let links = match (generators, id_includes.into_link_rows()) {
            (Some(generators), Some(rows)) => {
                let targets = id_link_targets(&db, rows, library_id, &generators)?;
                Some((generators, targets))
            }
            _ => None,
        };
        (projections, links)
    };
    if let Some((generators, targets)) = links {
        let cache = vm_scope.services.providers.id_link_cache();
        let links = resolve_id_links(&cache, targets, &generators, |provider_id, calls| {
            crate::plugins::executor::dispatch_id_links_in_vm(
                vm.clone(),
                chain.clone(),
                provider_id,
                calls,
            )
        })
        .await;
        apply_id_links(&mut projections, &links);
    }
    Ok(projections)
}

fn parse_includes(value: luau::Value) -> luau::runtime::Result<Vec<EntityInclude>> {
    let mut include_values = Vec::new();
    match value {
        luau::Value::Nil => {}
        luau::Value::String(bytes) => {
            include_values.push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?)
        }
        luau::Value::Table(table) => {
            for (_, value) in table.pairs()? {
                let luau::Value::String(bytes) = value else {
                    return Err(crate::plugins::runtime_error(
                        "include entries must be strings when include is an array",
                    ));
                };
                include_values
                    .push(String::from_utf8(bytes).map_err(crate::plugins::runtime_error)?);
            }
        }
        other => {
            return Err(crate::plugins::runtime_error(format!(
                "include must be a string or an array of strings, got {}",
                other.type_name()
            )));
        }
    }

    let mut includes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for include_key in include_values {
        let include = EntityInclude::parse(&include_key).ok_or_else(|| {
            let valid = EntityInclude::ALL
                .iter()
                .map(|include| include.as_key())
                .collect::<Vec<_>>()
                .join(", ");
            crate::plugins::runtime_error(format!(
                "unknown include '{}'; expected one of: {valid}",
                include_key
            ))
        })?;
        if seen.insert(include) {
            includes.push(include);
        }
    }
    Ok(includes)
}

fn projection_to_luau_owned(
    projection: EntityProjectionInfo,
) -> luau::runtime::Result<luau::Value> {
    match projection {
        EntityProjectionInfo::Release(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        EntityProjectionInfo::Track(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
        EntityProjectionInfo::Artist(projection) => {
            harmony_luau::serializable_to_luau_owned(projection)
        }
    }
}

fn install_entity_constants(
    vm: &luau::Vm,
    _origin: &harmony_core::ChunkOrigin,
    root: &luau::Table,
) -> luau::runtime::Result<()> {
    install_string_table(
        vm,
        root,
        "CreditType",
        &[
            ("Artist", "artist"),
            ("Vocalist", "vocalist"),
            ("Instrumentalist", "instrumentalist"),
            ("Composer", "composer"),
            ("Lyricist", "lyricist"),
            ("Arranger", "arranger"),
            ("Writer", "writer"),
            ("Producer", "producer"),
            ("Conductor", "conductor"),
            ("Engineer", "engineer"),
            ("Mixer", "mixer"),
            ("Remixer", "remixer"),
        ],
    )?;
    install_string_table(
        vm,
        root,
        "ArtistCreditSource",
        &[("Track", "track"), ("Release", "release")],
    )
}

fn install_string_table(
    vm: &luau::Vm,
    root: &luau::Table,
    key: &str,
    entries: &[(&str, &str)],
) -> luau::runtime::Result<()> {
    let table = vm.create_table()?;
    for (name, value) in entries {
        table.set(name, luau::Value::String(value.as_bytes().to_vec()))?;
    }
    table.set_readonly(true)?;
    root.set(key, &table)
}

#[cfg(feature = "docgen")]
fn field(name: &'static str, ty: LuauType) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: None,
    }
}

#[cfg(feature = "docgen")]
fn include_selector_type() -> LuauType {
    LuauType::union(vec![String::luau_type(), Vec::<String>::luau_type()])
}

#[cfg(feature = "docgen")]
fn string_enum(values: impl IntoIterator<Item = &'static str>) -> LuauType {
    LuauType::union(values.into_iter().map(LuauType::string_literal).collect())
}

#[cfg(feature = "docgen")]
fn track_type() -> LuauType {
    LuauType::object(vec![
        field("id", String::luau_type()),
        field("track_title", String::luau_type()),
        field("sort_title", Option::<String>::luau_type()),
        field("year", Option::<u32>::luau_type()),
        field("disc", Option::<u32>::luau_type()),
        field("disc_total", Option::<u32>::luau_type()),
        field("track", Option::<u32>::luau_type()),
        field("track_total", Option::<u32>::luau_type()),
        field("duration_ms", Option::<u64>::luau_type()),
        field("track_gain_db", Option::<f64>::luau_type()),
        field("album_gain_db", Option::<f64>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at_ms", Option::<u64>::luau_type()),
        field("file_created_at_ms", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn release_type() -> LuauType {
    LuauType::object(vec![
        field("id", String::luau_type()),
        field("release_title", String::luau_type()),
        field("sort_title", Option::<String>::luau_type()),
        field(
            "release_type",
            LuauType::optional(LuauType::named("ReleaseType")),
        ),
        field("release_date", Option::<String>::luau_type()),
        field("description", Option::<String>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at_ms", Option::<u64>::luau_type()),
        field("file_created_at_ms", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn artist_type() -> LuauType {
    LuauType::object(vec![
        field("id", String::luau_type()),
        field("artist_name", String::luau_type()),
        field("scan_name", String::luau_type()),
        field("sort_name", Option::<String>::luau_type()),
        field(
            "artist_type",
            LuauType::optional(LuauType::named("ArtistType")),
        ),
        field("description", Option::<String>::luau_type()),
        field("locked", Option::<bool>::luau_type()),
        field("created_at_ms", Option::<u64>::luau_type()),
    ])
}

#[cfg(feature = "docgen")]
fn entity_type_aliases() -> Vec<TypeAliasDescriptor> {
    vec![
        TypeAliasDescriptor::new(
            "CreditType",
            string_enum([
                "artist",
                "vocalist",
                "instrumentalist",
                "composer",
                "lyricist",
                "arranger",
                "writer",
                "producer",
                "conductor",
                "engineer",
                "mixer",
                "remixer",
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "ArtistCreditSource",
            string_enum(["track", "release"]),
            None,
        ),
        TypeAliasDescriptor::new(
            "ReleaseType",
            string_enum([
                "album",
                "single",
                "ep",
                "compilation",
                "soundtrack",
                "live",
                "remix",
                "broadcast",
                "other",
                "unknown",
            ]),
            None,
        ),
        TypeAliasDescriptor::new(
            "ArtistType",
            string_enum(["person", "group", "character", "orchestra", "choir"]),
            None,
        ),
        TypeAliasDescriptor::new("Track", track_type(), None),
        TypeAliasDescriptor::new("Release", release_type(), None),
        TypeAliasDescriptor::new("Artist", artist_type(), None),
        types::LuauReleaseProjectionKind::type_alias_descriptor(),
        types::LuauTrackProjectionKind::type_alias_descriptor(),
        types::LuauArtistProjectionKind::type_alias_descriptor(),
        TypeAliasDescriptor::new("EntityIncludeSelector", include_selector_type(), None),
        TypeAliasDescriptor::new(
            "EntityProjectionInfo",
            LuauType::union(vec![
                LuauType::named("ReleaseProjectionInfo"),
                LuauType::named("TrackProjectionInfo"),
                LuauType::named("ArtistProjectionInfo"),
            ]),
            Some("Typed entity projection keyed by entity_type."),
        ),
    ]
}

#[cfg(feature = "docgen")]
fn entity_interfaces() -> Vec<harmony_luau::InterfaceDescriptor> {
    let mut options = harmony_luau::InterfaceDescriptor::new("EntityGetOptions", None);
    options.fields.push(FieldDescriptor {
        name: "include",
        ty: LuauType::optional(LuauType::named("EntityIncludeSelector")),
        description: Some("What each projection includes. Defaults to nothing."),
    });
    options.fields.push(FieldDescriptor {
        name: "library_id",
        ty: Option::<String>::luau_type(),
        description: Some(
            "The library whose locale `links` follow, and under whose root `tracks` read lookup hints from paths. Defaults to none.",
        ),
    });
    vec![
        options,
        types::LuauEntityLookupHints::interface_descriptor(),
        types::LuauProjectionEntryInfo::interface_descriptor(),
        types::LuauCreditProjectionInfo::interface_descriptor(),
        types::LuauCreditedArtistProjectionInfo::interface_descriptor(),
        types::LuauReleaseProjectionTrack::interface_descriptor(),
        types::LuauReleaseProjectionIncludes::interface_descriptor(),
        types::LuauTrackProjectionIncludes::interface_descriptor(),
        types::LuauArtistProjectionIncludes::interface_descriptor(),
        types::LuauReleaseProjectionInfo::interface_descriptor(),
        types::LuauTrackProjectionInfo::interface_descriptor(),
        types::LuauArtistProjectionInfo::interface_descriptor(),
        crate::plugins::types::LuauIdLink::interface_descriptor(),
    ]
}

#[cfg(feature = "docgen")]
fn describe_module(descriptor: &mut ModuleDescriptor) {
    descriptor.fields = [
        (vec!["CreditType", "Artist"], "artist"),
        (vec!["CreditType", "Vocalist"], "vocalist"),
        (vec!["CreditType", "Instrumentalist"], "instrumentalist"),
        (vec!["CreditType", "Composer"], "composer"),
        (vec!["CreditType", "Lyricist"], "lyricist"),
        (vec!["CreditType", "Arranger"], "arranger"),
        (vec!["CreditType", "Writer"], "writer"),
        (vec!["CreditType", "Producer"], "producer"),
        (vec!["CreditType", "Conductor"], "conductor"),
        (vec!["CreditType", "Engineer"], "engineer"),
        (vec!["CreditType", "Mixer"], "mixer"),
        (vec!["CreditType", "Remixer"], "remixer"),
        (vec!["ArtistCreditSource", "Track"], "track"),
        (vec!["ArtistCreditSource", "Release"], "release"),
    ]
    .into_iter()
    .map(|(path, value)| ModuleFieldDescriptor {
        path,
        description: None,
        ty: LuauType::string_literal(value),
    })
    .collect();
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &EntitiesModule::module_descriptor(),
        &entity_type_aliases(),
        &entity_interfaces(),
        &[],
    )
}
