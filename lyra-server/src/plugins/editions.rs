// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use lyra_metadata::MediaFormat;

struct EditionsModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/editions")
        .capability("lyra.editions")
        .function(media_formats_spec())
        .function(media_formats_match_spec())
        .function(barcode_spec())
        .function(barcodes_match_spec())
        .install(|_| Ok(ModuleExport::new(EditionsModule)))
}

fn media_formats_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("media_formats")
        .named_arg::<Option<luau::Value>>("input")
        .returns::<Vec<String>>()
        .call(media_formats_callback)
}

fn media_formats_match_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("media_formats_match")
        .named_arg::<Option<luau::Value>>("a")
        .named_arg::<Option<luau::Value>>("b")
        .returns::<bool>()
        .call(media_formats_match_callback)
}

fn barcode_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("barcode")
        .named_arg::<Option<String>>("input")
        .returns::<Option<String>>()
        .call(barcode_callback)
}

fn barcodes_match_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("barcodes_match")
        .named_arg::<Option<String>>("a")
        .named_arg::<Option<String>>("b")
        .returns::<bool>()
        .call(barcodes_match_callback)
}

fn media_formats_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: Option<luau::Value> = frame.args.read_optional_named("input")?;
    let formats = media_formats_value(frame.vm, input)?;
    let mut result = luau::OwnedTable::with_capacity(formats.len(), 0);
    for format in formats {
        result.push_array(luau::Value::String(format.as_str().as_bytes().to_vec()));
    }
    frame.returns.write(luau::Value::TableData(result))
}

fn media_formats_match_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let a: Option<luau::Value> = frame.args.read_optional_named("a")?;
    let b: Option<luau::Value> = frame.args.read_optional_named("b")?;
    let a = media_formats_value(frame.vm, a)?;
    let b = media_formats_value(frame.vm, b)?;
    frame
        .returns
        .write(lyra_metadata::media_formats_match(&a, &b))
}

fn barcode_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: Option<luau::Value> = frame.args.read_optional_named("input")?;
    let barcode = string_value(input).and_then(|input| lyra_metadata::normalize_barcode(&input));
    frame.returns.write(barcode)
}

fn barcodes_match_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let a: Option<luau::Value> = frame.args.read_optional_named("a")?;
    let b: Option<luau::Value> = frame.args.read_optional_named("b")?;
    let matched = match (string_value(a), string_value(b)) {
        (Some(a), Some(b)) => lyra_metadata::barcodes_match(&a, &b),
        _ => false,
    };
    frame.returns.write(matched)
}

fn media_formats_value(
    vm: &luau::Vm,
    value: Option<luau::Value>,
) -> luau::runtime::Result<Vec<MediaFormat>> {
    match value {
        Some(luau::Value::Table(table)) => {
            let strings: Vec<String> = table
                .array_values_raw(vm)?
                .into_iter()
                .filter_map(|value| string_value(Some(value)))
                .collect();
            Ok(lyra_metadata::collect_media_formats(
                strings.iter().map(String::as_str),
            ))
        }
        other => Ok(string_value(other)
            .map(|input| lyra_metadata::parse_media_formats(&input))
            .unwrap_or_default()),
    }
}

fn string_value(value: Option<luau::Value>) -> Option<String> {
    match value {
        Some(luau::Value::String(bytes)) => String::from_utf8(bytes).ok(),
        _ => None,
    }
}

#[cfg(feature = "docgen")]
fn param(name: &'static str, ty: LuauType) -> ParameterDescriptor {
    ParameterDescriptor {
        name,
        ty,
        description: None,
        variadic: false,
    }
}

#[cfg(feature = "docgen")]
fn media_formats_input_type() -> LuauType {
    LuauType::optional(LuauType::union(vec![
        String::luau_type(),
        Vec::<String>::luau_type(),
    ]))
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Editions",
        local_name: "editions",
        description: Some(
            "Normalization for the signals that tell editions of a release apart: medium format and barcode. Release contexts carry `media_formats` and `barcode` already normalized; use this module to bring provider data onto the same terms.",
        ),
        fields: Vec::new(),
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["media_formats"],
                description: Some(
                    "Parses a format string, or an array of them, into distinct canonical formats: \"cd\", \"vinyl\", \"digital\", \"cassette\", \"sacd\", \"dvd\", \"bluray\", \"minidisc\", \"other\". Understands MusicBrainz format names, ID3v2 media type codes, and tracker media names such as \"WEB\". Unrecognized values are dropped.",
                ),
                params: vec![param("input", media_formats_input_type())],
                returns: vec![Vec::<String>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["media_formats_match"],
                description: Some(
                    "Returns true when both sides share at least one canonical format. Each side may be raw or canonical, a string or an array. A side with no recognized format never matches.",
                ),
                params: vec![
                    param("a", media_formats_input_type()),
                    param("b", media_formats_input_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["barcode"],
                description: Some(
                    "Normalizes a UPC, EAN, or GTIN to its zero-padded 14-digit form, so every spelling of one code compares equal. Returns nil for input that is not a plausible barcode.",
                ),
                params: vec![param("input", Option::<String>::luau_type())],
                returns: vec![Option::<String>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["barcodes_match"],
                description: Some(
                    "Returns true when both sides normalize to the same barcode. Nil or invalid input never matches.",
                ),
                params: vec![
                    param("a", Option::<String>::luau_type()),
                    param("b", Option::<String>::luau_type()),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&module_descriptor(), &[], &[], &[])
}
