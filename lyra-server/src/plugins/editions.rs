// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_luau as luau;
#[cfg(feature = "docgen")]
use harmony_luau::{
    DescribeModule,
    render_definition_file_with_support,
};
use harmony_luau::{
    LuauType,
    LuauTypeInfo,
};
use lyra_metadata::MediaFormat;

pub(crate) struct EditionsModule;

/// A format list given as a string or an array of strings, read leniently.
struct MediaFormatsInput(Option<luau::Value>);

impl<'vm> luau::FromLuau<'vm> for MediaFormatsInput {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        reader.read().map(Self)
    }
}

impl LuauTypeInfo for MediaFormatsInput {
    fn luau_type() -> LuauType {
        LuauType::optional(LuauType::union(vec![
            String::luau_type(),
            Vec::<String>::luau_type(),
        ]))
    }
}

/// A string argument that reads as nothing when it is anything else.
struct LenientString(Option<String>);

impl<'vm> luau::FromLuau<'vm> for LenientString {
    fn read(reader: &mut luau::ArgReader<'vm>) -> luau::runtime::Result<Self> {
        let value: Option<luau::Value> = reader.read()?;
        Ok(Self(string_value(value)))
    }
}

impl LuauTypeInfo for LenientString {
    fn luau_type() -> LuauType {
        Option::<String>::luau_type()
    }
}

#[harmony_macros::module(
    id = "lyra/editions",
    description = "Normalizes edition formats and barcodes. Release contexts are already normalized; providers must map formats outside file-tag vocabularies to canonical names.",
    docs_feature = "docgen"
)]
impl EditionsModule {
    #[harmony(
        description = "Parses tag values or canonical names (a string or array) into distinct formats: \"cd\", \"vinyl\", \"digital\", \"cassette\", \"sacd\", \"dvd\", \"bluray\", \"minidisc\", \"other\". Drops unrecognized values.",
        returns(Vec<String>)
    )]
    fn media_formats(input: MediaFormatsInput) -> luau::runtime::Result<luau::OwnedTable> {
        let formats = media_formats_value(input.0)?;
        Ok(luau::OwnedTable::from_array(
            formats.into_iter().map(|format| format.as_str()),
        ))
    }

    #[harmony(
        description = "Returns true when both inputs share a format parsed by `media_formats`. Unrecognized formats never match."
    )]
    fn media_formats_match(
        a: MediaFormatsInput,
        b: MediaFormatsInput,
    ) -> luau::runtime::Result<bool> {
        let a = media_formats_value(a.0)?;
        let b = media_formats_value(b.0)?;
        Ok(lyra_metadata::media_formats_match(&a, &b))
    }

    #[harmony(
        description = "Normalizes a UPC, EAN, or GTIN to its zero-padded 14-digit form, so every spelling of one code compares equal. Returns nil for input that is not a plausible barcode."
    )]
    fn barcode(input: LenientString) -> Option<String> {
        input
            .0
            .and_then(|input| lyra_metadata::normalize_barcode(&input))
    }

    #[harmony(
        description = "Returns true when both sides normalize to the same barcode. Nil or invalid input never matches."
    )]
    fn barcodes_match(a: LenientString, b: LenientString) -> bool {
        match (a.0, b.0) {
            (Some(a), Some(b)) => lyra_metadata::barcodes_match(&a, &b),
            _ => false,
        }
    }
}

fn media_formats_value(value: Option<luau::Value>) -> luau::runtime::Result<Vec<MediaFormat>> {
    match value {
        Some(luau::Value::Table(table)) => {
            let strings: Vec<String> = table
                .array_values()?
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
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(&EditionsModule::module_descriptor(), &[], &[], &[])
}
