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
use harmony_luau::{
    DescribeInterface,
    FieldDescriptor,
    InterfaceDescriptor,
    LuauType,
    LuauTypeInfo,
};
#[cfg(feature = "docgen")]
use harmony_luau::{
    ModuleDescriptor,
    ModuleFieldDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};

use crate::locale;

const UNDETERMINED: &str = "und";

struct LocaleModule;

pub(crate) fn module_spec() -> ModuleSpec {
    ModuleSpec::new("lyra/locale")
        .capability("lyra.locale")
        .function(language_spec())
        .function(country_spec())
        .function(parse_spec())
        .function(tag_spec())
        .function(languages_match_spec())
        .initializer(install_locale_constants)
        .install(|_| Ok(ModuleExport::new(LocaleModule)))
}

fn install_locale_constants(
    vm: &luau::Vm,
    _origin: &harmony_core::ChunkOrigin,
    root: &luau::Table,
) -> luau::runtime::Result<()> {
    root.set_raw(
        vm,
        "UNDETERMINED",
        luau::Value::String(UNDETERMINED.as_bytes().to_vec()),
    )
}

fn language_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("language")
        .named_arg::<Option<String>>("input")
        .returns::<Option<LanguageRecord>>()
        .call(language_callback)
}

fn country_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("country")
        .named_arg::<Option<String>>("input")
        .returns::<Option<CountryRecord>>()
        .call(country_callback)
}

fn parse_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("parse")
        .named_arg::<Option<String>>("tag")
        .returns::<LocaleRecord>()
        .call(parse_callback)
}

fn tag_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("tag")
        .named_arg::<luau::Value>("language")
        .named_arg::<Option<luau::Value>>("country")
        .returns::<Option<String>>()
        .call(tag_callback)
}

fn languages_match_spec() -> FunctionSpec {
    FunctionSpec::sync_fn("languages_match")
        .named_arg::<Option<luau::Value>>("a")
        .named_arg::<Option<luau::Value>>("b")
        .returns::<bool>()
        .call(languages_match_callback)
}

fn language_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: Option<luau::Value> = frame.args.read_optional_named("input")?;
    let language = string_value(input).and_then(|input| locale::resolve_language(&input));
    frame
        .returns
        .write(language.map(|language| LanguageRecord::from(language).into_luau_table()))
}

fn country_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let input: Option<luau::Value> = frame.args.read_optional_named("input")?;
    let country = string_value(input).and_then(|input| locale::resolve_country(&input));
    frame
        .returns
        .write(country.map(|country| CountryRecord::from(country).into_luau_table()))
}

fn parse_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let tag: Option<luau::Value> = frame.args.read_optional_named("tag")?;
    let parsed = string_value(tag)
        .map(|tag| parse_tag(&tag))
        .unwrap_or_default();
    frame.returns.write(parsed.into_luau_table())
}

fn tag_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let language: Option<luau::Value> = frame.args.read_optional_named("language")?;
    let country: Option<luau::Value> = frame.args.read_optional_named("country")?;
    let language = language.and_then(|value| language_value(frame.vm, value));
    let country = country.and_then(|value| country_value(frame.vm, value));
    frame
        .returns
        .write(language.map(|language| locale_tag(language, country)))
}

fn languages_match_callback(mut frame: luau::CallFrame<'_>) -> luau::runtime::Result<()> {
    let a: Option<luau::Value> = frame.args.read_optional_named("a")?;
    let b: Option<luau::Value> = frame.args.read_optional_named("b")?;
    let a = a.and_then(|value| language_code_value(frame.vm, value));
    let b = b.and_then(|value| language_code_value(frame.vm, value));
    frame
        .returns
        .write(languages_match(a.as_deref(), b.as_deref()))
}

// Mirrors `services::metadata::lyrics::selection::language_matches`: both
// sides are normalized ISO 639-3 codes (or "und") and must compare equal.
fn languages_match(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

fn locale_tag(language: isolang::Language, country: Option<celes::Country>) -> String {
    let code = language.to_639_1().unwrap_or_else(|| language.to_639_3());
    match country {
        Some(country) => format!("{code}-{}", country.alpha2),
        None => code.to_string(),
    }
}

fn parse_tag(tag: &str) -> LocaleRecord {
    let mut parts = tag.trim().split(['-', '_']);
    let language = parts.next().and_then(locale::resolve_language);
    let country = parts.next_back().and_then(locale::resolve_country);
    LocaleRecord {
        language: language.map(LanguageRecord::from),
        country: country.map(CountryRecord::from),
    }
}

fn string_value(value: Option<luau::Value>) -> Option<String> {
    match value {
        Some(luau::Value::String(bytes)) => String::from_utf8(bytes).ok(),
        _ => None,
    }
}

fn language_value(vm: &luau::Vm, value: luau::Value) -> Option<isolang::Language> {
    match value {
        luau::Value::String(bytes) => {
            let input = String::from_utf8(bytes).ok()?;
            locale::resolve_language(&input)
        }
        luau::Value::Table(table) => {
            let iso3 = string_value(table.get_raw(vm, "iso3").ok());
            iso3.and_then(|iso3| locale::resolve_language(&iso3))
        }
        _ => None,
    }
}

fn language_code_value(vm: &luau::Vm, value: luau::Value) -> Option<String> {
    language_value(vm, value).map(|language| language.to_639_3().to_string())
}

fn country_value(vm: &luau::Vm, value: luau::Value) -> Option<celes::Country> {
    match value {
        luau::Value::String(bytes) => {
            let input = String::from_utf8(bytes).ok()?;
            locale::resolve_country(&input)
        }
        luau::Value::Table(table) => {
            let alpha2 = string_value(table.get_raw(vm, "alpha2").ok());
            alpha2.and_then(|alpha2| locale::resolve_country(&alpha2))
        }
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LanguageRecord {
    iso3: &'static str,
    iso2: Option<&'static str>,
    name: &'static str,
}

impl From<isolang::Language> for LanguageRecord {
    fn from(language: isolang::Language) -> Self {
        Self {
            iso3: language.to_639_3(),
            iso2: language.to_639_1(),
            name: language.to_name(),
        }
    }
}

impl LanguageRecord {
    fn into_luau_table(self) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::with_capacity(0, 3);
        table.set_field("iso3", string_field(self.iso3));
        table.set_field(
            "iso2",
            self.iso2.map(string_field).unwrap_or(luau::Value::Nil),
        );
        table.set_field("name", string_field(self.name));
        table
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CountryRecord {
    alpha2: &'static str,
    name: &'static str,
}

impl From<celes::Country> for CountryRecord {
    fn from(country: celes::Country) -> Self {
        Self {
            alpha2: country.alpha2,
            name: country.long_name,
        }
    }
}

impl CountryRecord {
    fn into_luau_table(self) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::with_capacity(0, 2);
        table.set_field("alpha2", string_field(self.alpha2));
        table.set_field("name", string_field(self.name));
        table
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LocaleRecord {
    language: Option<LanguageRecord>,
    country: Option<CountryRecord>,
}

impl LocaleRecord {
    fn into_luau_table(self) -> luau::OwnedTable {
        let mut table = luau::OwnedTable::with_capacity(0, 2);
        if let Some(language) = self.language {
            table.set_field(
                "language",
                luau::Value::TableData(language.into_luau_table()),
            );
        }
        if let Some(country) = self.country {
            table.set_field("country", luau::Value::TableData(country.into_luau_table()));
        }
        table
    }
}

fn string_field(value: &str) -> luau::Value {
    luau::Value::String(value.as_bytes().to_vec())
}

impl LuauTypeInfo for LanguageRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Language")
    }
}

impl DescribeInterface for LanguageRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "Language",
            Some("A language resolved through the server's ISO 639 tables."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "iso3",
                ty: String::luau_type(),
                description: Some("ISO 639-3 code."),
            },
            FieldDescriptor {
                name: "iso2",
                ty: Option::<String>::luau_type(),
                description: Some("ISO 639-1 code, when one exists."),
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: Some("English name."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for CountryRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Country")
    }
}

impl DescribeInterface for CountryRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "Country",
            Some("A country resolved through the server's ISO 3166 tables."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "alpha2",
                ty: String::luau_type(),
                description: Some("ISO 3166-1 alpha-2 code."),
            },
            FieldDescriptor {
                name: "name",
                ty: String::luau_type(),
                description: Some("English name."),
            },
        ]);
        descriptor
    }
}

impl LuauTypeInfo for LocaleRecord {
    fn luau_type() -> LuauType {
        LuauType::named("Locale")
    }
}

impl DescribeInterface for LocaleRecord {
    fn interface_descriptor() -> InterfaceDescriptor {
        let mut descriptor = InterfaceDescriptor::new(
            "Locale",
            Some("The language and country halves of a parsed locale tag."),
        );
        descriptor.fields.extend([
            FieldDescriptor {
                name: "language",
                ty: Option::<LanguageRecord>::luau_type(),
                description: None,
            },
            FieldDescriptor {
                name: "country",
                ty: Option::<CountryRecord>::luau_type(),
                description: None,
            },
        ]);
        descriptor
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
fn language_input_type() -> LuauType {
    LuauType::union(vec![LanguageRecord::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
fn country_input_type() -> LuauType {
    LuauType::union(vec![CountryRecord::luau_type(), String::luau_type()])
}

#[cfg(feature = "docgen")]
fn module_descriptor() -> ModuleDescriptor {
    ModuleDescriptor {
        name: "Locale",
        local_name: "locale",
        description: Some(
            "Language and country normalization backed by the server's ISO 639 and ISO 3166 tables.",
        ),
        fields: vec![ModuleFieldDescriptor {
            path: vec!["UNDETERMINED"],
            description: Some("The ISO 639-3 code for an undetermined language."),
            ty: LuauType::string_literal(UNDETERMINED),
        }],
        functions: vec![
            ModuleFunctionDescriptor {
                path: vec!["language"],
                description: Some(
                    "Resolves an ISO 639-1 code, ISO 639-3 code, or English name to a language. Returns nil for unrecognized or blank input.",
                ),
                params: vec![param("input", Option::<String>::luau_type())],
                returns: vec![Option::<LanguageRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["country"],
                description: Some(
                    "Resolves an alpha-2 code, alpha-3 code, or name to a country. Returns nil for unrecognized or blank input.",
                ),
                params: vec![param("input", Option::<String>::luau_type())],
                returns: vec![Option::<CountryRecord>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["parse"],
                description: Some(
                    "Splits a tag such as \"ja-JP\", \"ja_JP\", or \"en\" into its language and country. Either half is nil when it cannot be resolved.",
                ),
                params: vec![param("tag", Option::<String>::luau_type())],
                returns: vec![LocaleRecord::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["tag"],
                description: Some(
                    "Builds a BCP 47 style tag such as \"ja-JP\", preferring the ISO 639-1 code. Returns nil when the language cannot be resolved; an unresolvable country is omitted.",
                ),
                params: vec![
                    param("language", language_input_type()),
                    param("country", LuauType::optional(country_input_type())),
                ],
                returns: vec![Option::<String>::luau_type()],
                yields: false,
            },
            ModuleFunctionDescriptor {
                path: vec!["languages_match"],
                description: Some(
                    "Returns true when both sides resolve to the same language. \"und\" only matches \"und\"; nil or unresolvable input never matches.",
                ),
                params: vec![
                    param("a", LuauType::optional(language_input_type())),
                    param("b", LuauType::optional(language_input_type())),
                ],
                returns: vec![bool::luau_type()],
                yields: false,
            },
        ],
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &module_descriptor(),
        &[],
        &[
            LanguageRecord::interface_descriptor(),
            CountryRecord::interface_descriptor(),
            LocaleRecord::interface_descriptor(),
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_record_exposes_iso_codes_and_name() {
        let record = LanguageRecord::from(locale::resolve_language("Japanese").unwrap());
        assert_eq!(
            record,
            LanguageRecord {
                iso3: "jpn",
                iso2: Some("ja"),
                name: "Japanese",
            }
        );
    }

    #[test]
    fn parse_tag_splits_language_and_country() {
        let parsed = parse_tag("ja_JP");
        assert_eq!(parsed.language.as_ref().map(|l| l.iso3), Some("jpn"));
        assert_eq!(parsed.country.as_ref().map(|c| c.alpha2), Some("JP"));

        let parsed = parse_tag("en-us");
        assert_eq!(parsed.language.as_ref().map(|l| l.iso3), Some("eng"));
        assert_eq!(parsed.country.as_ref().map(|c| c.alpha2), Some("US"));

        let parsed = parse_tag("en");
        assert_eq!(parsed.language.as_ref().map(|l| l.iso3), Some("eng"));
        assert!(parsed.country.is_none());

        let parsed = parse_tag("xx-Atlantis");
        assert_eq!(parsed, LocaleRecord::default());
    }

    #[test]
    fn locale_tag_prefers_iso2_and_falls_back_to_iso3() {
        let japanese = locale::resolve_language("ja").unwrap();
        let japan = locale::resolve_country("Japan").unwrap();
        assert_eq!(locale_tag(japanese, Some(japan)), "ja-JP");
        assert_eq!(locale_tag(japanese, None), "ja");

        let ainu = locale::resolve_language("ain").unwrap();
        assert_eq!(ainu.to_639_1(), None);
        assert_eq!(locale_tag(ainu, None), "ain");
    }

    #[test]
    fn languages_match_requires_both_sides() {
        assert!(languages_match(Some("eng"), Some("eng")));
        assert!(languages_match(Some("und"), Some("und")));
        assert!(!languages_match(Some("und"), Some("eng")));
        assert!(!languages_match(None, Some("eng")));
        assert!(!languages_match(None, None));
    }
}
