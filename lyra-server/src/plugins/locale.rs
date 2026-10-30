// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use harmony_core::ModuleSpec;
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
};

use lyra_metadata::locale;

const UNDETERMINED: &str = "und";

pub(crate) struct LocaleModule;

#[cfg(feature = "docgen")]
struct LanguageInput;

#[cfg(feature = "docgen")]
struct CountryInput;

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

fn install_constants(spec: ModuleSpec) -> ModuleSpec {
    spec.initializer(install_locale_constants)
}

#[cfg(feature = "docgen")]
fn describe_constants(descriptor: &mut ModuleDescriptor) {
    descriptor.fields.push(ModuleFieldDescriptor {
        path: vec!["UNDETERMINED"],
        description: Some("The ISO 639-3 code for an undetermined language."),
        ty: LuauType::string_literal(UNDETERMINED),
    });
}

#[harmony_macros::module(
    id = "lyra/locale",
    description = "Language and country normalization backed by the server's ISO 639 and ISO 3166 tables.",
    docs_feature = "docgen",
    extend = "install_constants",
    describe = "describe_constants"
)]
impl LocaleModule {
    #[harmony(
        description = "Resolves an ISO 639-1 code, ISO 639-3 code, or English name to a language. Returns nil for unrecognized or blank input.",
        args(input: Option<String>),
        returns(Option<LanguageRecord>)
    )]
    fn language(input: Option<luau::Value>) -> Option<luau::OwnedTable> {
        let language = string_value(input).and_then(|input| locale::resolve_language(&input));
        language.map(|language| LanguageRecord::from(language).into_luau_table())
    }

    #[harmony(
        description = "Resolves an alpha-2 code, alpha-3 code, or name to a country. Returns nil for unrecognized or blank input.",
        args(input: Option<String>),
        returns(Option<CountryRecord>)
    )]
    fn country(input: Option<luau::Value>) -> Option<luau::OwnedTable> {
        let country = string_value(input).and_then(|input| locale::resolve_country(&input));
        country.map(|country| CountryRecord::from(country).into_luau_table())
    }

    #[harmony(
        description = "Splits a tag such as \"ja-JP\", \"ja_JP\", or \"en\" into its language and country. Either half is nil when it cannot be resolved.",
        args(tag: Option<String>),
        returns(LocaleRecord)
    )]
    fn parse(tag: Option<luau::Value>) -> luau::OwnedTable {
        string_value(tag)
            .map(|tag| parse_tag(&tag))
            .unwrap_or_default()
            .into_luau_table()
    }

    #[harmony(
        description = "Builds a BCP 47 style tag such as \"ja-JP\", preferring the ISO 639-1 code. Returns nil when the language cannot be resolved; an unresolvable country is omitted.",
        args(language: LanguageInput, country: Option<CountryInput>)
    )]
    fn tag(
        vm: &luau::Vm,
        language: Option<luau::Value>,
        country: Option<luau::Value>,
    ) -> Option<String> {
        let language = language.and_then(|value| language_value(vm, value));
        let country = country.and_then(|value| country_value(vm, value));
        language.map(|language| locale_tag(language, country))
    }

    #[harmony(
        description = "Returns true when both sides resolve to the same language. \"und\" only matches \"und\"; nil or unresolvable input never matches.",
        args(a: Option<LanguageInput>, b: Option<LanguageInput>)
    )]
    fn languages_match(vm: &luau::Vm, a: Option<luau::Value>, b: Option<luau::Value>) -> bool {
        let a = a.and_then(|value| language_code_value(vm, value));
        let b = b.and_then(|value| language_code_value(vm, value));
        languages_match(a.as_deref(), b.as_deref())
    }
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
            table.set_field("language", language.into_luau_table());
        }
        if let Some(country) = self.country {
            table.set_field("country", country.into_luau_table());
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
impl LuauTypeInfo for LanguageInput {
    fn luau_type() -> LuauType {
        LuauType::union(vec![LanguageRecord::luau_type(), String::luau_type()])
    }
}

#[cfg(feature = "docgen")]
impl LuauTypeInfo for CountryInput {
    fn luau_type() -> LuauType {
        LuauType::union(vec![CountryRecord::luau_type(), String::luau_type()])
    }
}

#[cfg(feature = "docgen")]
pub(crate) fn render_luau_definition() -> std::result::Result<String, std::fmt::Error> {
    use harmony_luau::DescribeModule;

    harmony_luau::render_definition_file_with_support(
        &LocaleModule::module_descriptor(),
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
