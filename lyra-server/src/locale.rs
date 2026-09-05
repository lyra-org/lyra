// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use celes::Country;
use isolang::Language;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocaleValidationError {
    #[error("language code cannot be empty")]
    EmptyLanguage,
    #[error("unrecognized language: {input}")]
    UnrecognizedLanguage { input: String },
    #[error("country code cannot be empty")]
    EmptyCountry,
    #[error("unrecognized country: {input}")]
    UnrecognizedCountry { input: String },
}

pub fn resolve_language(input: &str) -> Option<Language> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    let lowercase = input.to_ascii_lowercase();
    Language::from_639_3(&lowercase)
        .or_else(|| Language::from_639_1(&lowercase))
        .or_else(|| Language::from_name(input))
        .or_else(|| Language::from_name_lowercase(&lowercase))
}

pub fn resolve_country(input: &str) -> Option<Country> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    input.parse::<Country>().ok()
}

pub fn validate_language(code: &str) -> Result<String, LocaleValidationError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(LocaleValidationError::EmptyLanguage);
    }

    resolve_language(code)
        .map(|lang| lang.to_639_3().to_string())
        .ok_or_else(|| LocaleValidationError::UnrecognizedLanguage {
            input: code.to_string(),
        })
}

pub fn validate_country(code: &str) -> Result<String, LocaleValidationError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(LocaleValidationError::EmptyCountry);
    }

    resolve_country(code)
        .map(|country| country.alpha2.to_string())
        .ok_or_else(|| LocaleValidationError::UnrecognizedCountry {
            input: code.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::{
        LocaleValidationError,
        validate_country,
        validate_language,
    };

    #[test]
    fn validate_language_normalizes_common_inputs() {
        assert_eq!(validate_language("en").unwrap(), "eng");
        assert_eq!(validate_language("ENG").unwrap(), "eng");
        assert_eq!(validate_language("Japanese").unwrap(), "jpn");
    }

    #[test]
    fn validate_language_matches_names_case_insensitively() {
        assert_eq!(validate_language("japanese").unwrap(), "jpn");
        assert_eq!(validate_language("JAPANESE").unwrap(), "jpn");
        assert_eq!(validate_language(" japanese ").unwrap(), "jpn");
    }

    #[test]
    fn validate_language_rejects_empty_input() {
        assert_eq!(
            validate_language("   ").unwrap_err(),
            LocaleValidationError::EmptyLanguage
        );
    }

    #[test]
    fn validate_country_normalizes_common_inputs() {
        assert_eq!(validate_country("US").unwrap(), "US");
        assert_eq!(validate_country("Japan").unwrap(), "JP");
    }

    #[test]
    fn validate_country_rejects_unknown_input() {
        assert_eq!(
            validate_country("Atlantis").unwrap_err(),
            LocaleValidationError::UnrecognizedCountry {
                input: "Atlantis".to_string(),
            }
        );
    }
}
