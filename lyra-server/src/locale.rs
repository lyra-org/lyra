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

pub fn validate_language(code: &str) -> Result<String, LocaleValidationError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(LocaleValidationError::EmptyLanguage);
    }

    if let Some(lang) = Language::from_639_3(code) {
        return Ok(lang.to_639_3().to_string());
    }

    if let Some(lang) = Language::from_639_1(code) {
        return Ok(lang.to_639_3().to_string());
    }

    if let Some(lang) = Language::from_name(code) {
        return Ok(lang.to_639_3().to_string());
    }

    Err(LocaleValidationError::UnrecognizedLanguage {
        input: code.to_string(),
    })
}

pub fn validate_country(code: &str) -> Result<String, LocaleValidationError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(LocaleValidationError::EmptyCountry);
    }

    let country =
        code.parse::<Country>()
            .map_err(|_| LocaleValidationError::UnrecognizedCountry {
                input: code.to_string(),
            })?;

    Ok(country.alpha2.to_string())
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
        assert_eq!(validate_language("Japanese").unwrap(), "jpn");
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
