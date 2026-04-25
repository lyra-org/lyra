// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

pub fn find_year_in(s: &str) -> Option<u32> {
    for token in s.split(|c: char| !c.is_ascii_digit()) {
        if token.len() == 4
            && (token.starts_with("19") || token.starts_with("20"))
            && let Ok(y) = token.parse::<u32>()
        {
            return Some(y);
        }
    }
    None
}

pub fn extract_year(date: Option<&str>, copyright: Option<&str>) -> Option<u32> {
    if let Some(d) = date
        && let Some(y) = find_year_in(d)
    {
        return Some(y);
    }
    if let Some(c) = copyright
        && let Some(y) = find_year_in(c)
    {
        return Some(y);
    }
    None
}

pub(crate) fn normalize_release_date(value: &str) -> Option<String> {
    let value = value.trim();
    match value.len() {
        4 if valid_release_year(value) => Some(value.to_string()),
        7 if valid_release_year_month(value) => Some(value.to_string()),
        10 if valid_release_year_month_day(value) => Some(value.to_string()),
        _ => None,
    }
}

fn valid_release_year(value: &str) -> bool {
    value.as_bytes().iter().all(u8::is_ascii_digit)
}

fn valid_release_year_month(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.get(4) == Some(&b'-')
        && valid_release_year(&value[..4])
        && value[5..7].as_bytes().iter().all(u8::is_ascii_digit)
        && value[5..7]
            .parse::<u32>()
            .is_ok_and(|month| (1..=12).contains(&month))
}

fn valid_release_year_month_day(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.get(7) != Some(&b'-') || !valid_release_year_month(&value[..7]) {
        return false;
    }

    let Ok(year) = value[..4].parse::<u32>() else {
        return false;
    };
    let Ok(month) = value[5..7].parse::<u32>() else {
        return false;
    };
    let Ok(day) = value[8..10].parse::<u32>() else {
        return false;
    };

    (1..=days_in_month(year, month)).contains(&day)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

pub(crate) fn extract_bracketed_or_parenthetical_year(text: &str) -> Option<u32> {
    let bytes = text.as_bytes();

    for idx in 0..bytes.len() {
        if bytes[idx] != b'(' || idx + 6 > bytes.len() {
            continue;
        }
        if bytes[idx + 5] != b')' {
            continue;
        }
        if bytes[idx + 1..idx + 5].iter().all(u8::is_ascii_digit)
            && let Some(token) = text.get(idx + 1..idx + 5)
            && let Some(year) = valid_year_number(token)
        {
            return Some(year);
        }
    }

    for idx in 0..bytes.len() {
        if bytes[idx] != b'[' || idx + 5 > bytes.len() {
            continue;
        }
        if !bytes[idx + 1..idx + 5].iter().all(u8::is_ascii_digit) {
            continue;
        }
        if !bytes[idx + 5..].contains(&b']') {
            continue;
        }
        if let Some(token) = text.get(idx + 1..idx + 5)
            && let Some(year) = valid_year_number(token)
        {
            return Some(year);
        }
    }

    None
}

pub(crate) fn valid_year_number(token: &str) -> Option<u32> {
    let year = token.parse::<u32>().ok()?;
    if (1900..=2100).contains(&year) {
        Some(year)
    } else {
        None
    }
}

pub(crate) fn contains_four_ascii_digits(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() < 4 {
        return false;
    }
    for idx in 0..=bytes.len() - 4 {
        if bytes[idx..idx + 4].iter().all(u8::is_ascii_digit) {
            return true;
        }
    }
    false
}

pub(crate) fn extract_year_from_text_for_lookup(text: &str) -> Option<u32> {
    if let Some(year) = extract_bracketed_or_parenthetical_year(text) {
        return Some(year);
    }

    let bytes = text.as_bytes();
    let mut idx = 0usize;
    while idx + 4 <= bytes.len() {
        if bytes[idx..idx + 4].iter().all(u8::is_ascii_digit) {
            if let Some(token) = text.get(idx..idx + 4)
                && let Some(year) = valid_year_number(token)
            {
                return Some(year);
            }
            idx += 4;
            continue;
        }
        idx += 1;
    }

    None
}
