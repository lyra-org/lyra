// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use crate::{
    LookupHints,
    year::{
        contains_four_ascii_digits,
        extract_bracketed_or_parenthetical_year,
        extract_year_from_text_for_lookup,
    },
};

pub(crate) fn parent_dir_for_lookup(file_path: &str) -> &str {
    let slash_idx = file_path.rfind('/');
    let backslash_idx = file_path.rfind('\\');
    match (slash_idx, backslash_idx) {
        (Some(slash), Some(backslash)) => {
            let idx = slash.max(backslash);
            &file_path[..idx]
        }
        (Some(idx), None) | (None, Some(idx)) => &file_path[..idx],
        (None, None) => "",
    }
}

pub(crate) fn folder_name_for_lookup(parent_dir: &str) -> &str {
    let slash_idx = parent_dir.rfind('/');
    let backslash_idx = parent_dir.rfind('\\');
    match (slash_idx, backslash_idx) {
        (Some(slash), Some(backslash)) => {
            let idx = slash.max(backslash);
            &parent_dir[idx + 1..]
        }
        (Some(idx), None) | (None, Some(idx)) => &parent_dir[idx + 1..],
        (None, None) => parent_dir,
    }
}

pub(crate) fn split_artist_album_lookup_from_hierarchy(
    parent_dir: &str,
) -> (Option<String>, Option<String>) {
    let album_folder = folder_name_for_lookup(parent_dir).trim();
    let artist_folder = folder_name_for_lookup(parent_dir_for_lookup(parent_dir)).trim();

    let cleaned_album = strip_trailing_parenthetical_year_block(&strip_trailing_bracket_block(
        &strip_leading_bracket_block(album_folder),
    ));
    let cleaned_album = cleaned_album.trim();

    let artist_name = if artist_folder.is_empty() {
        None
    } else {
        Some(artist_folder.to_string())
    };

    if artist_name.is_none() {
        return (None, None);
    }

    let album_title = if cleaned_album.is_empty() {
        None
    } else {
        Some(cleaned_album.to_string())
    };

    (artist_name, album_title)
}

pub(crate) fn strip_leading_bracket_block(text: &str) -> String {
    if !text.starts_with('[') {
        return text.to_string();
    }
    let Some(close_idx) = text.find(']') else {
        return text.to_string();
    };
    if close_idx <= 1 {
        return text.to_string();
    }
    text[close_idx + 1..].trim_start().to_string()
}

pub(crate) fn strip_trailing_bracket_block(text: &str) -> String {
    let trimmed = text.trim_end();
    if !trimmed.ends_with(']') {
        return trimmed.to_string();
    }
    let Some(open_idx) = trimmed.rfind('[') else {
        return trimmed.to_string();
    };
    let Some(inner) = trimmed.get(open_idx + 1..trimmed.len() - 1) else {
        return trimmed.to_string();
    };
    if inner.is_empty() || inner.contains(']') {
        return trimmed.to_string();
    }
    trimmed[..open_idx].trim_end().to_string()
}

pub(crate) fn strip_trailing_parenthetical_year_block(text: &str) -> String {
    let trimmed = text.trim_end();
    if !trimmed.ends_with(')') {
        return trimmed.to_string();
    }
    let Some(open_idx) = trimmed.rfind('(') else {
        return trimmed.to_string();
    };
    let Some(inner) = trimmed.get(open_idx + 1..trimmed.len() - 1) else {
        return trimmed.to_string();
    };
    if inner.is_empty() || inner.contains(')') || !contains_four_ascii_digits(inner) {
        return trimmed.to_string();
    }
    trimmed[..open_idx].trim_end().to_string()
}

pub(crate) fn split_artist_album_lookup(cleaned_folder: &str) -> (Option<String>, Option<String>) {
    for (idx, ch) in cleaned_folder.char_indices() {
        if !matches!(ch, '-' | '–' | '—') {
            continue;
        }

        let before = &cleaned_folder[..idx];
        let after = &cleaned_folder[idx + ch.len_utf8()..];
        let before_ws = before.chars().next_back().is_some_and(char::is_whitespace);
        let after_ws = after.chars().next().is_some_and(char::is_whitespace);
        if !before_ws || !after_ws {
            continue;
        }

        let artist = before.trim();
        let album = after.trim();
        let artist_name = if artist.is_empty() {
            None
        } else {
            Some(artist.to_string())
        };
        let album_title = if album.is_empty() {
            None
        } else {
            Some(album.to_string())
        };
        return (artist_name, album_title);
    }

    (None, None)
}

fn strip_library_root_for_lookup(file_path: &str, library_root: &str) -> String {
    let normalized_path = file_path.replace('\\', "/");
    let normalized_root = library_root
        .replace('\\', "/")
        .trim_end_matches('/')
        .to_string();

    if normalized_root.is_empty() {
        return normalized_path;
    }

    if normalized_path.eq_ignore_ascii_case(&normalized_root) {
        return String::new();
    }

    if normalized_path.len() > normalized_root.len()
        && normalized_path
            .get(..normalized_root.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&normalized_root))
        && normalized_path.as_bytes().get(normalized_root.len()) == Some(&b'/')
    {
        return normalized_path
            .get(normalized_root.len() + 1..)
            .unwrap_or_default()
            .to_string();
    }

    normalized_path
}

fn split_artist_album_lookup_from_ancestors(parent_dir: &str) -> (Option<String>, Option<String>) {
    let mut current = parent_dir;
    loop {
        let folder_name = folder_name_for_lookup(current);
        let cleaned_folder = strip_trailing_parenthetical_year_block(
            &strip_trailing_bracket_block(&strip_leading_bracket_block(folder_name)),
        );
        let (artist_name, album_title) = split_artist_album_lookup(cleaned_folder.trim());
        if artist_name.is_some() || album_title.is_some() {
            return (artist_name, album_title);
        }

        let next = parent_dir_for_lookup(current);
        if next.is_empty() || next == current {
            return (None, None);
        }
        current = next;
    }
}

pub(crate) fn extract_year_from_file_path_for_canonical(file_path: &str) -> Option<u32> {
    if file_path.is_empty() {
        return None;
    }

    let parent_dir = parent_dir_for_lookup(file_path);
    let folder_name = folder_name_for_lookup(parent_dir);

    extract_bracketed_or_parenthetical_year(folder_name)
        .or_else(|| extract_bracketed_or_parenthetical_year(file_path))
}

pub fn extract_lookup_hints_from_file_path_with_library_root(
    file_path: &str,
    library_root: Option<&str>,
) -> LookupHints {
    if file_path.is_empty() {
        return LookupHints::default();
    }

    let lookup_path = if let Some(root) = library_root {
        strip_library_root_for_lookup(file_path, root)
    } else {
        file_path.replace('\\', "/")
    };

    let parent_dir = parent_dir_for_lookup(&lookup_path);
    let folder_name = folder_name_for_lookup(parent_dir);

    let inferred_year = extract_year_from_text_for_lookup(folder_name)
        .or_else(|| extract_year_from_text_for_lookup(&lookup_path))
        .or_else(|| extract_year_from_text_for_lookup(file_path));

    let cleaned_folder = strip_trailing_parenthetical_year_block(&strip_trailing_bracket_block(
        &strip_leading_bracket_block(folder_name),
    ));
    let cleaned_folder = cleaned_folder.trim();
    let (artist_name, album_title) = {
        let (artist_name, album_title) = split_artist_album_lookup(cleaned_folder);
        if artist_name.is_some() || album_title.is_some() {
            (artist_name, album_title)
        } else {
            let (artist_name, album_title) = split_artist_album_lookup_from_ancestors(parent_dir);
            if artist_name.is_some() || album_title.is_some() {
                (artist_name, album_title)
            } else if library_root.is_some() {
                split_artist_album_lookup_from_hierarchy(parent_dir)
            } else {
                (None, None)
            }
        }
    };

    LookupHints {
        artist_name,
        album_title,
        year: inferred_year,
    }
}

pub fn extract_lookup_hints_from_file_path(file_path: &str) -> LookupHints {
    extract_lookup_hints_from_file_path_with_library_root(file_path, None)
}

pub fn infer_lookup_hints_from_tracks(track_hints: &[LookupHints]) -> LookupHints {
    let mut inferred_year = None;

    for hints in track_hints {
        if inferred_year.is_none() {
            inferred_year = hints.year;
        }
        if hints.artist_name.is_some() || hints.album_title.is_some() {
            return LookupHints {
                artist_name: hints.artist_name.clone(),
                album_title: hints.album_title.clone(),
                year: inferred_year,
            };
        }
    }

    LookupHints {
        artist_name: None,
        album_title: None,
        year: inferred_year,
    }
}
