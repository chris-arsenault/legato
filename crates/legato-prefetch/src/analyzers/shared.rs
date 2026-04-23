use std::{collections::BTreeSet, path::Path};

use flate2::read::GzDecoder;
use legato_types::PrefetchPriority;
use regex::Regex;

use crate::{PrefetchError, PrefetchHint};

pub(super) fn matches_extension(path: &Path, extension: &str) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case(extension))
}

pub(super) fn decode_als_xml(bytes: &[u8]) -> Result<String, PrefetchError> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(bytes);
        let mut xml = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut xml)
            .map_err(|error| PrefetchError::Parse(error.to_string()))?;
        return Ok(xml);
    }

    String::from_utf8(bytes.to_vec()).map_err(|error| PrefetchError::Parse(error.to_string()))
}

pub(super) fn sample_path_regex() -> Regex {
    Regex::new(
        r#"(?i)([A-Za-z]:\\[^"'<>|]+?\.(wav|aif|aiff|flac|ncw|mp3)|/[^"'<>|]+?\.(wav|aif|aiff|flac|ncw|mp3))"#,
    )
    .expect("regex")
}

pub(super) fn extract_paths_from_text(text: &str, regex: &Regex) -> Vec<String> {
    regex
        .find_iter(text)
        .map(|match_| sanitize_path(match_.as_str()))
        .filter(|path| !path.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn extract_plugin_paths(bytes: &[u8]) -> Vec<String> {
    let regex = generic_path_regex();
    let mut paths = extract_paths_from_text(&String::from_utf8_lossy(bytes), &regex);
    for utf16_text in decode_utf16le_candidates(bytes) {
        paths.extend(extract_paths_from_text(&utf16_text, &regex));
        paths.extend(extract_path_tokens(&utf16_text));
    }
    dedup_sorted(paths)
}

pub(super) fn extract_plugins_from_text(text: &str) -> Vec<String> {
    let regex = Regex::new("(?i)(kontakt|serum|omnisphere|falcon|play|massive)").expect("regex");
    regex
        .find_iter(text)
        .map(|match_| capitalize_plugin(match_.as_str()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn detect_plugin_names(file_name: &str, bytes: &[u8]) -> Vec<String> {
    let mut plugins = BTreeSet::new();
    for candidate in [
        "kontakt",
        "serum",
        "omnisphere",
        "falcon",
        "play",
        "massive",
    ] {
        if file_name.contains(candidate)
            || String::from_utf8_lossy(bytes)
                .to_ascii_lowercase()
                .contains(candidate)
        {
            plugins.insert(capitalize_plugin(candidate));
        }
    }
    plugins.into_iter().collect()
}

pub(super) fn sort_and_dedup_hints(hints: &mut Vec<PrefetchHint>) {
    hints.sort_by(|left, right| {
        priority_rank(left.priority)
            .cmp(&priority_rank(right.priority))
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.file_offset.cmp(&right.file_offset))
    });
    hints.dedup();
}

pub(super) fn has_sample_extension(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    [".wav", ".aif", ".aiff", ".flac", ".ncw", ".mp3"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
}

fn priority_rank(priority: PrefetchPriority) -> u8 {
    match priority {
        PrefetchPriority::P0 => 0,
        PrefetchPriority::P1 => 1,
        PrefetchPriority::P2 => 2,
        PrefetchPriority::P3 => 3,
    }
}

fn generic_path_regex() -> Regex {
    Regex::new(r#"(?i)([A-Za-z]:\\[^"'<>|]{3,}?\.(wav|aif|aiff|flac|ncw|nki|nkr|nkc|sfz|rex|caf)|/[^"'<>|]{3,}?\.(wav|aif|aiff|flac|ncw|nki|nkr|nkc|sfz|rex|caf))"#)
        .expect("regex")
}

fn extract_path_tokens(text: &str) -> Vec<String> {
    text.split(|character: char| {
        character.is_whitespace()
            || matches!(
                character,
                '"' | '\'' | ';' | ',' | '(' | ')' | '[' | ']' | '{' | '}'
            )
    })
    .map(sanitize_path)
    .filter(|token| {
        token.contains(['\\', '/'])
            && [
                ".wav", ".aif", ".aiff", ".flac", ".ncw", ".nki", ".nkr", ".nkc", ".sfz", ".rex",
                ".caf",
            ]
            .iter()
            .any(|suffix| token.to_ascii_lowercase().ends_with(suffix))
    })
    .collect()
}

fn sanitize_path(path: &str) -> String {
    path.trim_matches(|character| matches!(character, '"' | '\'' | '\0'))
        .replace("\\\\", "\\")
}

fn capitalize_plugin(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    let mut characters = lower.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
        None => String::new(),
    }
}

fn decode_utf16le_candidates(bytes: &[u8]) -> Vec<String> {
    let mut candidates = Vec::new();
    for start in [0usize, 1usize] {
        if bytes.len().saturating_sub(start) < 2 {
            continue;
        }

        let units = bytes[start..]
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        candidates.push(String::from_utf16_lossy(&units));
    }
    candidates
}

fn dedup_sorted(paths: Vec<String>) -> Vec<String> {
    paths
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
