use crate::{
    Diagnostic, KontaktVersion, PrefetchError, PrefetchHint, ProjectAnalysis, ProjectFormat,
};
use legato_types::PrefetchPriority;

use super::{
    AnalyzerMatch, ProjectAnalyzer, ProjectInput,
    shared::{extract_plugin_paths, has_sample_extension, matches_extension},
};

/// Analyzer for Kontakt `.nki` instruments.
#[derive(Debug)]
pub struct KontaktAnalyzer;

/// Static analyzer instance registered in the built-in registry.
pub static KONTAKT_ANALYZER: KontaktAnalyzer = KontaktAnalyzer;

impl ProjectAnalyzer for KontaktAnalyzer {
    fn key(&self) -> &'static str {
        "kontakt-nki"
    }

    fn matches(&self, input: ProjectInput<'_>) -> AnalyzerMatch {
        if matches_extension(input.path, "nki") {
            AnalyzerMatch::ByExtension
        } else {
            AnalyzerMatch::Unsupported
        }
    }

    fn analyze(&self, input: ProjectInput<'_>) -> Result<ProjectAnalysis, PrefetchError> {
        let version = detect_kontakt_version(input.bytes);
        let structured_paths = match version {
            KontaktVersion::V6 => parse_kontakt_v6(input.bytes),
            KontaktVersion::V7 => parse_kontakt_v7(input.bytes),
            KontaktVersion::V5 | KontaktVersion::Unknown => extract_kontakt_paths(input.bytes),
        };
        let paths = if structured_paths.is_empty() {
            extract_kontakt_paths(input.bytes)
        } else {
            structured_paths
        };

        let hints = paths
            .into_iter()
            .map(|sample_path| PrefetchHint {
                path: sample_path,
                file_offset: 0,
                length: 4 * 1024 * 1024,
                priority: PrefetchPriority::P0,
            })
            .collect::<Vec<_>>();

        let diagnostics = vec![
            Diagnostic {
                code: String::from("kontakt_version"),
                message: format!("detected Kontakt version {:?}", version),
            },
            Diagnostic {
                code: String::from("kontakt_fallback"),
                message: format!(
                    "planned fallback/structured NKI analysis for {}",
                    input.path.display()
                ),
            },
        ];

        Ok(ProjectAnalysis {
            format: ProjectFormat::KontaktNki,
            hints,
            plugins: vec![String::from("Kontakt")],
            diagnostics,
            wait_through: PrefetchPriority::P0,
        })
    }
}

/// Best-effort Kontakt major version identification.
#[must_use]
pub fn detect_kontakt_version(bytes: &[u8]) -> KontaktVersion {
    let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    if text.contains("kontakt 7") || text.contains("k7") {
        KontaktVersion::V7
    } else if text.contains("kontakt 6") || text.contains("k6") {
        KontaktVersion::V6
    } else if text.contains("kontakt 5") || text.contains("k5") {
        KontaktVersion::V5
    } else {
        KontaktVersion::Unknown
    }
}

pub(crate) fn extract_kontakt_paths(bytes: &[u8]) -> Vec<String> {
    let mut paths = extract_plugin_paths(bytes);
    paths.retain(|path| {
        let lower = path.to_ascii_lowercase();
        has_sample_extension(path)
            || lower.ends_with(".nkr")
            || lower.ends_with(".nkc")
            || lower.ends_with(".nicnt")
    });
    dedup_sorted(paths)
}

fn parse_kontakt_v6(bytes: &[u8]) -> Vec<String> {
    extract_kontakt_paths(bytes)
        .into_iter()
        .filter(|path| has_sample_extension(path))
        .collect()
}

fn parse_kontakt_v7(bytes: &[u8]) -> Vec<String> {
    extract_kontakt_paths(bytes)
        .into_iter()
        .filter(|path| has_sample_extension(path) || path.to_ascii_lowercase().ends_with(".nkr"))
        .collect()
}

fn dedup_sorted(paths: Vec<String>) -> Vec<String> {
    use std::collections::BTreeSet;

    paths
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
