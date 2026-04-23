use crate::{Diagnostic, PrefetchError, PrefetchHint, ProjectAnalysis, ProjectFormat};
use legato_types::PrefetchPriority;

use super::{
    AnalyzerMatch, ProjectAnalyzer, ProjectInput,
    shared::{
        decode_als_xml, extract_paths_from_text, extract_plugins_from_text, matches_extension,
        sample_path_regex,
    },
};

/// Analyzer for Ableton `.als` sessions.
#[derive(Debug)]
pub struct AbletonAnalyzer;

/// Static analyzer instance registered in the built-in registry.
pub static ABLETON_ANALYZER: AbletonAnalyzer = AbletonAnalyzer;

impl ProjectAnalyzer for AbletonAnalyzer {
    fn key(&self) -> &'static str {
        "ableton-als"
    }

    fn matches(&self, input: ProjectInput<'_>) -> AnalyzerMatch {
        if matches_extension(input.path, "als") {
            AnalyzerMatch::ByExtension
        } else {
            AnalyzerMatch::Unsupported
        }
    }

    fn analyze(&self, input: ProjectInput<'_>) -> Result<ProjectAnalysis, PrefetchError> {
        let xml = decode_als_xml(input.bytes)?;
        let sample_regex = sample_path_regex();
        let sample_paths = extract_paths_from_text(&xml, &sample_regex);
        let plugins = extract_plugins_from_text(&xml);
        let hints = sample_paths
            .into_iter()
            .map(|path| PrefetchHint {
                path,
                file_offset: 0,
                length: 256 * 1024,
                priority: PrefetchPriority::P1,
            })
            .collect::<Vec<_>>();

        let mut diagnostics = Vec::new();
        diagnostics.push(Diagnostic {
            code: String::from("als_format"),
            message: format!("analyzed Ableton Live set {}", input.path.display()),
        });
        if plugins.is_empty() {
            diagnostics.push(Diagnostic {
                code: String::from("als_plugins_missing"),
                message: String::from("no plugin descriptors detected in ALS payload"),
            });
        }

        Ok(ProjectAnalysis {
            format: ProjectFormat::AbletonAls,
            hints,
            plugins,
            diagnostics,
            wait_through: PrefetchPriority::P1,
        })
    }
}
