use std::path::Path;

use crate::{PrefetchError, ProjectAnalysis};

pub mod ableton;
pub mod kontakt;
pub mod plugin_state;
mod shared;

/// Match strength returned by one registered analyzer.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AnalyzerMatch {
    /// The analyzer does not recognize the supplied input.
    Unsupported,
    /// The analyzer recognizes the input from filename/extension hints.
    ByExtension,
    /// The analyzer recognizes the input from in-band content.
    ByContent,
}

/// Borrowed project input passed through the analyzer registry.
#[derive(Clone, Copy, Debug)]
pub struct ProjectInput<'a> {
    /// Source path used for extension and diagnostics.
    pub path: &'a Path,
    /// Raw file bytes used for format-aware parsing.
    pub bytes: &'a [u8],
}

/// One statically registered project analyzer.
pub trait ProjectAnalyzer: Sync {
    /// Stable analyzer key used for diagnostics and tests.
    fn key(&self) -> &'static str;

    /// Returns how confidently this analyzer recognizes the supplied input.
    fn matches(&self, input: ProjectInput<'_>) -> AnalyzerMatch;

    /// Produces a structured project analysis for the supplied input.
    fn analyze(&self, input: ProjectInput<'_>) -> Result<ProjectAnalysis, PrefetchError>;
}

/// Static analyzer registry used by the prefetch microkernel.
pub struct AnalyzerRegistry {
    analyzers: &'static [&'static dyn ProjectAnalyzer],
}

impl AnalyzerRegistry {
    /// Creates a registry from a static analyzer slice.
    pub const fn new(analyzers: &'static [&'static dyn ProjectAnalyzer]) -> Self {
        Self { analyzers }
    }

    /// Returns the analyzers registered in this registry.
    #[must_use]
    pub fn analyzers(&self) -> &'static [&'static dyn ProjectAnalyzer] {
        self.analyzers
    }

    /// Returns whether any registered analyzer recognizes the supplied path.
    #[must_use]
    pub fn supports_path(&self, path: &Path) -> bool {
        self.select_analyzer(ProjectInput { path, bytes: &[] })
            .is_some()
    }

    /// Runs the best matching analyzer for the supplied project bytes.
    pub fn analyze(&self, path: &Path, bytes: &[u8]) -> Result<ProjectAnalysis, PrefetchError> {
        let input = ProjectInput { path, bytes };
        let Some(analyzer) = self.select_analyzer(input) else {
            return Err(PrefetchError::UnsupportedFormat(path.to_path_buf()));
        };
        analyzer.analyze(input)
    }

    fn select_analyzer(&self, input: ProjectInput<'_>) -> Option<&'static dyn ProjectAnalyzer> {
        let mut selected = None;
        let mut selected_match = AnalyzerMatch::Unsupported;
        for analyzer in self.analyzers {
            let candidate_match = analyzer.matches(input);
            if candidate_match > selected_match {
                selected = Some(*analyzer);
                selected_match = candidate_match;
            }
        }
        selected
    }
}

static BUILTIN_ANALYZERS: [&dyn ProjectAnalyzer; 3] = [
    &ableton::ABLETON_ANALYZER,
    &kontakt::KONTAKT_ANALYZER,
    &plugin_state::PLUGIN_STATE_ANALYZER,
];

static BUILTIN_REGISTRY: AnalyzerRegistry = AnalyzerRegistry::new(&BUILTIN_ANALYZERS);

/// Returns the built-in analyzer registry used by Legato.
#[must_use]
pub fn project_analyzer_registry() -> &'static AnalyzerRegistry {
    &BUILTIN_REGISTRY
}
