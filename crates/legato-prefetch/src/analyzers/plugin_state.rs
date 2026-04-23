use crate::{Diagnostic, PrefetchError, PrefetchHint, ProjectAnalysis, ProjectFormat};
use legato_types::PrefetchPriority;

use super::{
    AnalyzerMatch, ProjectAnalyzer, ProjectInput,
    kontakt::extract_kontakt_paths,
    shared::{detect_plugin_names, extract_plugin_paths, matches_extension, sort_and_dedup_hints},
};

/// Analyzer for plugin-state blobs such as `fxp`, `fxb`, and `vstpreset`.
#[derive(Debug)]
pub struct PluginStateAnalyzer;

/// Static analyzer instance registered in the built-in registry.
pub static PLUGIN_STATE_ANALYZER: PluginStateAnalyzer = PluginStateAnalyzer;

impl ProjectAnalyzer for PluginStateAnalyzer {
    fn key(&self) -> &'static str {
        "plugin-state"
    }

    fn matches(&self, input: ProjectInput<'_>) -> AnalyzerMatch {
        if ["fxp", "fxb", "vstpreset"]
            .iter()
            .any(|extension| matches_extension(input.path, extension))
        {
            AnalyzerMatch::ByExtension
        } else {
            AnalyzerMatch::Unsupported
        }
    }

    fn analyze(&self, input: ProjectInput<'_>) -> Result<ProjectAnalysis, PrefetchError> {
        let lower_name = input
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_ascii_lowercase())
            .unwrap_or_default();

        let plugins = detect_plugin_names(&lower_name, input.bytes);
        let mut hints = Vec::new();

        if plugins
            .iter()
            .any(|plugin| plugin.eq_ignore_ascii_case("kontakt"))
        {
            hints.extend(
                extract_kontakt_paths(input.bytes)
                    .into_iter()
                    .map(|sample_path| PrefetchHint {
                        path: sample_path,
                        file_offset: 0,
                        length: 4 * 1024 * 1024,
                        priority: PrefetchPriority::P0,
                    }),
            );
        }

        hints.extend(
            extract_plugin_paths(input.bytes)
                .into_iter()
                .map(|sample_path| PrefetchHint {
                    path: sample_path,
                    file_offset: 0,
                    length: 256 * 1024,
                    priority: PrefetchPriority::P2,
                }),
        );
        sort_and_dedup_hints(&mut hints);

        Ok(ProjectAnalysis {
            format: ProjectFormat::PluginState,
            hints,
            plugins,
            diagnostics: vec![Diagnostic {
                code: String::from("plugin_state"),
                message: format!("analyzed plugin state {}", input.path.display()),
            }],
            wait_through: PrefetchPriority::P1,
        })
    }
}
