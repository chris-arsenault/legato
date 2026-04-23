//! Project-aware prefetch planning, parsing, and CLI execution.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use flate2::read::GzDecoder;
use legato_client_cache::open_cache_database;
use legato_client_core::{ClientConfig, FilesystemOpenHandle, FilesystemService};
use legato_foundation::load_config;
use legato_types::{PrefetchHintPath, PrefetchPriority};
use regex::Regex;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

/// Supported project/input formats understood by the prefetch planner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ProjectFormat {
    /// Ableton Live `.als` project.
    AbletonAls,
    /// Kontakt `.nki` instrument.
    KontaktNki,
    /// Plugin-state blob such as `.fxp`, `.fxb`, or `.vstpreset`.
    PluginState,
    /// An unsupported file type.
    Unsupported,
}

/// Best-effort Kontakt major version identification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum KontaktVersion {
    /// Kontakt 5-compatible blob.
    V5,
    /// Kontakt 6-compatible blob.
    V6,
    /// Kontakt 7-compatible blob.
    V7,
    /// Unknown or undetected version.
    Unknown,
}

/// A single planner diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Diagnostic {
    /// Short machine-readable code for the diagnostic.
    pub code: String,
    /// Human-readable diagnostic text.
    pub message: String,
}

/// Structured result of analyzing a project or plugin state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProjectAnalysis {
    /// Detected top-level input format.
    pub format: ProjectFormat,
    /// Planner-emitted prefetch hints.
    pub hints: Vec<PrefetchHint>,
    /// Detected plugin descriptors or plugin names.
    pub plugins: Vec<String>,
    /// Planner diagnostics.
    pub diagnostics: Vec<Diagnostic>,
    /// Suggested launcher wait-through priority.
    pub wait_through: PrefetchPriority,
}

/// Serializable prefetch hint used by the CLI output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PrefetchHint {
    /// Canonical or hinted path to warm.
    pub path: String,
    /// Starting byte offset for the hint.
    pub file_offset: u64,
    /// Total byte length requested.
    pub length: u64,
    /// Planner-assigned priority.
    pub priority: PrefetchPriority,
}

impl From<PrefetchHintPath> for PrefetchHint {
    fn from(value: PrefetchHintPath) -> Self {
        Self {
            path: value.path.to_string_lossy().into_owned(),
            file_offset: value.file_offset,
            length: value.length,
            priority: value.priority,
        }
    }
}

impl From<PrefetchHint> for PrefetchHintPath {
    fn from(value: PrefetchHint) -> Self {
        Self {
            path: PathBuf::from(value.path),
            file_offset: value.file_offset,
            length: value.length,
            priority: value.priority,
        }
    }
}

/// CLI subcommand selection for `legato-prefetch`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrefetchCommand {
    /// Analyze a project and print the planned hints.
    Analyze {
        /// Project or state file to inspect.
        project_path: PathBuf,
        /// Whether to emit JSON instead of a text summary.
        json: bool,
    },
    /// Analyze and execute prefetches, optionally waiting through a priority.
    Run {
        /// Project or state file to inspect.
        project_path: PathBuf,
        /// Whether to emit JSON instead of a text summary.
        json: bool,
        /// Requested wait-through priority.
        wait_through: PrefetchPriority,
        /// Path to the generated `legatofs.toml` client config.
        config_path: Option<PathBuf>,
    },
}

/// Summary of one real local prefetch run.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ExecutionReport {
    /// Hints accepted for processing after analysis.
    pub accepted: usize,
    /// Hints already resident before work began.
    pub skipped: usize,
    /// Hints that completed by fetching or reading through the local cache.
    pub completed: usize,
    /// Hints that failed without corrupting local state.
    pub failed: usize,
    /// Bytes returned by completed read-through work.
    pub bytes_read: u64,
    /// Net new bytes represented in the local extent store after the run.
    pub bytes_fetched: u64,
}

#[derive(Debug, Default, Deserialize)]
struct PrefetchClientProcessConfig {
    #[serde(default)]
    client: ClientConfig,
    #[serde(default)]
    mount: PrefetchMountConfig,
}

#[derive(Debug, Deserialize)]
struct PrefetchMountConfig {
    #[serde(default = "default_state_dir")]
    state_dir: String,
}

impl Default for PrefetchMountConfig {
    fn default() -> Self {
        Self {
            state_dir: default_state_dir(),
        }
    }
}

/// Structured result returned by the CLI runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandResult {
    /// Process exit code.
    pub exit_code: i32,
    /// User-facing output.
    pub output: String,
}

/// Errors surfaced by project parsing or CLI execution.
#[derive(Debug)]
pub enum PrefetchError {
    /// Underlying I/O failure.
    Io(std::io::Error),
    /// The provided CLI invocation is invalid.
    InvalidCli(String),
    /// The input format is unsupported.
    UnsupportedFormat(PathBuf),
    /// The input file could not be interpreted.
    Parse(String),
    /// Downstream cache/runtime operation failed.
    Runtime(String),
}

impl std::fmt::Display for PrefetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::InvalidCli(message) => write!(formatter, "invalid CLI usage: {message}"),
            Self::UnsupportedFormat(path) => {
                write!(formatter, "unsupported project format: {}", path.display())
            }
            Self::Parse(message) => write!(formatter, "parse error: {message}"),
            Self::Runtime(message) => write!(formatter, "runtime error: {message}"),
        }
    }
}

impl std::error::Error for PrefetchError {}

impl From<std::io::Error> for PrefetchError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Parses a `legato-prefetch` CLI invocation.
pub fn parse_cli_args<I, S>(args: I) -> Result<PrefetchCommand, PrefetchError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let tokens = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let tokens = if tokens.is_empty() {
        Vec::new()
    } else {
        tokens[1..].to_vec()
    };

    if tokens.is_empty() {
        return Err(PrefetchError::InvalidCli(String::from(
            "expected `analyze <path>` or `run <path> [--wait-through P0|P1|P2|P3] [--json]`",
        )));
    }

    let mut json = false;
    let mut wait_through = PrefetchPriority::P1;
    let mut config_path = None;
    let mut positionals = Vec::new();
    let command = tokens[0].clone();
    let mut index = 1;

    while index < tokens.len() {
        match tokens[index].as_str() {
            "--json" => {
                json = true;
                index += 1;
            }
            "--wait-through" => {
                let Some(value) = tokens.get(index + 1) else {
                    return Err(PrefetchError::InvalidCli(String::from(
                        "missing value for --wait-through",
                    )));
                };
                wait_through = parse_priority(value)?;
                index += 2;
            }
            "--config" => {
                let Some(value) = tokens.get(index + 1) else {
                    return Err(PrefetchError::InvalidCli(String::from(
                        "missing value for --config",
                    )));
                };
                config_path = Some(PathBuf::from(value));
                index += 2;
            }
            other => {
                positionals.push(String::from(other));
                index += 1;
            }
        }
    }

    let Some(project_path) = positionals.first() else {
        return Err(PrefetchError::InvalidCli(String::from(
            "missing project path argument",
        )));
    };

    match command.as_str() {
        "analyze" => Ok(PrefetchCommand::Analyze {
            project_path: PathBuf::from(project_path),
            json,
        }),
        "run" => Ok(PrefetchCommand::Run {
            project_path: PathBuf::from(project_path),
            json,
            wait_through,
            config_path,
        }),
        _ => Err(PrefetchError::InvalidCli(format!(
            "unknown command `{command}`"
        ))),
    }
}

/// Executes one CLI command end to end.
pub fn run_cli_command(command: PrefetchCommand) -> Result<CommandResult, PrefetchError> {
    match command {
        PrefetchCommand::Analyze { project_path, json } => {
            let analysis = analyze_project(&project_path)?;
            Ok(CommandResult {
                exit_code: 0,
                output: render_analysis(&analysis, json)?,
            })
        }
        PrefetchCommand::Run {
            project_path,
            json,
            wait_through,
            config_path,
        } => {
            let mut analysis = analyze_project(&project_path)?;
            analysis.wait_through = wait_through;
            let execution = execute_analysis(&analysis, config_path.as_deref())?;
            Ok(CommandResult {
                exit_code: 0,
                output: render_execution(&analysis, &execution, json)?,
            })
        }
    }
}

/// Analyzes a project or plugin-state file and returns structured hints.
pub fn analyze_project(path: &Path) -> Result<ProjectAnalysis, PrefetchError> {
    let bytes = fs::read(path)?;
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase());

    match extension.as_deref() {
        Some("als") => analyze_als(path, &bytes),
        Some("nki") => analyze_nki(path, &bytes),
        Some("fxp") | Some("fxb") | Some("vstpreset") => analyze_plugin_state(path, &bytes),
        _ => Err(PrefetchError::UnsupportedFormat(path.to_path_buf())),
    }
}

fn analyze_als(path: &Path, bytes: &[u8]) -> Result<ProjectAnalysis, PrefetchError> {
    let xml = decode_als_xml(bytes)?;
    let sample_paths = extract_paths_from_text(&xml, sample_path_regex());
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
        message: format!("analyzed Ableton Live set {}", path.display()),
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

fn analyze_nki(path: &Path, bytes: &[u8]) -> Result<ProjectAnalysis, PrefetchError> {
    let version = detect_kontakt_version(bytes);
    let structured_paths = match version {
        KontaktVersion::V6 => parse_kontakt_v6(bytes),
        KontaktVersion::V7 => parse_kontakt_v7(bytes),
        KontaktVersion::V5 | KontaktVersion::Unknown => extract_kontakt_paths(bytes),
    };
    let paths = if structured_paths.is_empty() {
        extract_kontakt_paths(bytes)
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
                path.display()
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

fn analyze_plugin_state(path: &Path, bytes: &[u8]) -> Result<ProjectAnalysis, PrefetchError> {
    let lower_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();

    let plugins = detect_plugin_names(&lower_name, bytes);
    let mut hints = Vec::new();

    if plugins
        .iter()
        .any(|plugin| plugin.eq_ignore_ascii_case("kontakt"))
    {
        hints.extend(
            extract_kontakt_paths(bytes)
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
        extract_plugin_paths(bytes)
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
            message: format!("analyzed plugin state {}", path.display()),
        }],
        wait_through: PrefetchPriority::P1,
    })
}

fn decode_als_xml(bytes: &[u8]) -> Result<String, PrefetchError> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(bytes);
        let mut xml = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut xml)
            .map_err(|error| PrefetchError::Parse(error.to_string()))?;
        return Ok(xml);
    }

    String::from_utf8(bytes.to_vec()).map_err(|error| PrefetchError::Parse(error.to_string()))
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

fn extract_kontakt_paths(bytes: &[u8]) -> Vec<String> {
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

fn extract_plugin_paths(bytes: &[u8]) -> Vec<String> {
    let regex = generic_path_regex();
    let mut paths = extract_paths_from_text(&String::from_utf8_lossy(bytes), regex.clone());
    for utf16_text in decode_utf16le_candidates(bytes) {
        paths.extend(extract_paths_from_text(&utf16_text, regex.clone()));
        paths.extend(extract_path_tokens(&utf16_text));
    }
    dedup_sorted(paths)
}

fn extract_paths_from_text(text: &str, regex: Regex) -> Vec<String> {
    regex
        .find_iter(text)
        .map(|match_| sanitize_path(match_.as_str()))
        .filter(|path| !path.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
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

fn extract_plugins_from_text(text: &str) -> Vec<String> {
    let regex = Regex::new("(?i)(kontakt|serum|omnisphere|falcon|play|massive)").expect("regex");
    regex
        .find_iter(text)
        .map(|match_| capitalize_plugin(match_.as_str()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn detect_plugin_names(file_name: &str, bytes: &[u8]) -> Vec<String> {
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

fn detect_kontakt_version(bytes: &[u8]) -> KontaktVersion {
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

fn execute_analysis(
    analysis: &ProjectAnalysis,
    config_path: Option<&Path>,
) -> Result<ExecutionReport, PrefetchError> {
    let config_path = config_path
        .map(Path::to_path_buf)
        .unwrap_or_else(default_config_path);
    let process_config =
        load_config::<PrefetchClientProcessConfig>(Some(&config_path), "LEGATO_FS")
            .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    runtime.block_on(execute_analysis_live(analysis, process_config))
}

async fn execute_analysis_live(
    analysis: &ProjectAnalysis,
    process_config: PrefetchClientProcessConfig,
) -> Result<ExecutionReport, PrefetchError> {
    let state_dir = PathBuf::from(&process_config.mount.state_dir);
    let bytes_before = local_extent_bytes(&state_dir)?;
    let mut service = FilesystemService::connect(
        process_config.client,
        default_client_name(),
        Path::new(&process_config.mount.state_dir),
    )
    .await
    .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    let mut report = ExecutionReport::default();

    for hint in scheduled_hints(
        analysis
            .hints
            .clone()
            .into_iter()
            .map(PrefetchHintPath::from)
            .collect(),
    ) {
        report.accepted += 1;
        match prefetch_one_hint(&mut service, &state_dir, &hint).await {
            Ok(PrefetchHintOutcome::AlreadyResident) => report.skipped += 1,
            Ok(PrefetchHintOutcome::Completed { bytes_read }) => {
                report.completed += 1;
                report.bytes_read = report.bytes_read.saturating_add(bytes_read);
            }
            Err(_error) => report.failed += 1,
        }
    }

    let bytes_after = local_extent_bytes(&state_dir)?;
    report.bytes_fetched = bytes_after.saturating_sub(bytes_before);
    Ok(report)
}

fn render_analysis(analysis: &ProjectAnalysis, json: bool) -> Result<String, PrefetchError> {
    if json {
        return serde_json::to_string_pretty(analysis)
            .map_err(|error| PrefetchError::Runtime(error.to_string()));
    }

    Ok(format!(
        "format: {:?}\nhints: {}\nplugins: {}\nwait-through: {:?}",
        analysis.format,
        analysis.hints.len(),
        analysis.plugins.join(", "),
        analysis.wait_through
    ))
}

fn render_execution(
    analysis: &ProjectAnalysis,
    execution: &ExecutionReport,
    json: bool,
) -> Result<String, PrefetchError> {
    if json {
        return serde_json::to_string_pretty(&(analysis, execution))
            .map_err(|error| PrefetchError::Runtime(error.to_string()));
    }

    Ok(format!(
        "accepted: {}\nskipped: {}\ncompleted: {}\nfailed: {}\nbytes-read: {}\nbytes-fetched: {}\nwait-through: {:?}",
        execution.accepted,
        execution.skipped,
        execution.completed,
        execution.failed,
        execution.bytes_read,
        execution.bytes_fetched,
        analysis.wait_through
    ))
}

enum PrefetchHintOutcome {
    AlreadyResident,
    Completed { bytes_read: u64 },
}

async fn prefetch_one_hint(
    service: &mut FilesystemService,
    state_dir: &Path,
    hint: &PrefetchHintPath,
) -> Result<PrefetchHintOutcome, PrefetchError> {
    let handle = service
        .open(hint.path.to_string_lossy().as_ref())
        .await
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    if hint_already_resident(state_dir, &handle, hint)? {
        service
            .release(handle.local_handle)
            .await
            .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
        return Ok(PrefetchHintOutcome::AlreadyResident);
    }

    let read_size = hint
        .length
        .max(1)
        .min(u64::from(u32::MAX))
        .min(handle.size.saturating_sub(hint.file_offset)) as u32;
    let bytes = service
        .read(handle.local_handle, hint.file_offset, read_size)
        .await
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    service
        .release(handle.local_handle)
        .await
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    Ok(PrefetchHintOutcome::Completed {
        bytes_read: bytes.len() as u64,
    })
}

fn hint_already_resident(
    state_dir: &Path,
    handle: &FilesystemOpenHandle,
    hint: &PrefetchHintPath,
) -> Result<bool, PrefetchError> {
    let required = handle
        .extents
        .iter()
        .filter(|extent| {
            let extent_end = extent.file_offset.saturating_add(extent.length);
            let hint_end = hint.file_offset.saturating_add(hint.length.max(1));
            extent.file_offset < hint_end && extent_end > hint.file_offset
        })
        .collect::<Vec<_>>();
    if required.is_empty() {
        return Ok(false);
    }

    let connection = open_cache_database(&state_dir.join("client.sqlite"))
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    for extent in required {
        let present: Option<i64> = connection
            .query_row(
                "SELECT 1 FROM extent_entries WHERE file_id = ?1 AND extent_index = ?2 AND state = 'ready'",
                [handle.file_id.0 as i64, extent.extent_index as i64],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
        if present.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn local_extent_bytes(state_dir: &Path) -> Result<u64, PrefetchError> {
    let connection = open_cache_database(&state_dir.join("client.sqlite"))
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    connection
        .query_row(
            "SELECT COALESCE(SUM(content_size), 0) FROM extent_entries WHERE state = 'ready'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value.max(0) as u64)
        .map_err(|error| PrefetchError::Runtime(error.to_string()))
}

fn scheduled_hints(mut hints: Vec<PrefetchHintPath>) -> Vec<PrefetchHintPath> {
    hints.sort_by(|left, right| {
        priority_rank(left.priority)
            .cmp(&priority_rank(right.priority))
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.file_offset.cmp(&right.file_offset))
    });
    hints
}

fn sort_and_dedup_hints(hints: &mut Vec<PrefetchHint>) {
    hints.sort_by(|left, right| {
        priority_rank(left.priority)
            .cmp(&priority_rank(right.priority))
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.file_offset.cmp(&right.file_offset))
    });
    hints.dedup();
}

fn parse_priority(value: &str) -> Result<PrefetchPriority, PrefetchError> {
    match value.to_ascii_uppercase().as_str() {
        "P0" => Ok(PrefetchPriority::P0),
        "P1" => Ok(PrefetchPriority::P1),
        "P2" => Ok(PrefetchPriority::P2),
        "P3" => Ok(PrefetchPriority::P3),
        _ => Err(PrefetchError::InvalidCli(format!(
            "invalid priority `{value}`"
        ))),
    }
}

fn priority_rank(priority: PrefetchPriority) -> u8 {
    match priority {
        PrefetchPriority::P0 => 0,
        PrefetchPriority::P1 => 1,
        PrefetchPriority::P2 => 2,
        PrefetchPriority::P3 => 3,
    }
}

fn sample_path_regex() -> Regex {
    Regex::new(r#"(?i)([A-Za-z]:\\[^"'<>|]+?\.(wav|aif|aiff|flac|ncw|mp3)|/[^"'<>|]+?\.(wav|aif|aiff|flac|ncw|mp3))"#)
        .expect("regex")
}

fn generic_path_regex() -> Regex {
    Regex::new(r#"(?i)([A-Za-z]:\\[^"'<>|]{3,}?\.(wav|aif|aiff|flac|ncw|nki|nkr|nkc|sfz|rex|caf)|/[^"'<>|]{3,}?\.(wav|aif|aiff|flac|ncw|nki|nkr|nkc|sfz|rex|caf))"#)
        .expect("regex")
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

fn has_sample_extension(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    [".wav", ".aif", ".aiff", ".flac", ".ncw", ".mp3"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
}

fn default_config_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        return PathBuf::from("/Library/Application Support/Legato/legatofs.toml");
    }
    #[cfg(target_os = "windows")]
    {
        return PathBuf::from("C:\\ProgramData\\Legato\\legatofs.toml");
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        PathBuf::from("/tmp/legatofs.toml")
    }
}

fn default_state_dir() -> String {
    #[cfg(target_os = "macos")]
    {
        return String::from("/Library/Application Support/Legato");
    }
    #[cfg(target_os = "windows")]
    {
        return String::from("C:\\ProgramData\\Legato");
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        String::from("/tmp/legato-state")
    }
}

fn default_client_name() -> String {
    std::env::var("LEGATO_CLIENT_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| String::from("legato-prefetch"))
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use flate2::{Compression, write::GzEncoder};
    use tempfile::tempdir;

    use super::{
        KontaktVersion, PrefetchCommand, ProjectAnalysis, ProjectFormat, analyze_project,
        detect_kontakt_version, parse_cli_args,
    };
    use legato_types::PrefetchPriority;

    #[test]
    fn als_analysis_extracts_samples_and_plugins_from_gzipped_xml() {
        let temp = tempdir().expect("tempdir should be created");
        let project = temp.path().join("session.als");
        let xml = r#"<Ableton><Plugin Device="Kontakt"/><SampleRef Path="/Samples/Kick.wav"/><Audio Path="C:\Library\Snare.aif"/></Ableton>"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(xml.as_bytes())
            .expect("xml should be compressed");
        fs::write(&project, encoder.finish().expect("gzip should finish"))
            .expect("als should be written");

        let analysis = analyze_project(&project).expect("analysis should succeed");

        assert_eq!(analysis.format, ProjectFormat::AbletonAls);
        assert_eq!(analysis.hints.len(), 2);
        assert!(analysis.plugins.iter().any(|plugin| plugin == "Kontakt"));
    }

    #[test]
    fn kontakt_nki_analysis_detects_version_and_extracts_samples() {
        let temp = tempdir().expect("tempdir should be created");
        let project = temp.path().join("piano.nki");
        fs::write(
            &project,
            b"Kontakt 7\x00/Samples/GrandPiano.wav\x00/Samples/RoomMic.flac",
        )
        .expect("nki should be written");

        let analysis = analyze_project(&project).expect("analysis should succeed");

        assert_eq!(analysis.format, ProjectFormat::KontaktNki);
        assert_eq!(detect_kontakt_version(b"Kontakt 7"), KontaktVersion::V7);
        assert!(
            analysis
                .hints
                .iter()
                .any(|hint| hint.path.ends_with("GrandPiano.wav"))
        );
    }

    #[test]
    fn plugin_state_analysis_extracts_utf16_and_known_plugin_paths() {
        let temp = tempdir().expect("tempdir should be created");
        let project = temp.path().join("kontakt.vstpreset");
        let utf16 = "C:\\Library\\Strings\\Long.ncw"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let mut bytes = b"Kontakt".to_vec();
        bytes.extend_from_slice(&utf16);
        fs::write(&project, bytes).expect("plugin preset should be written");

        let analysis = analyze_project(&project).expect("analysis should succeed");

        assert_eq!(analysis.format, ProjectFormat::PluginState);
        assert!(analysis.plugins.iter().any(|plugin| plugin == "Kontakt"));
        assert!(
            analysis
                .hints
                .iter()
                .any(|hint| hint.path.ends_with("Long.ncw"))
        );
    }

    #[test]
    fn cli_parser_and_runner_support_analyze_and_run() {
        let temp = tempdir().expect("tempdir should be created");
        let project = temp.path().join("session.als");
        let xml =
            r#"<Ableton><Plugin Device="Serum"/><SampleRef Path="/Samples/Kick.wav"/></Ableton>"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(xml.as_bytes())
            .expect("xml should be compressed");
        fs::write(&project, encoder.finish().expect("gzip should finish"))
            .expect("als should be written");

        let analyze = parse_cli_args(vec![
            String::from("legato-prefetch"),
            String::from("analyze"),
            project.to_string_lossy().into_owned(),
            String::from("--json"),
        ])
        .expect("analyze command should parse");
        let run = parse_cli_args(vec![
            String::from("legato-prefetch"),
            String::from("run"),
            project.to_string_lossy().into_owned(),
            String::from("--wait-through"),
            String::from("P0"),
            String::from("--config"),
            String::from("/tmp/legatofs.toml"),
        ])
        .expect("run command should parse");

        assert!(matches!(
            analyze,
            PrefetchCommand::Analyze { json: true, .. }
        ));
        assert!(matches!(
            run,
            PrefetchCommand::Run {
                wait_through: PrefetchPriority::P0,
                config_path: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn execution_report_renders_residency_and_fetch_counts() {
        let analysis = ProjectAnalysis {
            format: ProjectFormat::AbletonAls,
            hints: Vec::new(),
            plugins: Vec::new(),
            diagnostics: Vec::new(),
            wait_through: PrefetchPriority::P1,
        };
        let output = super::render_execution(
            &analysis,
            &super::ExecutionReport {
                accepted: 2,
                skipped: 1,
                completed: 1,
                failed: 0,
                bytes_read: 4096,
                bytes_fetched: 4096,
            },
            false,
        )
        .expect("execution should render");

        assert!(output.contains("accepted: 2"));
        assert!(output.contains("skipped: 1"));
        assert!(output.contains("bytes-fetched: 4096"));
    }
}
