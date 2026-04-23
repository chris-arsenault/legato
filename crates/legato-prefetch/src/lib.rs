//! Project-aware prefetch planning, parsing, CLI execution, and mount-triggered warming.

use std::{
    fs,
    path::{Path, PathBuf},
};

use legato_client_cache::client_store::ClientLegatoStore;
use legato_client_core::{ClientConfig, FilesystemOpenHandle, FilesystemService};
use legato_foundation::load_config;
use legato_types::{PrefetchHintPath, PrefetchPriority};
use serde::{Deserialize, Serialize};

mod analyzers;

pub use analyzers::kontakt::detect_kontakt_version;
pub use analyzers::{AnalyzerMatch, AnalyzerRegistry, ProjectAnalyzer, project_analyzer_registry};

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

const MAX_INLINE_PROJECT_BYTES: u64 = 16 * 1024 * 1024;

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
    analyze_project_bytes(path, &bytes)
}

/// Analyzes project bytes using the supplied path as the format hint.
pub fn analyze_project_bytes(path: &Path, bytes: &[u8]) -> Result<ProjectAnalysis, PrefetchError> {
    analyzers::project_analyzer_registry().analyze(path, bytes)
}

/// Returns whether one path is eligible for project-open prefetch.
#[must_use]
pub fn supports_project_prefetch(path: &Path) -> bool {
    analyzers::project_analyzer_registry().supports_path(path)
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

/// Executes integrated project prefetch for one already-opened project handle.
///
/// The mount adapters call this after a supported project or preset is opened so
/// Legato can warm dependent sample files without requiring a separate CLI step.
pub async fn prefetch_opened_project(
    service: &mut FilesystemService,
    handle: &FilesystemOpenHandle,
) -> Result<Option<ExecutionReport>, PrefetchError> {
    let project_path = Path::new(&handle.path);
    if !supports_project_prefetch(project_path) || handle.size == 0 {
        return Ok(None);
    }
    if handle.size > MAX_INLINE_PROJECT_BYTES {
        return Ok(None);
    }

    let project_bytes = service
        .read(handle.local_handle, 0, handle.size as u32)
        .await
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    let analysis = analyze_project_bytes(project_path, &project_bytes)?;
    let mut report = ExecutionReport::default();

    for hint in scheduled_hints(
        analysis
            .hints
            .into_iter()
            .map(PrefetchHintPath::from)
            .collect(),
    ) {
        report.accepted += 1;
        match prefetch_one_hint_inline(service, &hint).await {
            Ok(bytes_read) => {
                report.completed += 1;
                report.bytes_read = report.bytes_read.saturating_add(bytes_read);
            }
            Err(_error) => report.failed += 1,
        }
    }

    Ok(Some(report))
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

async fn prefetch_one_hint_inline(
    service: &mut FilesystemService,
    hint: &PrefetchHintPath,
) -> Result<u64, PrefetchError> {
    let handle = service
        .open(hint.path.to_string_lossy().as_ref())
        .await
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
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
    Ok(bytes.len() as u64)
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

    let store = ClientLegatoStore::open(state_dir, 0)
        .map_err(|error| PrefetchError::Runtime(error.to_string()))?;
    for extent in required {
        if store
            .get_extent(handle.file_id, extent.extent_index)
            .map_err(|error| PrefetchError::Runtime(error.to_string()))?
            .is_none()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn local_extent_bytes(state_dir: &Path) -> Result<u64, PrefetchError> {
    ClientLegatoStore::open(state_dir, 0)
        .map(|store| store.resident_bytes())
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
    use std::{fs, io::Write, path::Path};

    use flate2::{Compression, write::GzEncoder};
    use tempfile::tempdir;

    use super::{
        KontaktVersion, PrefetchCommand, ProjectAnalysis, ProjectFormat, analyze_project,
        detect_kontakt_version, parse_cli_args, project_analyzer_registry,
        supports_project_prefetch,
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

    #[test]
    fn built_in_registry_exposes_static_analyzers() {
        let registry = project_analyzer_registry();
        let keys = registry
            .analyzers()
            .iter()
            .map(|analyzer| analyzer.key())
            .collect::<Vec<_>>();

        assert_eq!(keys, vec!["ableton-als", "kontakt-nki", "plugin-state"]);
        assert!(supports_project_prefetch(Path::new("session.als")));
        assert!(supports_project_prefetch(Path::new("piano.nki")));
        assert!(supports_project_prefetch(Path::new("preset.vstpreset")));
        assert!(!supports_project_prefetch(Path::new("notes.txt")));
    }
}
