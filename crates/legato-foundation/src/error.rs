//! Shared error definitions for bootstrap and operational plumbing.

/// Common failure modes encountered by shared foundation helpers.
#[derive(Debug)]
pub enum FoundationError {
    /// Wrapper for configuration loading failures.
    Config(config::ConfigError),
    /// Wrapper for tracing subscriber setup failures.
    TracingFilter(tracing_subscriber::filter::ParseError),
    /// Wrapper for global subscriber initialization failures.
    TracingInit(tracing_subscriber::util::TryInitError),
}

impl std::fmt::Display for FoundationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "configuration error: {error}"),
            Self::TracingFilter(error) => write!(formatter, "tracing filter error: {error}"),
            Self::TracingInit(error) => write!(formatter, "tracing initialization error: {error}"),
        }
    }
}

impl std::error::Error for FoundationError {}

impl From<config::ConfigError> for FoundationError {
    fn from(error: config::ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<tracing_subscriber::filter::ParseError> for FoundationError {
    fn from(error: tracing_subscriber::filter::ParseError) -> Self {
        Self::TracingFilter(error)
    }
}

impl From<tracing_subscriber::util::TryInitError> for FoundationError {
    fn from(error: tracing_subscriber::util::TryInitError) -> Self {
        Self::TracingInit(error)
    }
}
