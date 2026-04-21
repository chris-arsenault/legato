//! Typed configuration loading utilities backed by TOML and env overlays.

use std::path::Path;

use serde::de::DeserializeOwned;

use crate::FoundationError;

/// Loads a typed configuration value from an optional TOML file plus env overrides.
///
/// Environment variables use `__` as the nesting separator, so
/// `LEGATO_SERVER__COMMON__TRACING__LEVEL=debug` maps to
/// `common.tracing.level`.
pub fn load_config<T>(path: Option<&Path>, env_prefix: &str) -> Result<T, FoundationError>
where
    T: DeserializeOwned,
{
    let mut builder = config::Config::builder();

    if let Some(path) = path {
        builder = builder.add_source(
            config::File::from(path)
                .format(config::FileFormat::Toml)
                .required(false),
        );
    }

    builder = builder.add_source(
        config::Environment::with_prefix(env_prefix)
            .separator("__")
            .try_parsing(true),
    );

    Ok(builder.build()?.try_deserialize()?)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde::Deserialize;
    use temp_env::with_var;
    use tempfile::NamedTempFile;

    use super::load_config;

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct TestConfig {
        endpoint: String,
        enabled: bool,
    }

    #[test]
    fn env_values_override_file_backed_configuration() {
        let file = NamedTempFile::new().expect("temp file should be created");
        fs::write(
            file.path(),
            "endpoint = \"legato.lan:7823\"\nenabled = false\n",
        )
        .expect("config fixture should be written");

        let config = with_var("LEGATO_FOUNDATION_TEST__ENABLED", Some("true"), || {
            load_config::<TestConfig>(Some(file.path()), "LEGATO_FOUNDATION_TEST")
                .expect("config should load")
        });

        assert_eq!(
            config,
            TestConfig {
                endpoint: String::from("legato.lan:7823"),
                enabled: true,
            }
        );
    }
}
