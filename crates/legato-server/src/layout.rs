//! Transfer-layout policy for semantic file classification.

use std::{
    fs,
    path::{Path, PathBuf},
};

use legato_proto::{ExtentDescriptor, FileLayout, TransferClass};
use serde::Deserialize;

/// Reserved file name for per-library layout policy overrides.
pub const DEFAULT_POLICY_FILE: &str = ".legato-layout.toml";

/// Server-side transfer-layout policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutPolicy {
    unitary_max_bytes: u64,
    streamed_extent_bytes: u64,
    random_extent_bytes: u64,
    rules: Vec<LayoutRule>,
}

/// One concrete layout decision for a specific file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutDecision {
    /// Transfer class chosen for the file.
    pub transfer_class: TransferClass,
    /// Preferred extent size for non-unitary files.
    pub extent_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LayoutRule {
    pattern: String,
    transfer_class: TransferClass,
    extent_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LayoutPolicyFile {
    #[serde(default)]
    policy: LayoutPolicyToml,
    #[serde(default)]
    rule: Vec<LayoutRuleToml>,
}

#[derive(Debug, Default, Deserialize)]
struct LayoutPolicyToml {
    unitary_max_bytes: Option<u64>,
    streamed_extent_bytes: Option<u64>,
    random_extent_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LayoutRuleToml {
    pattern: String,
    transfer_class: TransferClassToml,
    extent_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransferClassToml {
    Unitary,
    Streamed,
    Random,
}

impl LayoutPolicy {
    /// Loads a layout policy from the library root if present, otherwise returns defaults.
    pub fn load(library_root: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let policy_path = policy_path(library_root);
        if !policy_path.exists() {
            return Ok(Self::default());
        }

        let parsed: LayoutPolicyFile = toml::from_str(&fs::read_to_string(policy_path)?)?;
        let mut policy = Self::default();
        if let Some(value) = parsed.policy.unitary_max_bytes {
            policy.unitary_max_bytes = value;
        }
        if let Some(value) = parsed.policy.streamed_extent_bytes {
            policy.streamed_extent_bytes = value;
        }
        if let Some(value) = parsed.policy.random_extent_bytes {
            policy.random_extent_bytes = value;
        }
        policy.rules = parsed
            .rule
            .into_iter()
            .map(|rule| LayoutRule {
                pattern: rule.pattern,
                transfer_class: match rule.transfer_class {
                    TransferClassToml::Unitary => TransferClass::Unitary,
                    TransferClassToml::Streamed => TransferClass::Streamed,
                    TransferClassToml::Random => TransferClass::Random,
                },
                extent_bytes: rule.extent_bytes,
            })
            .collect();
        Ok(policy)
    }

    /// Classifies one file path into a transfer-layout decision.
    #[must_use]
    pub fn classify(&self, path: &str, size: u64, is_dir: bool) -> LayoutDecision {
        if is_dir {
            return LayoutDecision {
                transfer_class: TransferClass::Unitary,
                extent_bytes: size.max(1),
            };
        }

        for rule in &self.rules {
            if glob_matches(&rule.pattern, path) {
                return LayoutDecision {
                    transfer_class: rule.transfer_class,
                    extent_bytes: rule
                        .extent_bytes
                        .unwrap_or_else(|| self.default_extent_bytes(rule.transfer_class, size)),
                };
            }
        }

        let transfer_class = infer_transfer_class(path, size, self.unitary_max_bytes);
        LayoutDecision {
            transfer_class,
            extent_bytes: self.default_extent_bytes(transfer_class, size),
        }
    }

    /// Builds one file layout from metadata values.
    #[must_use]
    pub fn file_layout(&self, path: &str, size: u64, is_dir: bool) -> FileLayout {
        let decision = self.classify(path, size, is_dir);
        decision.file_layout(size, is_dir)
    }

    /// Returns the stored layout decision for a file path.
    #[must_use]
    pub fn file_decision(&self, path: &str, size: u64, is_dir: bool) -> LayoutDecision {
        self.classify(path, size, is_dir)
    }
}

impl LayoutDecision {
    /// Builds one file layout from a stored decision.
    #[must_use]
    pub fn file_layout(&self, size: u64, is_dir: bool) -> FileLayout {
        let extent_length = match self.transfer_class {
            TransferClass::Unitary => size.max(1),
            _ => self.extent_bytes.max(1),
        };
        let extent_count = if size == 0 {
            1
        } else {
            size.div_ceil(extent_length)
        };
        let mut extents = Vec::with_capacity(extent_count as usize);
        for extent_index in 0..extent_count {
            let file_offset = extent_index * extent_length;
            let length = if size == 0 {
                0
            } else {
                std::cmp::min(extent_length, size - file_offset)
            };
            extents.push(ExtentDescriptor {
                extent_index: extent_index as u32,
                file_offset,
                length,
                extent_hash: Vec::new(),
            });
        }

        FileLayout {
            transfer_class: if is_dir {
                TransferClass::Unitary as i32
            } else {
                self.transfer_class as i32
            },
            extents,
        }
    }

    /// Returns the preferred extent size for storage and transfer metadata.
    #[must_use]
    pub fn stored_extent_bytes(&self, size: u64, is_dir: bool) -> u64 {
        if is_dir || self.transfer_class == TransferClass::Unitary {
            size.max(1)
        } else {
            self.extent_bytes.max(1)
        }
    }
}

impl LayoutPolicy {
    fn default_extent_bytes(&self, transfer_class: TransferClass, size: u64) -> u64 {
        match transfer_class {
            TransferClass::Unitary => size.max(1),
            TransferClass::Streamed => self.streamed_extent_bytes,
            TransferClass::Random => self.random_extent_bytes,
            TransferClass::Unspecified => self.random_extent_bytes,
        }
    }
}

impl Default for LayoutPolicy {
    fn default() -> Self {
        Self {
            unitary_max_bytes: 4 * 1024 * 1024,
            streamed_extent_bytes: 4 * 1024 * 1024,
            random_extent_bytes: 1024 * 1024,
            rules: Vec::new(),
        }
    }
}

fn infer_transfer_class(path: &str, size: u64, unitary_max_bytes: u64) -> TransferClass {
    let lower = path.to_ascii_lowercase();
    if has_suffix(
        &lower,
        &[".nki", ".nkm", ".fxp", ".fxb", ".vstpreset", ".nicnt"],
    ) || size <= unitary_max_bytes
    {
        return TransferClass::Unitary;
    }
    if has_suffix(&lower, &[".nkr", ".nkc", ".bin", ".db"]) || lower.contains("/steam/") {
        return TransferClass::Random;
    }
    if has_suffix(
        &lower,
        &[".wav", ".aif", ".aiff", ".flac", ".ncw", ".caf", ".rex"],
    ) {
        return TransferClass::Streamed;
    }
    if size > 128 * 1024 * 1024 {
        TransferClass::Random
    } else {
        TransferClass::Streamed
    }
}

fn has_suffix(path: &str, suffixes: &[&str]) -> bool {
    suffixes.iter().any(|suffix| path.ends_with(suffix))
}

/// Returns the reserved policy file path for one library root.
#[must_use]
pub fn policy_path(library_root: &Path) -> PathBuf {
    library_root.join(DEFAULT_POLICY_FILE)
}

/// Returns whether a path refers to the reserved policy file.
#[must_use]
pub fn is_policy_path(library_root: &Path, path: &Path) -> bool {
    path == policy_path(library_root)
}

fn glob_matches(pattern: &str, path: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let mut remaining = path;
    let mut first = true;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        if first && !pattern.starts_with('*') {
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
        } else if let Some(position) = remaining.find(part) {
            remaining = &remaining[position + part.len()..];
        } else {
            return false;
        }
        first = false;
    }
    pattern.ends_with('*') || remaining.is_empty()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use legato_proto::TransferClass;
    use tempfile::tempdir;

    use super::LayoutPolicy;

    #[test]
    fn default_policy_classifies_realistic_library_shapes() {
        let policy = LayoutPolicy::default();

        assert_eq!(
            policy
                .classify("/srv/libraries/Kontakt/Piano/piano.nki", 600 * 1024, false)
                .transfer_class,
            TransferClass::Unitary
        );
        assert_eq!(
            policy
                .classify(
                    "/srv/libraries/Samples/Strings/legato.wav",
                    64 * 1024 * 1024,
                    false
                )
                .transfer_class,
            TransferClass::Streamed
        );
        assert_eq!(
            policy
                .classify(
                    "/srv/libraries/Containers/library.nkr",
                    8 * 1024 * 1024,
                    false
                )
                .transfer_class,
            TransferClass::Random
        );
    }

    #[test]
    fn override_file_can_force_layout_rules() {
        let fixture = tempdir().expect("tempdir should be created");
        fs::write(
            fixture.path().join(".legato-layout.toml"),
            r#"
[policy]
streamed_extent_bytes = 8388608

[[rule]]
pattern = "/srv/libraries/Vendor/*"
transfer_class = "random"
extent_bytes = 2097152
"#,
        )
        .expect("policy file should be written");

        let policy = LayoutPolicy::load(fixture.path()).expect("policy should load");
        let decision = policy.classify(
            "/srv/libraries/Vendor/container.dat",
            64 * 1024 * 1024,
            false,
        );

        assert_eq!(decision.transfer_class, TransferClass::Random);
        assert_eq!(decision.extent_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn file_layout_uses_extent_boundaries_for_streamed_content() {
        let policy = LayoutPolicy::default();
        let layout = policy.file_layout(
            "/srv/libraries/Samples/Brass/sustain.wav",
            10 * 1024 * 1024,
            false,
        );

        assert_eq!(layout.transfer_class, TransferClass::Streamed as i32);
        assert_eq!(layout.extents.len(), 3);
        assert_eq!(layout.extents[0].length, 4 * 1024 * 1024);
        assert_eq!(layout.extents[2].length, 2 * 1024 * 1024);
    }
}
