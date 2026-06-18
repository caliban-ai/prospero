//! Resolve a repo root to its caliband control socket, and ensure a daemon is
//! running for it.
//!
//! Mirrors caliban's socket-path rule:
//! `${CALIBAN_DAEMON_RUNTIME_DIR:-$XDG_RUNTIME_DIR/caliban}/<hash16>.sock`,
//! falling back to `${TMPDIR}/caliban-daemon/<hash16>.sock`, where `hash16` is
//! the first 16 hex chars of `SHA-256(canonical_repo_root)`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::net::UnixStream;

use crate::caliband::client::CalibandClient;
use crate::error::{CoreError, Result};

/// First 16 hex chars of `SHA-256` of the path's string form.
pub fn hash16(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    hex::encode(digest)[..16].to_string()
}

/// The subset of process environment that affects socket-path resolution.
/// Captured explicitly so resolution is a pure, testable function.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryEnv {
    /// `$CALIBAN_DAEMON_RUNTIME_DIR` — highest priority base dir.
    pub caliban_daemon_runtime_dir: Option<PathBuf>,
    /// `$XDG_RUNTIME_DIR` — `caliban` subdir is used when set.
    pub xdg_runtime_dir: Option<PathBuf>,
    /// `$TMPDIR` — fallback base, `caliban-daemon` subdir.
    pub tmpdir: Option<PathBuf>,
}

impl DiscoveryEnv {
    /// Capture the relevant variables from the real process environment.
    pub fn from_process() -> Self {
        Self {
            caliban_daemon_runtime_dir: std::env::var_os("CALIBAN_DAEMON_RUNTIME_DIR")
                .map(Into::into),
            xdg_runtime_dir: std::env::var_os("XDG_RUNTIME_DIR").map(Into::into),
            tmpdir: std::env::var_os("TMPDIR").map(Into::into),
        }
    }

    /// The base directory caliband sockets live in, per the resolution rule.
    fn socket_base_dir(&self) -> PathBuf {
        if let Some(dir) = &self.caliban_daemon_runtime_dir {
            dir.clone()
        } else if let Some(xdg) = &self.xdg_runtime_dir {
            xdg.join("caliban")
        } else {
            self.tmpdir
                .clone()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join("caliban-daemon")
        }
    }
}

/// Compute the control socket path for a (already canonical) repo root.
pub fn control_socket_path(repo_root_canonical: &Path, env: &DiscoveryEnv) -> PathBuf {
    env.socket_base_dir()
        .join(format!("{}.sock", hash16(repo_root_canonical)))
}

/// Canonicalize a repo root, mapping the IO error to a discovery error.
///
/// caliband derives its control-socket name by hashing the raw `--repo-root`
/// argument it is given, so prospero must hand caliband the *same* canonical
/// form it hashes for socket lookup — otherwise a symlinked path (e.g. macOS
/// `/tmp` → `/private/tmp`) yields two different socket names and discovery
/// waits forever. (#45)
pub fn canonical_root(repo_root: &Path) -> Result<PathBuf> {
    repo_root.canonicalize().map_err(|e| {
        CoreError::Discovery(format!("cannot canonicalize {}: {e}", repo_root.display()))
    })
}

/// Canonicalize a repo root and compute its control socket path.
pub fn resolve_socket(repo_root: &Path, env: &DiscoveryEnv) -> Result<PathBuf> {
    Ok(control_socket_path(&canonical_root(repo_root)?, env))
}

/// Configuration for [`ensure_caliband`].
#[derive(Debug, Clone)]
pub struct EnsureConfig {
    /// Spawn `caliband --repo-root <root>` if no daemon is reachable.
    pub autostart: bool,
    /// The caliban daemon binary name/path.
    pub caliband_bin: String,
    /// How long to wait for the socket to come up after autostart.
    pub startup_timeout: Duration,
    /// Extra environment variables layered onto the caliband process.
    pub env: std::collections::BTreeMap<String, String>,
}

impl Default for EnsureConfig {
    fn default() -> Self {
        Self {
            autostart: true,
            caliband_bin: "caliband".to_string(),
            startup_timeout: Duration::from_secs(10),
            env: std::collections::BTreeMap::new(),
        }
    }
}

/// Ensure a caliband daemon is reachable for `repo_root`, returning a client
/// bound to its control socket. If none is reachable and `autostart` is set,
/// spawns the daemon and waits for the socket.
pub async fn ensure_caliband(
    repo_root: &Path,
    env: &DiscoveryEnv,
    cfg: &EnsureConfig,
) -> Result<CalibandClient> {
    // Canonicalize ONCE: the socket we wait on and the `--repo-root` we hand
    // caliband must derive from the same path, or their socket names diverge on
    // symlinked roots and we wait forever. (#45)
    let canonical = canonical_root(repo_root)?;
    let socket = control_socket_path(&canonical, env);

    if UnixStream::connect(&socket).await.is_ok() {
        return Ok(CalibandClient::new(socket));
    }

    if !cfg.autostart {
        return Err(CoreError::Discovery(format!(
            "no caliband reachable at {} and autostart is disabled",
            socket.display()
        )));
    }

    tokio::process::Command::new(&cfg.caliband_bin)
        .arg("--repo-root")
        .arg(&canonical)
        .envs(&cfg.env)
        .spawn()
        .map_err(|e| CoreError::Discovery(format!("failed to spawn {} : {e}", cfg.caliband_bin)))?;

    // Poll until the socket accepts a connection or we time out.
    let deadline = tokio::time::Instant::now() + cfg.startup_timeout;
    loop {
        if UnixStream::connect(&socket).await.is_ok() {
            return Ok(CalibandClient::new(socket));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CoreError::Discovery(format!(
                "caliband did not come up at {} within {:?}",
                socket.display(),
                cfg.startup_timeout
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash16_is_stable_and_16_chars() {
        let h = hash16(Path::new("/home/u/dev/prospero"));
        assert_eq!(h.len(), 16);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        // Deterministic.
        assert_eq!(h, hash16(Path::new("/home/u/dev/prospero")));
        // Different path → different hash.
        assert_ne!(h, hash16(Path::new("/home/u/dev/caliban")));
    }

    #[test]
    fn runtime_dir_env_takes_priority() {
        let env = DiscoveryEnv {
            caliban_daemon_runtime_dir: Some("/run/cal".into()),
            xdg_runtime_dir: Some("/run/user/1000".into()),
            tmpdir: Some("/tmp".into()),
        };
        let p = control_socket_path(Path::new("/repo"), &env);
        assert_eq!(
            p,
            PathBuf::from(format!("/run/cal/{}.sock", hash16(Path::new("/repo"))))
        );
    }

    #[test]
    fn xdg_runtime_dir_used_when_no_override() {
        let env = DiscoveryEnv {
            caliban_daemon_runtime_dir: None,
            xdg_runtime_dir: Some("/run/user/1000".into()),
            tmpdir: Some("/tmp".into()),
        };
        let p = control_socket_path(Path::new("/repo"), &env);
        assert_eq!(
            p,
            PathBuf::from(format!(
                "/run/user/1000/caliban/{}.sock",
                hash16(Path::new("/repo"))
            ))
        );
    }

    #[test]
    fn tmpdir_fallback_when_nothing_else() {
        let env = DiscoveryEnv {
            caliban_daemon_runtime_dir: None,
            xdg_runtime_dir: None,
            tmpdir: Some("/var/tmp".into()),
        };
        let p = control_socket_path(Path::new("/repo"), &env);
        assert_eq!(
            p,
            PathBuf::from(format!(
                "/var/tmp/caliban-daemon/{}.sock",
                hash16(Path::new("/repo"))
            ))
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_socket_canonicalizes_symlinked_roots() {
        use std::os::unix::fs::symlink;
        // A symlink whose path differs from its canonical target (mirrors the
        // macOS `/tmp` -> `/private/tmp` case). The socket must be derived from
        // the canonical form so it matches the one caliband creates. (#45)
        let real = tempfile::tempdir().unwrap();
        let real_canon = real.path().canonicalize().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let link = scratch.path().join("link");
        symlink(&real_canon, &link).unwrap();
        assert_ne!(link, real_canon, "symlink path must differ from canonical");

        let env = DiscoveryEnv {
            tmpdir: Some("/var/tmp".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_socket(&link, &env).unwrap(),
            resolve_socket(&real_canon, &env).unwrap(),
            "a symlinked root and its canonical form must resolve to one socket"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ensure_caliband_spawns_caliband_with_the_canonical_root() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        // prospero waits on the socket derived from the CANONICAL root, but
        // caliband hashes the raw `--repo-root` it is handed. If we spawn it
        // with a symlinked path, the two socket names diverge and discovery
        // hangs. This pins the spawn arg to the canonical form. (#45)
        let real = tempfile::tempdir().unwrap();
        let real_canon = real.path().canonicalize().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let link = scratch.path().join("link");
        symlink(&real_canon, &link).unwrap();
        assert_ne!(link, real_canon);

        // A stand-in caliband that records the `--repo-root` it received, then
        // exits without ever creating a socket.
        let recorded = scratch.path().join("recorded-root");
        let script = scratch.path().join("fake-caliband.sh");
        std::fs::write(
            &script,
            format!("#!/bin/sh\nprintf '%s' \"$2\" > '{}'\n", recorded.display()),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cfg = EnsureConfig {
            autostart: true,
            caliband_bin: script.to_string_lossy().into_owned(),
            startup_timeout: Duration::from_millis(300),
            env: std::collections::BTreeMap::new(),
        };
        let env = DiscoveryEnv {
            tmpdir: Some(scratch.path().to_path_buf()),
            ..Default::default()
        };

        // No socket ever appears, so this returns Err after the timeout — we
        // only assert on which root caliband was spawned with.
        let _ = ensure_caliband(&link, &env, &cfg).await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = std::fs::read_to_string(&recorded).expect("fake caliband recorded its root");
        assert_eq!(
            got,
            real_canon.to_string_lossy(),
            "caliband must be spawned with the canonical root, not the symlinked path"
        );
    }
}
