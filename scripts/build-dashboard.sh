#!/usr/bin/env bash
# Build Dashboard v2 (Dioxus → WASM, #97) and write the embeddable bundle to
# crates/api/dashboard-v2/.
#
# This is the single entrypoint used by both humans and CI (.github/workflows/
# ci.yml), so the local and CI code paths are identical — same shape as
# scripts/coverage.sh.
#
# Why the output is committed: crates/api/src/dashboard_v2.rs `include_bytes!`s
# this bundle into prosperod, which keeps the "one binary ships the UI" property
# and means an ordinary `cargo build` needs no wasm toolchain. The cost is a
# build artifact in git, so CI reruns this script and diffs the tree — a stale
# bundle fails the build instead of shipping.
#
# Requirements:
#   * a cargo that can target wasm32-unknown-unknown
#       rustup target add wasm32-unknown-unknown
#   * the wasm-bindgen CLI, at the SAME version as the wasm-bindgen crate
#       cargo install wasm-bindgen-cli --version <ver> --locked
#       (or `brew install wasm-bindgen` when the version happens to match)
#   * optionally binaryen for wasm-opt (`brew install binaryen`) — size only
#
# Environment:
#   CARGO_WASM   path to a cargo binary that can target wasm32 (overrides
#                autodetection; useful when the default toolchain is a Homebrew
#                rust with only the host std installed)
#
# Usage: scripts/build-dashboard.sh

set -euo pipefail

cd "$(dirname "$0")/.."

CRATE_DIR="crates/dashboard"
OUT_DIR="crates/api/dashboard-v2"
TARGET="wasm32-unknown-unknown"
OUT_NAME="prospero-dashboard"

# --- 1. Resolve a cargo that can target wasm32 -------------------------------
# The repo's default toolchain is not guaranteed to have the wasm32 std (a
# Homebrew rust, for instance, ships only the host target), so prefer an
# explicit override, then any rustup toolchain, then whatever is on PATH.

# True when this rustc actually has the target's standard library. Note that
# `rustc --print target-libdir` exits 0 and prints a path even when the target
# is NOT installed, so the path's existence is what must be tested.
has_target_std() {
  local rustc="$1" libdir
  [ -x "$rustc" ] || return 1
  libdir="$("$rustc" --print target-libdir --target "$TARGET" 2>/dev/null)" || return 1
  [ -n "$libdir" ] && [ -d "$libdir" ] && [ -n "$(ls -A "$libdir" 2>/dev/null)" ]
}

resolve_cargo() {
  if [ -n "${CARGO_WASM:-}" ]; then
    echo "$CARGO_WASM"
    return
  fi
  local c
  for c in "$HOME"/.rustup/toolchains/*/bin/cargo; do
    if [ -x "$c" ] && has_target_std "$(dirname "$c")/rustc"; then
      echo "$c"
      return
    fi
  done
  command -v cargo
}

CARGO="$(resolve_cargo)"
if [ -z "$CARGO" ]; then
  echo "error: no cargo found on PATH." >&2
  exit 1
fi
RUSTC="$(dirname "$CARGO")/rustc"
[ -x "$RUSTC" ] || RUSTC="$(command -v rustc)"

if ! has_target_std "$RUSTC"; then
  cat >&2 <<EOF
error: $CARGO cannot build for $TARGET (the standard library for that target
       is not installed).

  Install it with:   rustup target add $TARGET
  Or point CARGO_WASM at a cargo that can:
                     CARGO_WASM=~/.rustup/toolchains/stable-*/bin/cargo $0
EOF
  exit 1
fi

echo "==> building $CRATE_DIR for $TARGET"
echo "    cargo: $CARGO ($("$RUSTC" --version))"
# Pin RUSTC to the resolved cargo's SIBLING rustc. A toolchain-local cargo
# (~/.rustup/toolchains/*/bin/cargo) is not a rustup proxy: left alone it picks
# up whatever `rustc` is first on PATH, which on a machine with a Homebrew rust
# means a different sysroot and a baffling "can't find crate for `core`".
(cd "$CRATE_DIR" && RUSTC="$RUSTC" "$CARGO" build --release --target "$TARGET")

# Binary targets keep the package name verbatim (hyphens preserved) — unlike
# lib targets, which cargo renames to snake_case.
WASM="$CRATE_DIR/target/$TARGET/release/${OUT_NAME}.wasm"
if [ ! -f "$WASM" ]; then
  echo "error: expected $WASM after the build, but it is missing." >&2
  exit 1
fi

# --- 2. Guard wasm-bindgen crate/CLI version skew ----------------------------
# Skew here is the single most common failure in this toolchain and it surfaces
# as opaque runtime errors in the browser rather than a build failure, so catch
# it now with an actionable message.
LOCK_VER="$(awk '
  /^name = "wasm-bindgen"$/ { in_wb = 1; next }
  in_wb && /^version = / { gsub(/[",]/, ""); print $3; exit }
' "$CRATE_DIR/Cargo.lock")"

if [ -z "$LOCK_VER" ]; then
  echo "error: could not read the wasm-bindgen version from $CRATE_DIR/Cargo.lock" >&2
  exit 1
fi

if ! command -v wasm-bindgen >/dev/null 2>&1; then
  cat >&2 <<EOF
error: the wasm-bindgen CLI was not found on PATH.

  Install the matching version:
    cargo install wasm-bindgen-cli --version $LOCK_VER --locked
EOF
  exit 1
fi

CLI_VER="$(wasm-bindgen --version | awk '{print $2}')"
if [ "$CLI_VER" != "$LOCK_VER" ]; then
  cat >&2 <<EOF
error: wasm-bindgen version skew — the crate is $LOCK_VER but the CLI is $CLI_VER.
       Mismatched versions produce a bundle that fails at runtime in the browser.

  Fix with:
    cargo install wasm-bindgen-cli --version $LOCK_VER --locked --force
EOF
  exit 1
fi

# --- 3. Generate the JS glue + processed wasm --------------------------------
echo "==> wasm-bindgen $CLI_VER → $OUT_DIR"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
wasm-bindgen --target web --no-typescript \
  --out-dir "$OUT_DIR" --out-name "$OUT_NAME" "$WASM"

# --- 4. Shrink when binaryen is available (never required) -------------------
if command -v wasm-opt >/dev/null 2>&1; then
  echo "==> wasm-opt -Oz"
  wasm-opt -Oz --enable-bulk-memory --enable-nontrapping-float-to-int \
    "$OUT_DIR/${OUT_NAME}_bg.wasm" -o "$OUT_DIR/${OUT_NAME}_bg.wasm"
else
  echo "note: wasm-opt not found (brew install binaryen) — skipping the size pass."
fi

# --- 5. Static assets --------------------------------------------------------
cp "$CRATE_DIR/index.html" "$CRATE_DIR/app.css" "$OUT_DIR/"

echo "==> bundle written to $OUT_DIR"
ls -la "$OUT_DIR"
