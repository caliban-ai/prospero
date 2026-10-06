#!/usr/bin/env bash
# Copy docs/adr/*.md into the mdBook guide and regenerate the ADR SUMMARY block.
# Run from the repo root (the docs workflow runs it before `mdbook build`).
# Portable across BSD (macOS) and GNU awk.
set -euo pipefail

ADR_SRC="docs/adr"
GUIDE_SRC="docs/guide/src"
DEST="$GUIDE_SRC/adr"
SUMMARY="$GUIDE_SRC/SUMMARY.md"
MARKER="<!-- adrs -->"

# Files in docs/adr that are not themselves ADRs.
is_adr() {
  case "$(basename "$1")" in
    index.md | README.md | template.md) return 1 ;;
    *) return 0 ;;
  esac
}

mkdir -p "$DEST"
cp "$ADR_SRC"/*.md "$DEST"/

REPO_BLOB="https://github.com/caliban-ai/prospero/blob/main"

# Collapse `.` and `..` segments. sed does the substituting rather than bash
# parameter expansion, whose replacement string treats a backslash literally.
normalize() {
  local p="$1" before
  while :; do
    before="$p"
    p="$(printf '%s' "$p" | sed -E 's#/\./#/#g; s#^\./##')"
    [ "$p" = "$before" ] && break
  done
  while :; do
    before="$p"
    p="$(printf '%s' "$p" | sed -E 's#(^|/)[^/]+/\.\./#\1#')"
    [ "$p" = "$before" ] && break
  done
  printf '%s' "$p"
}

# Is this guide-relative path a page mdBook will actually render? SUMMARY.md is
# the authority: mdBook builds what SUMMARY lists and nothing else.
is_chapter() {
  grep -qF "](./$1)" "$SUMMARY"
}

# Rewrite the links that leave the book (#268).
#
# An ADR is written to sit in docs/adr/, where `README.md` and
# `../superpowers/specs/x.md` both resolve. Copied into the book they do not:
# README.md and template.md are deliberately not chapters (is_adr excludes them)
# and nothing ingests docs/superpowers/, so mdBook rewrites both to pages it never
# built — and warns about neither, because it validates no links at all.
#
# Each target is resolved twice, which is the point. Against the *copied* location
# to decide whether it stays in the book, and against the ADR's *original* home to
# build the URL — an ADR's `../x` means "next to docs/adr", not "next to
# docs/guide/src/adr". A rewrite that got this wrong would turn one 404 into
# another, so a target that does not exist fails the build rather than shipping.
for f in "$DEST"/*.md; do
  # Only real ADRs: README.md is not rendered, and template.md's Source line is a
  # `...` placeholder that is not supposed to resolve.
  is_adr "$f" || continue

  targets="$(
    {
      grep -oE '\]\([^)]+\)' "$f" | sed 's/^](//; s/)$//' || true
      grep -oE '^\[[^]]+\]:[[:space:]]*[^[:space:]]+' "$f" |
        sed -E 's/^\[[^]]*\]:[[:space:]]*//' || true
    } | sort -u
  )"

  for t in $targets; do
    case "$t" in
      http://* | https://* | '#'* | mailto:*) continue ;;
    esac

    base="${t%%#*}"
    anchor="${t#"$base"}"
    [ -n "$base" ] || continue

    is_chapter "$(normalize "adr/$base")" && continue

    orig="$(normalize "$ADR_SRC/$base")"
    if [ ! -e "$orig" ]; then
      echo "error: $(basename "$f") links to '$t', which does not exist" >&2
      echo "       (resolved from $ADR_SRC to '$orig')" >&2
      echo "       A link rewritten to a missing file is still a dead link." >&2
      exit 1
    fi

    # `|` as the delimiter: anchors contain `#`, paths contain `/`.
    tmp="$f.tmp"
    sed "s|](${t})|](${REPO_BLOB}/${orig}${anchor})|g" "$f" >"$tmp"
    mv "$tmp" "$f"
    sed -E "s|^(\[[^]]*\]:[[:space:]]*)${t}[[:space:]]*\$|\1${REPO_BLOB}/${orig}${anchor}|" "$f" >"$tmp"
    mv "$tmp" "$f"
  done
done

# Build an ADR index page from the file titles (first markdown H1 of each file).
{
  echo "# Architecture Decision Records"
  echo
  for f in "$DEST"/*.md; do
    is_adr "$f" || continue
    base="$(basename "$f")"
    title="$(grep -m1 '^# ' "$f" | sed 's/^# //')"
    echo "- [${title:-$base}](./${base})"
  done
} > "$DEST/index.md"

# Build the nested SUMMARY entries (newest mdBook needs every page listed).
entries=""
for f in "$DEST"/*.md; do
  is_adr "$f" || continue
  base="$(basename "$f")"
  title="$(grep -m1 '^# ' "$f" | sed 's/^# //')"
  entries+="  - [${title:-$base}](./adr/${base})"$'\n'
done

# Regenerate everything after the marker: keep the file up to and including the
# marker line, then append the fresh entries. Uses sed (not a multi-line awk -v),
# which is portable across BSD/macOS and GNU awk.
grep -qF -- "$MARKER" "$SUMMARY" || {
  echo "error: ADR marker '$MARKER' not found in $SUMMARY" >&2
  exit 1
}
tmp="$SUMMARY.tmp"
sed "/$MARKER/q" "$SUMMARY" > "$tmp"
printf '%s' "$entries" >> "$tmp"
mv "$tmp" "$SUMMARY"
