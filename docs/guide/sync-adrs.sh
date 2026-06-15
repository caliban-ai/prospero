#!/usr/bin/env bash
# Copy docs/adr/*.md into the mdBook guide and regenerate the ADR SUMMARY block.
# Run from the repo root (the docs workflow runs it before `mdbook build`).
set -euo pipefail

ADR_SRC="docs/adr"
GUIDE_SRC="docs/guide/src"
DEST="$GUIDE_SRC/adr"
SUMMARY="$GUIDE_SRC/SUMMARY.md"

mkdir -p "$DEST"
cp "$ADR_SRC"/*.md "$DEST"/

# Build an ADR index page from the file titles (first markdown H1 of each file).
{
  echo "# Architecture Decision Records"
  echo
  for f in $(ls "$DEST"/*.md | sort); do
    base="$(basename "$f")"
    [[ "$base" == "index.md" ]] && continue
    title="$(grep -m1 '^# ' "$f" | sed 's/^# //')"
    echo "- [${title:-$base}](./${base})"
  done
} > "$DEST/index.md"

# Regenerate the nested SUMMARY block between the marker and EOF.
entries=""
for f in $(ls "$DEST"/*.md | sort); do
  base="$(basename "$f")"
  [[ "$base" == "index.md" ]] && continue
  title="$(grep -m1 '^# ' "$f" | sed 's/^# //')"
  entries+="  - [${title:-$base}](./adr/${base})"$'\n'
done

# Replace everything after the <!-- adrs --> marker with the fresh entries.
awk -v entries="$entries" '
  /<!-- adrs -->/ { print; printf "%s", entries; skip=1; next }
  skip { next }
  { print }
' "$SUMMARY" > "$SUMMARY.tmp"
mv "$SUMMARY.tmp" "$SUMMARY"
