#!/usr/bin/env bash
# Resolve every internal link in the built guide against the files on disk.
# Run from the repo root, after `mdbook build` (the docs workflow does both).
#
# mdBook validates no links at all — it rewrites `](x.md)` to `href="x.html"` and
# emits no warning when the target does not exist. So "the guide builds clean" was
# never evidence that its links resolve, which is how #268's dead links survived a
# docs-accuracy pass. This script is that missing evidence.
#
# Two kinds of link are checked:
#
#   * relative hrefs, resolved against the page that contains them and looked up
#     in the built output;
#   * absolute links back into this repository, resolved against the working tree.
#     Those exist because sync-adrs.sh rewrites links that leave the book into
#     absolute repo URLs — which converts a checked relative link into an
#     unchecked absolute one. Skipping them would bless exactly the bad rewrite
#     the rewriting introduces.
#
# Third-party URLs are deliberately NOT fetched: the docs build must not depend on
# someone else's availability, and a flaky external host failing the build teaches
# people to ignore it.
set -euo pipefail

BOOK="${1:-docs/guide/book}"
REPO_SLUG="caliban-ai/prospero"

[ -d "$BOOK" ] || {
  echo "error: no built book at $BOOK (run 'mdbook build docs/guide' first)" >&2
  exit 2
}

# Collapse `.` and `..` segments. Hand-rolled rather than `realpath -m`, which is
# GNU-only, and without bash-4 array features so this also runs on macOS bash 3.2.
normalize() {
  local p="$1" before
  # Both passes loop, because a replacement can create a new match: `a/././b`
  # needs two rounds, and `a/b/../../c` needs two of the second kind. sed does
  # the substituting rather than bash parameter expansion, whose replacement
  # string treats a backslash literally — `${p//\/.\//\/}` silently produces
  # `book\/page.html` and every link then reads as broken.
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

checked=0
broken=0

# `book/api` is rustdoc, assembled by docs.yml from `cargo doc` AFTER mdbook runs.
# In the workflow the check runs after that step, so it is present and links into
# it are verified. A local `mdbook build` alone has no rustdoc, and failing the
# whole check for that would make it useless locally — which is how a check stops
# being run at all.
skip_api=0
if [ ! -d "$BOOK/api" ]; then
  skip_api=1
  echo "note: no $BOOK/api (rustdoc not assembled); links into it are not checked"
fi

# `book/api` is rustdoc output, assembled by the workflow from `cargo doc`. It is
# generated, enormous, and not ours to validate.
while IFS= read -r page; do
  page_dir="$(dirname "$page")"

  # One href per line. `grep -o` keeps this to the attribute itself.
  while IFS= read -r href; do
    [ -n "$href" ] || continue

    case "$href" in
      '#'* | mailto:* | javascript:* | data:*) continue ;;
      "https://github.com/$REPO_SLUG/blob/main/"* | "https://github.com/$REPO_SLUG/tree/main/"*)
        # Self-referencing: resolve against the working tree.
        target="${href#https://github.com/$REPO_SLUG/}"
        target="${target#blob/main/}"
        target="${target#tree/main/}"
        target="${target%%#*}"
        checked=$((checked + 1))
        if [ ! -e "$target" ]; then
          echo "BROKEN  $page"
          echo "        -> $href"
          echo "        (absolute self-link; no $target in the working tree)"
          broken=$((broken + 1))
        fi
        continue
        ;;
      http://* | https://* | //*) continue ;;
    esac

    # Relative link: strip anchor and query, then resolve against the page.
    target="${href%%#*}"
    target="${target%%\?*}"
    [ -n "$target" ] || continue

    case "$target" in
      /*) resolved="$BOOK$target" ;;
      *) resolved="$(normalize "$page_dir/$target")" ;;
    esac

    if [ "$skip_api" -eq 1 ]; then
      case "$resolved" in "$BOOK/api/"*) continue ;; esac
    fi

    checked=$((checked + 1))
    # A directory link resolves through its index.html, as a web server would.
    if [ ! -e "$resolved" ] && [ ! -e "$resolved/index.html" ]; then
      echo "BROKEN  $page"
      echo "        -> $href"
      echo "        (resolved to $resolved)"
      broken=$((broken + 1))
    fi
  done < <(grep -o 'href="[^"]*"' "$page" | sed 's/^href="//; s/"$//')
done < <(find "$BOOK" -name '*.html' -not -path "$BOOK/api/*" | sort)

echo "check-links: $checked links checked in $BOOK, $broken broken"
[ "$broken" -eq 0 ] || {
  echo "error: the guide has broken links; mdBook does not catch these" >&2
  exit 1
}
