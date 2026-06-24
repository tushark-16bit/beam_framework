#!/bin/bash
# Fires on every Claude Stop event.
# If Java or config files were modified in the working tree or index since the
# last commit, checks that documentation files were also modified.
# Exits 0 always (non-blocking) but prints a visible warning when docs lag code.

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null) || exit 0
cd "$REPO" || exit 0

# Files changed (staged + unstaged) vs HEAD
CHANGED=$(git diff HEAD --name-only 2>/dev/null; git diff --name-only 2>/dev/null)

CODE_CHANGED=$(echo "$CHANGED" | grep -E '\.(java|xml|json|yaml|yml)$' | grep -v '\.claude/' | head -1)
DOC_CHANGED=$(echo "$CHANGED"  | grep -E '(CLAUDE\.md|README\.md|WALKTHROUGH\.md|EXAMPLE\.md)' | head -1)

if [ -n "$CODE_CHANGED" ] && [ -z "$DOC_CHANGED" ]; then
  echo ""
  echo "╔══════════════════════════════════════════════════════════════════╗" >&2
  echo "║  ⚠️  DOC DRIFT DETECTED                                          ║" >&2
  echo "║  Code files changed but no documentation updated this session.  ║" >&2
  echo "║                                                                  ║" >&2
  echo "║  Update before committing:                                       ║" >&2
  echo "║    • CLAUDE.md  — file map, execution paths, invariants         ║" >&2
  echo "║    • beam-*/README.md — whichever module(s) changed             ║" >&2
  echo "║    • WALKTHROUGH.md  — if architecture or flow changed          ║" >&2
  echo "╚══════════════════════════════════════════════════════════════════╝" >&2
  echo ""
fi

exit 0
