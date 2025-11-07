#!/usr/bin/env bash
set -euo pipefail
# verify-sbom.sh: Enforces policy that when dependency manifest files change, the SBOM file must also change.
# Generic so patterns can be repurposed for other artifact freshness checks.
#
# Configurable variables (override via env):
#  MANIFEST_PATTERNS    Space-delimited gitignore-style patterns to monitor.
#                       Default: gradle/libs.versions.toml gradle.properties settings.gradle.kts **/build.gradle.kts
#  SBOM_FILE            Path to canonical SBOM file (default sbom.cdx.json)
#  EXIT_CODE_MISSING    Exit code when manifests changed but SBOM did not (default 10)
#  SKIP_SBOM_VERIFY     If set to 1, skip verification (default unset)
#  DIFF_BASE            Explicit git ref/commit to diff against (optional). If unset, merge-base with origin/main or HEAD~1 fallback.
#  VERBOSE              If set to 1, prints expanded manifest list and full diff file list.
#
# Behavior:
# 1. Determine diff range (patch vs mainline) by attempting merge-base with origin/main.
# 2. Capture changed files including both committed changes and staged/uncommitted (by combining git diff --name-status for range and working tree).
# 3. Expand manifest patterns via git ls-files.
# 4. If any manifest changed but SBOM_FILE not changed, fail with guidance.
# 5. If SBOM_FILE changed, pass.
# 6. If no manifest changed, pass.
#
# Local usage: run from repo root after making changes; exit codes surface policy status.

MANIFEST_PATTERNS="${MANIFEST_PATTERNS:-build.gradle.kts gradle/libs.versions.toml gradle.properties settings.gradle.kts **/build.gradle.kts}"
SBOM_FILE="${SBOM_FILE:-sbom.json}"
EXIT_CODE_MISSING="${EXIT_CODE_MISSING:-10}"
DIFF_BASE="${DIFF_BASE:-}" # optional user-provided base ref
VERBOSE="${VERBOSE:-0}"

log() { printf '\n[verify-sbom] %s\n' "$*"; }

if [[ "${SKIP_SBOM_VERIFY:-}" == "1" ]]; then
  log "Skipping verification (SKIP_SBOM_VERIFY=1)"; exit 0; fi

# Determine base for diff
if [[ -n "$DIFF_BASE" ]]; then
  base_ref="$DIFF_BASE"
else
  git fetch origin main >/dev/null 2>&1 || true
  base_ref="$(git merge-base HEAD origin/main 2>/dev/null || echo '')"
  if [[ -z "$base_ref" ]]; then
    # Fallback to previous commit
    base_ref="HEAD~1"
  fi
fi

range="$base_ref..HEAD"
log "Using diff range: $range"

# Committed diff
committed_status="$(git diff --name-status $range || true)"
# Working tree (staged + unstaged) diff vs HEAD
wt_status="$(git diff --name-status HEAD || true)"
# Combine and extract filenames (second column); preserve uniqueness
changed_files=$(printf "%s\n%s\n" "$committed_status" "$wt_status" | awk '{print $2}' | grep -v '^$' | sort -u)

if [[ $VERBOSE == 1 ]]; then
  log "Changed files:"; echo "$changed_files"
fi

# Expand manifest patterns to tracked files
expanded_manifests=""
for pattern in $MANIFEST_PATTERNS; do
  # git ls-files supports pathspec; '**' requires extglob-like; rely on grep fallback
  matches=$(git ls-files "$pattern" 2>/dev/null || true)
  if [[ -z "$matches" && "$pattern" == *"**"* ]]; then
    # Manual glob expansion for recursive pattern
    matches=$(git ls-files | grep -E "$(echo "$pattern" | sed 's/**/.*'/ | sed 's/\./\\./g')" || true)
  fi
  expanded_manifests+="$matches\n"
done
expanded_manifests=$(echo -e "$expanded_manifests" | grep -v '^$' | sort -u)

if [[ $VERBOSE == 1 ]]; then
  log "Expanded manifests:"; echo "$expanded_manifests"
fi

manifest_hit=0
manifest_changed_list=()
while IFS= read -r mf; do
  if echo "$changed_files" | grep -Fxq "$mf"; then
    manifest_hit=1
    manifest_changed_list+=("$mf")
  fi
done <<< "$expanded_manifests"

if [[ $manifest_hit -eq 0 ]]; then
  log "No manifest changes detected; passing."
  exit 0
fi

if echo "$changed_files" | grep -Fxq "$SBOM_FILE"; then
  log "SBOM file '$SBOM_FILE' updated alongside manifest changes; pass."
  exit 0
fi

log "FAILURE: Manifest files changed but SBOM '$SBOM_FILE' was not modified." >&2
log "Changed manifest(s):" >&2
for mf in "${manifest_changed_list[@]}"; do echo " - $mf" >&2; done
log "Regenerate SBOM locally (e.g., bash .evergreen/generate-sbom.sh) and commit '$SBOM_FILE'." >&2
exit "$EXIT_CODE_MISSING"
