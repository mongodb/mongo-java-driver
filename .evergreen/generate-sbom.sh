#!/usr/bin/env bash
set -euo pipefail

# Ephemeral SBOM generator (Gradle/Java) using mise + CycloneDX Gradle plugin.
# Environment overrides:
#  MISE_JAVA_VERSION   Java (Temurin) major version (default from Gradle sourceCompatibility or 21)
#  SBOM_OUT            Output filename (default sbom.json)
#
# Usage: bash .evergreen/generate-sbom.sh

## resolve_java_version
# Determines the required Java version by finding the maximum sourceCompatibility in Gradle files.
resolve_java_version() {
  local max_version
  max_version=$(find . -name "*.gradle.kts" -exec grep -h 'sourceCompatibility = JavaVersion.VERSION_' {} \; | sed 's/.*VERSION_\([0-9]*\).*/\1/' | sort -n | tail -1)
  echo "${max_version:-21}"
}

JAVA_VERSION="${MISE_JAVA_VERSION:-$(resolve_java_version)}"
JQ_VERSION="${JQ_VERSION:-latest}" # jq version or 'latest'
OUT_JSON="${SBOM_OUT:-sbom.json}"

log() { printf '\n[sbom] %s\n' "$*"; }

# Ensure mise is available (installed locally in $HOME) and PATH includes shims.

ensure_mise() {
  # Installer places binary in ~/.local/bin/mise by default.
  if ! command -v mise >/dev/null 2>&1; then
    log "Installing mise"
    curl -fsSL https://mise.run | bash >/dev/null 2>&1 || { log "mise install script failed"; exit 1; }
  fi
  # Ensure ~/.local/bin precedes so 'mise' is found even if shims absent.
  export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$HOME/.local/share/mise/bin:$PATH"
  if ! command -v mise >/dev/null 2>&1; then
    log "mise not found on PATH after install"; ls -al "$HOME/.local/bin" || true; exit 1
  fi
}

## resolve_toolchain_flags
# Returns space-separated tool@version specs required for SBOM generation.
resolve_toolchain_flags() {
  printf 'java@temurin-%s jq@%s' "$JAVA_VERSION" "$JQ_VERSION"
}

## prepare_exec_prefix
# Builds the mise exec prefix for ephemeral command runs.
prepare_exec_prefix() {
  local tools
  tools="$(resolve_toolchain_flags)"
  echo "mise exec $tools --"
}

## generate_sbom
# Executes Gradle CycloneDX plugin to generate SBOM.
generate_sbom() {
  log "Generating SBOM using Gradle CycloneDX plugin"
  local exec_prefix
  exec_prefix="$(prepare_exec_prefix)"
  $exec_prefix ./gradlew cyclonedxBom || {
    log "SBOM generation failed"; exit 1; }
  log "SBOM generated"
}

## install_toolchains
# Installs required runtime versions into the local mise cache unconditionally.
# (mise skips download if already present.)
install_toolchains() {
  local tools
  tools="$(resolve_toolchain_flags)"
  log "Installing toolchains: $tools"
  mise install $tools >/dev/null
}

## format_sbom
# Formats the SBOM JSON with jq (required). Exits non-zero if formatting fails.
format_sbom() {
  log "Formatting SBOM via jq@$JQ_VERSION"
  if ! mise exec jq@"$JQ_VERSION" -- jq . "$OUT_JSON" > "$OUT_JSON.tmp" 2>/dev/null; then
    log "jq formatting failed"; return 1
  fi
  mv "$OUT_JSON.tmp" "$OUT_JSON"
}

## ensure_cyclonedx_cli
# Downloads CycloneDX CLI binary if not available.
ensure_cyclonedx_cli() {
  if [ ! -f /tmp/cyclonedx ]; then
    log "Downloading CycloneDX CLI"
    local arch
    arch="$(uname -m)"
    case "$arch" in
      x86_64) arch="x64" ;;
      aarch64) arch="arm64" ;;
      *) log "Unsupported architecture for CycloneDX CLI: $arch"; exit 1 ;;
    esac
    local url="https://github.com/CycloneDX/cyclonedx-cli/releases/latest/download/cyclonedx-linux-${arch}"
    curl -L -s -o /tmp/cyclonedx "$url" || { log "Failed to download CycloneDX CLI"; exit 1; }
    chmod +x /tmp/cyclonedx || { log "Failed to make CycloneDX CLI executable"; exit 1; }
  fi
}

## verify_sbom
# Verifies the SBOM is valid CycloneDX format using CycloneDX CLI.
verify_sbom() {
  log "Verifying SBOM validity with CycloneDX CLI"
  local size
  size=$(stat -c%s "$OUT_JSON" 2>/dev/null || echo 0)
  if [ "$size" -lt 1000 ]; then
    log "SBOM file too small (<1000 bytes)"; exit 1
  fi
  if ! /tmp/cyclonedx validate --input-file "$OUT_JSON" --fail-on-errors >/dev/null 2>&1; then
    log "SBOM validation failed"; exit 1
  fi
  log "SBOM verified successfully"
}

main() {
  ensure_mise
  install_toolchains
  generate_sbom
  format_sbom
  ensure_cyclonedx_cli
  verify_sbom
}

main "$@"