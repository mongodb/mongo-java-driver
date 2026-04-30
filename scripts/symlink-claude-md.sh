#!/usr/bin/env bash
# 1. Ensure every staged AGENTS.md has a companion CLAUDE.md containing
#    "@AGENTS.md" (a Claude Code import reference).
# 2. Ensure .claude/skills is a symlink to .agents/skills.

set -euo pipefail

claude_file_for_agents() {
  local agents_file="$1"
  local dir

  dir=$(dirname "$agents_file")

  if [ "$dir" = "." ]; then
    printf 'CLAUDE.md\n'
  else
    printf '%s/CLAUDE.md\n' "$dir"
  fi
}

remove_generated_file() {
  local generated_file="$1"

  if git ls-files --error-unmatch -- "$generated_file" > /dev/null 2>&1 || [ -e "$generated_file" ]; then
    rm -f "$generated_file"
    git add -u -- "$generated_file"
    echo "auto-removed: $generated_file"
  fi
}

sync_claude_skills_dir() {
  local claude_skills_dir=".claude/skills"
  local expected_target="../.agents/skills"
  local current_target

  if [ -L "$claude_skills_dir" ]; then
    current_target=$(readlink "$claude_skills_dir")
    if [ "$current_target" = "$expected_target" ]; then
      return
    fi

    rm -f "$claude_skills_dir"
  elif [ -e "$claude_skills_dir" ]; then
    rm -rf "$claude_skills_dir"
  fi

  mkdir -p .claude
  ln -s "$expected_target" "$claude_skills_dir"
  git add "$claude_skills_dir"
  echo "auto-synced: $claude_skills_dir -> $expected_target"
}

sync_claude_skills_dir

staged_agents=()
deleted_agents=()

while IFS=$'\t ' read -r status first_path second_path; do
  [ -n "$status" ] || continue

  case "$status" in
    R*)
      if [[ "$first_path" =~ (^|/)AGENTS\.md$ ]]; then
        deleted_agents+=("$first_path")
      fi
      if [[ "$second_path" =~ (^|/)AGENTS\.md$ ]]; then
        staged_agents+=("$second_path")
      fi
      ;;
    D)
      if [[ "$first_path" =~ (^|/)AGENTS\.md$ ]]; then
        deleted_agents+=("$first_path")
      fi
      ;;
    *)
      if [[ "$first_path" =~ (^|/)AGENTS\.md$ ]]; then
        staged_agents+=("$first_path")
      fi
      ;;
  esac
done < <(git diff --cached --name-status --find-renames --diff-filter=ADMR)

# --- CLAUDE.md sync ---
if [ "${#staged_agents[@]}" -gt 0 ]; then
  for agents_file in "${staged_agents[@]}"; do
    claude_file=$(claude_file_for_agents "$agents_file")

    # Skip if already a regular file with the correct content
    if [ -f "$claude_file" ] && ! [ -L "$claude_file" ] && [ "$(cat "$claude_file")" = "@AGENTS.md" ]; then
      continue
    fi

    # Remove symlink if present, then write the reference file
    rm -f "$claude_file"
    printf '@AGENTS.md\n' > "$claude_file"
    git add "$claude_file"
    echo "auto-created: $claude_file with @AGENTS.md reference"
  done
fi

if [ "${#deleted_agents[@]}" -gt 0 ]; then
  for agents_file in "${deleted_agents[@]}"; do
    remove_generated_file "$(claude_file_for_agents "$agents_file")"
  done
fi
