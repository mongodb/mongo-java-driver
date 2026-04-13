---
name: evergreen
description: Evergreen CI infrastructure, configuration validation. Use when modifying .evergreen/ config, preparing to submit changes or understanding the Evergreen test matrix.
disable-model-invocation: true
allowed-tools: Bash(evergreen *)
---
# Evergreen

## Evergreen (MongoDB Internal CI)

Primary CI runs on MongoDB’s Evergreen system.
Configuration lives in `.evergreen/`.

- Do not modify `.evergreen/` configuration without review
- Evergreen runs the full test matrix across MongoDB versions, OS platforms, and JDK versions

## Validating Evergreen Configuration

After modifying `.evergreen/` files, validate the config locally:

```bash
evergreen validate .evergreen/.evg.yml
```

Always run this before submitting changes to `.evergreen/` to catch syntax errors and invalid task definitions.

## Testing with a Patch Build

To test your changes on Evergreen before merging, create a patch build:

```bash
evergreen patch -u
```

This uploads your uncommitted and committed local changes as a patch build on Evergreen, allowing you to run the full CI
test matrix against your branch.
