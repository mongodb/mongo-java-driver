# AGENTS.md - bom

Bill of Materials (BOM) for MongoDB Java Driver dependency management.

- All dependency versions are managed in `gradle/libs.versions.toml` — never declare versions inline
- This module publishes a BOM POM only — no source code
- Do not add implementation dependencies here
