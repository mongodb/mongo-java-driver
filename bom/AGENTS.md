# AGENTS.md - bom

Bill of Materials (BOM) POM for the MongoDB Java Driver, simplifying dependency version
management.

**Depends on:** All published driver modules (transitive version constraints only)

## Notes

- Java Platform plugin — no source code, only version constraints
- Automatically includes all supported Scala version variants for `bson-scala` and
  `driver-scala`
- Validates generated POM: all dependencies must be `org.mongodb`, no `<scope>` or
  `<optional>` elements
- Do not add non-MongoDB dependencies to this module
