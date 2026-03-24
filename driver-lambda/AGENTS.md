# AGENTS.md - driver-lambda

AWS Lambda test application for validating the MongoDB Java Driver in Lambda
environments.

**Depends on:** `bson`, `driver-sync`

## Key Packages

- `com.mongodb.lambdatest` — Lambda test application entry point

## Notes

- Not published — internal testing only
- Java 11 source/target compatibility
- Built as a shadow JAR for Lambda deployment
- Non-standard source layout: `src/main` (no `java` subdirectory)
