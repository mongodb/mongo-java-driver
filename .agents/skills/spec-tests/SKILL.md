---
name: spec-tests
description: How to work with MongoDB specification tests — structure, rules, and adding new spec test support. Use when implementing or modifying behavior defined by the MongoDB Driver Specifications, or when working with the test data in testing/resources/specifications/.
---
# MongoDB Specification Tests

## Overview

The driver implements the [MongoDB Driver Specifications](https://github.com/mongodb/specifications).
Specification test data files live in `testing/resources/specifications/` — a git submodule.

## Rules

- **Do not modify existing specification tests** unless they test incorrect behavior
- **Do not modify spec test data** — it is managed upstream
- Create new tests instead of modifying existing ones
- Update the submodule via `git submodule update`

## Structure

Spec test data is organized by specification area:

- CRUD, SDAM, auth, CSFLE, retryable operations, and more
- Each spec area has JSON/YAML test data files defining inputs and expected outputs
- Driver test runners parse these files and execute against the driver

## Test Data Location

```
testing/
  resources/
    specifications/    # Git submodule — do not edit directly
    logback-test.xml   # Shared logback configuration for tests
```

## Adding Spec Test Support

1. Check `testing/resources/specifications/` for the relevant spec test data
2. Find existing test runners in the module (look for `*SpecificationTest*` or similar)
3. Extend existing patterns — each module handles spec tests slightly differently
4. Ensure tests run with `./gradlew check` or the module’s test task
