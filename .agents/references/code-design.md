---
name: code-design
description: Design rules for implementation code in the MongoDB Java Driver. Use when adding or modifying an internal program element, or changing the implementation of a public API program element.
---
# Code Design

## Executors

- When instantiating an executor, prefer the `MongoThreadPoolExecutor` and `MongoScheduledThreadPoolExecutor` implementations.
- Any task executed, submitted, or scheduled via an executor must not allow an `Exception` to be propagated;
  `Error`s should generally not be caught, but if they are, they must still be propagated.
