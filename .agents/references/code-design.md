---
name: code-design
description: Design rules for implementation code in the MongoDB Java Driver. Use when adding or modifying an internal program element, or changing the implementation of a public API program element.
---
# Code Design

## Access Modifiers

- Use the most restrictive access modifier that is sufficient for now.
- If there is an internal program element suitable for the task but not accessible, relax its access modifier
  to the least permissive one that makes it accessible,
  unless it contradicts the intent expressed in the documentation of the program element in question.
  Be careful not to make the program element part of the public API accidentally.
- When access is relaxed only for tests, annotate the program element with `VisibleForTesting`.
    

## Executors

- Avoid instantiating new executors/threads. Consider using the existing `CommonExecutor` or `AsyncClientExecutor`.
- If a new executor/thread is unavoidable, prefer instantiating a new executor with a single thread over a bare thread.
  It should be created and managed either by `CommonExecutor`, `AsyncClientExecutor`,
  or a class whose instance is accessible via them. This may require changing their design, implementation, documentation. 
- When instantiating an executor, prefer the `MongoThreadPoolExecutor` and `MongoScheduledThreadPoolExecutor` implementations.
- Use daemon threads, see `DaemonThreadFactory`.
- Any task executed, submitted, or scheduled via an executor must not allow an `Exception` to be propagated;
  `Error`s should generally not be caught, but if they are, they must still be propagated.
