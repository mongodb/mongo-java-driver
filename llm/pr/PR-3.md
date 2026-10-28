# PR-3: authenticators — first generated async code

Notes for reviewers; see commit messages for per-file detail.

## What this PR does
- The generator marks the sync authenticator methods `@HasAsync` (its model
  walks from the leaf pairs to their callers); their async twins are
  now GENERATED from the sync body (annotated `@GeneratedAsync`) and take the
  real `*Async` name. The previous hand-written body is kept verbatim as
  `*AsyncDefunct`, annotated `@DeadAsync`, for side-by-side review. All
  `@DeadAsync` code is deleted in one final cleanup PR at the end of the sequence.
- Generated bodies do not wrap the callback in `errorHandlingCallback`; that
  wrap now lives at the 4 `@CallbackBoundary` crossings (PR-2).
- The generator would live in a separate repo; its output is reviewed here like
  any other code. Regenerating a file is byte-idempotent.

## Pure refactoring commits
- The SaslAuthenticator conversation-helper extraction, the later split of
  `runSaslConversation`, and the final rename of that extraction's leftover identifiers are all
  `PureRefactor:` commits made by tools/pure-refactor/refactor-ops: each carries its exact operations in the
  commit message and is checked by `refactor-ops validate <sha>`, which replays them from the
  parent and requires an identical tree. Tiny hand commits before and after each hold the prep and
  residue that aren't refactoring.

## Decisions
- Generated methods take the real `*Async` name; the hand-written twin becomes
  `*AsyncDefunct` + `@DeadAsync`, loses `@Override`, body untouched.
- `Authenticator.authenticate`/`authenticateAsync` are a leaf pair
  (`@SyncLeaf`/`@AsyncLeaf`), so the generator reaches the X509, Default and
  Oidc overrides from there. `SaslAuthenticator.authenticate` is its own leaf
  pair (PR-2): its async form runs under `doAsSubject`, which can't come from
  the sync body.
- `SaslAuthenticator.authenticateAsync`'s `doAsSubject` body is extracted into
  `authenticateUnderSubjectAsync`, mirroring the sync side, so the conversation
  is reachable for generation.

## Alleged bugs in the replaced hand-written async code
Marked in the `*AsyncDefunct` bodies as `// bug:` comments, at the line concerned.
