# Design: `$scoreFusion` aggregation stage builder (JAVA-5990)

**Ticket:** [JAVA-5990](https://jira.mongodb.org/browse/JAVA-5990), split from
[DRIVERS-3100](https://jira.mongodb.org/browse/DRIVERS-3100) — "Add builder support for `$scoreFusion` stage."

**Scope decision:** `$scoreFusion` only. `$rankFusion`, the `$score` stage
(JAVA-6202), `$sigmoid`, and `$minMaxScaler` are out of scope, but naming is
chosen so those tickets can reuse types (see Naming).

**Reference implementations:** C# [mongodb/mongo-csharp-driver#1976](https://github.com/mongodb/mongo-csharp-driver/pull/1976)
(typed enums, weights/expression validation, 8.2 wire gate); PHP
[mongodb/mongo-php-library#1822](https://github.com/mongodb/mongo-php-library/pull/1822) (thin pass-through).

## Server stage being abstracted

`$scoreFusion` (MongoDB 8.2+, must be the first stage, single collection):

```json
{ "$scoreFusion": {
    "input": {
      "pipelines": { "name1": [ ...stages ], "name2": [ ...stages ] },
      "normalization": "none" | "sigmoid" | "minMaxScaler"
    },
    "combination": {
      "weights": { "name1": 0.3, "name2": 0.7 },
      "method": "avg" | "expression",
      "expression": { "$sum": [ { "$multiply": ["$$name1", 10] }, "$$name2" ] }
    },
    "scoreDetails": true
} }
```

Server rules the API must respect:

- `input.pipelines` (required): map of unique names to sub-pipelines on the same collection; at least one.
- `input.normalization` (required): one of `none`, `sigmoid`, `minMaxScaler`.
- `combination` (optional): `weights` is **mutually exclusive** with `expression`;
  `expression` requires `method: "expression"`; pipeline names bind as `$$name` variables.
- `scoreDetails` (optional, default `false`).

## Public API

All new types live in `com.mongodb.client.model` (driver-core). Javadoc tagged
`@mongodb.server.release 8.2`. Not `@Beta` (stage is GA in 8.2; C# shipped non-beta).

### `Aggregates` entry points

```java
public static Bson scoreFusion(List<FusionPipeline> pipelines, ScoreNormalization normalization)
public static Bson scoreFusion(List<FusionPipeline> pipelines, ScoreNormalization normalization, ScoreFusionOptions options)
```

### `FusionPipeline`

`Facet`-style value class holding a named sub-pipeline: static factories
`of(String name, List<? extends Bson> stages)` and `of(String name, Bson... stages)`,
getters, `equals`/`hashCode`/`toString`. Named without the `Score` prefix so a
future `$rankFusion` builder (identical `input.pipelines` shape) reuses it.

### `ScoreNormalization`

`@Sealed` interface with static factories `none()`, `sigmoid()`, `minMaxScaler()`
rendering the camelCase server strings. Named fusion-agnostically because
JAVA-6202 (`$score` stage) has the same `normalization` field and must reuse this
type — **coordinate with the JAVA-6202 implementer**.

### `ScoreFusionCombination`

`@Sealed` type making the weights-XOR-expression server rule unrepresentable:

- `ScoreFusionCombination.weighted(Bson weights)` → emits `combination.weights`;
  returns a subtype with optional `.method(...)` for `avg` (server default is sum-like behavior when omitted).
- `ScoreFusionCombination.expression(Bson expression)` → always emits
  `method: "expression"` together with `combination.expression`.

### `ScoreFusionOptions`

`@Sealed` immutable fluent interface, static factory `scoreFusionOptions()`:

- `combination(ScoreFusionCombination combination)`
- `scoreDetails(boolean scoreDetails)`
- `option(String name, Object value)` escape hatch (per `SearchOptions`/`VectorSearchOptions` convention).

### Example

```java
Bson stage = Aggregates.scoreFusion(
        asList(
                FusionPipeline.of("vector", vectorSearchStage),
                FusionPipeline.of("text", searchStage, Aggregates.limit(10))),
        ScoreNormalization.sigmoid(),
        scoreFusionOptions()
                .combination(ScoreFusionCombination.weighted(
                        new Document("vector", 0.7).append("text", 0.3)))
                .scoreDetails(true));
```

## Implementation

- Stage rendering via a private static nested `Bson`-implementing class in
  `Aggregates` (pattern: `SetWindowFieldsStage`), or the constructible-BSON
  helpers used by the search package — match surrounding style.
- Sealed interfaces backed by internal immutable implementations, following
  `VectorSearchOptions`.
- Rendering rules: omit `combination` entirely when the options carry none;
  emit `scoreDetails` only when `true` (matches C#).
- Validation (fail-fast `notNull`/`isTrueArgument`): pipelines non-null,
  non-empty, no null entries; names non-null, non-empty, unique; weights/expression
  non-null where required. Server-side rules (stage-must-be-first,
  same-collection) are left to the server per driver convention.

## Wrappers

- **Scala:** type aliases + companion forwarders in
  `org.mongodb.scala.model.package` (pattern: `Facet`) and a `scoreFusion`
  forwarder in Scala `Aggregates`; update Scala API-surface test expectations.
- **Kotlin:** uses Java model types directly — no change.
- Pure API addition — no binary-compatibility impact.

## Testing

Three layers:

1. **Unit** (alongside existing `Aggregates` unit tests): exact rendered BSON
   for minimal form, each normalization value, weights, weights+method,
   expression, scoreDetails, `option(...)` escape hatch; validation failures
   (empty/duplicate/blank names, null args).
2. **Functional end-to-end** (`AggregatesTest`, gated
   `serverVersionAtLeast(8, 2)`): every option exercised against a real server
   using `$match` + raw `$score` BSON sub-pipelines with chosen score values, so
   fused results and scores are deterministic and assertable — including
   `$$name` expression combination and `{$meta: "scoreDetails"}` projection.
   The raw `$score` documents carry a
   `// TODO JAVA-6202 replace raw $score documents with the $score builder once available`
   comment.
3. **Atlas integration** (`AggregatesSearchIntegrationTest`, gated
   `isAtlasSearchTest()`): one realistic hybrid test — one `$search` + one
   `$vectorSearch` sub-pipeline fused with normalization + weights, asserting
   sensible ranked results.

## Out of scope / future work

- `$rankFusion` builder (reuses `FusionPipeline`).
- `$score` stage builder — JAVA-6202 (reuses `ScoreNormalization`).
- `$sigmoid` expression and `$minMaxScaler` window-function builders.
