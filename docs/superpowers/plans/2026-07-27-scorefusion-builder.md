# `$scoreFusion` Aggregation Stage Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add builder support for the `$scoreFusion` aggregation stage (JAVA-5990) to `com.mongodb.client.model`, with Scala wrapper forwarders.

**Architecture:** Four new public types in `com.mongodb.client.model` — `FusionPipeline` (named sub-pipeline, `Facet`-style), `ScoreNormalization` (`@Sealed` interface + package-private `ScoreNormalizationBson`, modeled on `QuantileMethod`/`QuantileMethodBson`), `ScoreFusionCombination`/`WeightedScoreFusionCombination` and `ScoreFusionOptions` (`@Sealed` interfaces backed by one package-private `ScoreFusionConstructibleBson extends AbstractConstructibleBson`, modeled on `GeoNearConstructibleBson`/`SearchConstructibleBson`) — plus two `Aggregates.scoreFusion(...)` overloads rendering via a private nested `ScoreFusionStage implements Bson`.

**Tech Stack:** Java 8 source, Gradle, Groovy/Spock unit tests (`AggregatesSpecification.groovy`), JUnit 5 functional tests (`AggregatesTest`), Atlas-gated JUnit 5 test (`AggregatesSearchIntegrationTest`), Scala forwarders.

**Spec:** `docs/superpowers/specs/2026-07-27-scorefusion-builder-design.md`

## Global Constraints

- Java 8 language level only (no `var`, `List.of`, `Stream.toList`, etc.).
- Every new file starts with the Apache license header containing `Copyright 2008-present MongoDB, Inc.` (copy the exact 15-line header from `driver-core/src/main/com/mongodb/client/model/Facet.java`).
- New public API Javadoc: `@since 5.10`, `@mongodb.server.release 8.2`. Not `@Beta`.
- Package `com.mongodb.client.model` is NOT `@NonNullApi` — do not rely on it; use explicit `notNull(...)` runtime checks (`com.mongodb.assertions.Assertions`).
- Do not reformat code outside your changes. Run `./gradlew :driver-core:spotlessApply` if formatting complains (spotless does not cover Java here, but checkstyle does — match surrounding style, 4-space indent, `final` parameters).
- All work on branch `nh/hybrid_search`. Commit after each task.
- Working directory for all commands: `/Users/nabil.hachicha/MongoDB/mongo-java-driver/nabil_wt`.
- Unit test runs: `./gradlew :driver-core:test --tests "AggregatesSpecification"` (Spock). Functional tests need a running MongoDB: `./gradlew :driver-core:test --tests "com.mongodb.client.model.AggregatesTest" -Dorg.mongodb.test.uri="mongodb://localhost:27017"` — if no server is available, compile only (`./gradlew :driver-core:compileTestJava`) and say so; never claim tests passed without running them.

---

### Task 1: `ScoreNormalization`

**Files:**
- Create: `driver-core/src/main/com/mongodb/client/model/ScoreNormalization.java`
- Create: `driver-core/src/main/com/mongodb/client/model/ScoreNormalizationBson.java`
- Test: `driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy`

**Interfaces:**
- Consumes: nothing new.
- Produces: `public interface ScoreNormalization` with `static ScoreNormalization none()`, `static ScoreNormalization sigmoid()`, `static ScoreNormalization minMaxScaler()`, `static ScoreNormalization of(BsonValue)`, and instance method `BsonValue toBsonValue()`. Task 4's stage rendering calls `toBsonValue()`.

- [ ] **Step 1: Write the failing test.** Add to `AggregatesSpecification.groovy` (inside the class, near the other tests; add `import com.mongodb.client.model.ScoreNormalization` is unnecessary — same package):

```groovy
def 'should create ScoreNormalization'() {
    expect:
    ScoreNormalization.none().toBsonValue() == new BsonString('none')
    ScoreNormalization.sigmoid().toBsonValue() == new BsonString('sigmoid')
    ScoreNormalization.minMaxScaler().toBsonValue() == new BsonString('minMaxScaler')
    ScoreNormalization.of(new BsonString('sigmoid')).toBsonValue() == new BsonString('sigmoid')
}
```

Add `import org.bson.BsonString` to the spec's imports if not present.

- [ ] **Step 2: Run test to verify it fails.**

Run: `./gradlew :driver-core:test --tests "AggregatesSpecification"`
Expected: compilation failure — `unable to resolve class ScoreNormalization`.

- [ ] **Step 3: Implement.** `ScoreNormalization.java` (license header first, as in every new file):

```java
package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;
import org.bson.BsonString;
import org.bson.BsonValue;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The way in which the scores produced by the {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization,
 * ScoreFusionOptions) $scoreFusion} input pipelines are normalized before being combined.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreNormalization {
    /**
     * Returns a {@link ScoreNormalization} instance representing no normalization.
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization none() {
        return new ScoreNormalizationBson(new BsonString("none"));
    }

    /**
     * Returns a {@link ScoreNormalization} instance representing normalization via the sigmoid function.
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization sigmoid() {
        return new ScoreNormalizationBson(new BsonString("sigmoid"));
    }

    /**
     * Returns a {@link ScoreNormalization} instance representing min-max scaling of the scores to the range [0, 1].
     *
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization minMaxScaler() {
        return new ScoreNormalizationBson(new BsonString("minMaxScaler"));
    }

    /**
     * Creates a {@link ScoreNormalization} from a {@link BsonValue} in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     *
     * @param normalization A {@link BsonValue} representing the required {@link ScoreNormalization}.
     * @return The requested {@link ScoreNormalization}.
     */
    static ScoreNormalization of(final BsonValue normalization) {
        return new ScoreNormalizationBson(notNull("normalization", normalization));
    }

    /**
     * Converts this object to {@link BsonValue}.
     *
     * @return A {@link BsonValue} representing this {@link ScoreNormalization}.
     */
    BsonValue toBsonValue();
}
```

`ScoreNormalizationBson.java` — copy the structure of `QuantileMethodBson.java` (same package): a package-private `final class ScoreNormalizationBson implements ScoreNormalization` holding a `private final BsonValue normalization`, constructor assigning it, `toBsonValue()` returning it, plus `equals`/`hashCode` delegating to the field and `toString()` returning `"ScoreNormalization{normalization=" + normalization + '}'`.

- [ ] **Step 4: Run test to verify it passes.**

Run: `./gradlew :driver-core:test --tests "AggregatesSpecification"`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add driver-core/src/main/com/mongodb/client/model/ScoreNormalization.java driver-core/src/main/com/mongodb/client/model/ScoreNormalizationBson.java driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy
git commit -m "JAVA-5990 Add ScoreNormalization for the \$scoreFusion stage"
```

---

### Task 2: `FusionPipeline`

**Files:**
- Create: `driver-core/src/main/com/mongodb/client/model/FusionPipeline.java`
- Test: `driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy`

**Interfaces:**
- Produces: `public final class FusionPipeline` with `static FusionPipeline of(String name, List<? extends Bson> pipeline)`, `static FusionPipeline of(String name, Bson... pipeline)`, `String getName()`, `List<? extends Bson> getPipeline()`. Task 4 consumes both getters. Name deliberately fusion-generic so a future `$rankFusion` builder reuses it.

- [ ] **Step 1: Write the failing test.**

```groovy
def 'should create FusionPipeline'() {
    when:
    def pipeline = FusionPipeline.of('p1', match(eq('x', 1)), limit(2))

    then:
    pipeline.name == 'p1'
    pipeline.pipeline.size() == 2
    pipeline == FusionPipeline.of('p1', [match(eq('x', 1)), limit(2)])

    when:
    FusionPipeline.of('', match(eq('x', 1)))

    then:
    thrown(IllegalArgumentException)

    when:
    FusionPipeline.of('p1', [])

    then:
    thrown(IllegalArgumentException)
}
```

(`match`, `limit`, `eq` are already statically imported in this spec.)

- [ ] **Step 2: Run test to verify it fails.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — compilation failure on `FusionPipeline`.

- [ ] **Step 3: Implement.** `FusionPipeline.java` — model on `Facet.java` but final with static factories:

```java
package com.mongodb.client.model;

import org.bson.conversions.Bson;

import java.util.List;
import java.util.Objects;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * A named aggregation pipeline used as an input to a fusion pipeline stage, e.g.,
 * {@link Aggregates#scoreFusion(List, ScoreNormalization, ScoreFusionOptions) $scoreFusion}.
 * The name uniquely identifies the pipeline within the stage and may be referred to
 * by other parts of the stage, e.g., as the {@code "$$name"} variable in a
 * {@linkplain ScoreFusionCombination#expression(Bson) combination expression}.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
public final class FusionPipeline {
    private final String name;
    private final List<? extends Bson> pipeline;

    /**
     * Creates a new {@link FusionPipeline}.
     *
     * @param name The non-empty pipeline name, unique within the containing stage.
     * @param pipeline The non-empty pipeline.
     * @return The requested {@link FusionPipeline}.
     */
    public static FusionPipeline of(final String name, final List<? extends Bson> pipeline) {
        return new FusionPipeline(name, pipeline);
    }

    /**
     * Creates a new {@link FusionPipeline}.
     *
     * @param name The non-empty pipeline name, unique within the containing stage.
     * @param pipeline The non-empty pipeline.
     * @return The requested {@link FusionPipeline}.
     */
    public static FusionPipeline of(final String name, final Bson... pipeline) {
        return new FusionPipeline(name, asList(pipeline));
    }

    private FusionPipeline(final String name, final List<? extends Bson> pipeline) {
        notNull("name", name);
        isTrueArgument("name must not be empty", !name.isEmpty());
        notNull("pipeline", pipeline);
        isTrueArgument("pipeline must not be empty", !pipeline.isEmpty());
        for (Bson stage : pipeline) {
            notNull("stage", stage);
        }
        this.name = name;
        this.pipeline = unmodifiableList(pipeline);
    }

    /**
     * @return the pipeline name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the pipeline
     */
    public List<? extends Bson> getPipeline() {
        return pipeline;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FusionPipeline that = (FusionPipeline) o;
        return name.equals(that.name) && pipeline.equals(that.pipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, pipeline);
    }

    @Override
    public String toString() {
        return "FusionPipeline{"
                + "name='" + name + '\''
                + ", pipeline=" + pipeline
                + '}';
    }
}
```

Note: `unmodifiableList(pipeline)` needs an unchecked cast to `List<? extends Bson>` — implement as `this.pipeline = unmodifiableList(new java.util.ArrayList<Bson>(pipeline));` typed `List<Bson>` internally (defensive copy; adjust the field type to `List<Bson>` and the getter return type stays `List<? extends Bson>`).

- [ ] **Step 4: Run test to verify it passes.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — PASS.

- [ ] **Step 5: Commit.**

```bash
git add driver-core/src/main/com/mongodb/client/model/FusionPipeline.java driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy
git commit -m "JAVA-5990 Add FusionPipeline for fusion pipeline stages"
```

---

### Task 3: `ScoreFusionCombination`, `WeightedScoreFusionCombination`, `ScoreFusionOptions`

**Files:**
- Create: `driver-core/src/main/com/mongodb/client/model/ScoreFusionCombination.java`
- Create: `driver-core/src/main/com/mongodb/client/model/WeightedScoreFusionCombination.java`
- Create: `driver-core/src/main/com/mongodb/client/model/ScoreFusionOptions.java`
- Create: `driver-core/src/main/com/mongodb/client/model/ScoreFusionConstructibleBson.java`
- Test: `driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy`

**Interfaces:**
- Consumes: nothing from earlier tasks (independent of Tasks 1-2).
- Produces (Task 4 consumes `ScoreFusionOptions` as a `Bson` whose document holds the optional top-level `combination`/`scoreDetails` fields):
  - `interface ScoreFusionCombination extends Bson` with `static WeightedScoreFusionCombination weighted(Bson weights)` and `static ScoreFusionCombination expression(Bson expression)`.
  - `interface WeightedScoreFusionCombination extends ScoreFusionCombination` with `WeightedScoreFusionCombination avg()` (realizes the spec's `.method(avg)`; emits `method: "avg"` — no extra public enum needed since `expression(...)` implies `method: "expression"` automatically and the server default applies when unset).
  - `interface ScoreFusionOptions extends Bson` with `static ScoreFusionOptions scoreFusionOptions()`, `ScoreFusionOptions combination(ScoreFusionCombination combination)`, `ScoreFusionOptions scoreDetails(boolean scoreDetails)`, `ScoreFusionOptions option(String name, Object value)`.

- [ ] **Step 1: Write the failing test.**

```groovy
def 'should render ScoreFusionCombination and ScoreFusionOptions'() {
    expect:
    toBson(ScoreFusionCombination.weighted(new Document('p1', 0.3d).append('p2', 0.7d))) ==
            parse('{weights: {p1: 0.3, p2: 0.7}}')
    toBson(ScoreFusionCombination.weighted(new Document('p1', 0.3d)).avg()) ==
            parse('{weights: {p1: 0.3}, method: "avg"}')
    toBson(ScoreFusionCombination.expression(new Document('$sum', ['$$p1', '$$p2']))) ==
            parse('{method: "expression", expression: {$sum: ["$$p1", "$$p2"]}}')
    toBson(scoreFusionOptions()) == parse('{}')
    toBson(scoreFusionOptions()
            .combination(ScoreFusionCombination.expression(new Document('$sum', ['$$p1', '$$p2'])))
            .scoreDetails(true)) ==
            parse('{combination: {method: "expression", expression: {$sum: ["$$p1", "$$p2"]}}, scoreDetails: true}')
    toBson(scoreFusionOptions().option('scoreDetails', true)) == parse('{scoreDetails: true}')
}
```

Add static import `com.mongodb.client.model.ScoreFusionOptions.scoreFusionOptions` and (if missing) `org.bson.Document` import to the spec. `toBson`/`parse` already exist in the spec.

- [ ] **Step 2: Run test to verify it fails.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — compilation failure.

- [ ] **Step 3: Implement.** One backing class implements all three interfaces (pattern: `SearchConstructibleBson` implementing many interfaces; base pattern: `GeoNearConstructibleBson`).

`ScoreFusionConstructibleBson.java` (package-private):

```java
package com.mongodb.client.model;

import com.mongodb.annotations.Immutable;
import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class ScoreFusionConstructibleBson extends AbstractConstructibleBson<ScoreFusionConstructibleBson>
        implements ScoreFusionOptions, WeightedScoreFusionCombination {
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    static final ScoreFusionConstructibleBson EMPTY_IMMUTABLE =
            new ScoreFusionConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    ScoreFusionConstructibleBson(final Bson base) {
        super(base);
    }

    private ScoreFusionConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected ScoreFusionConstructibleBson newSelf(final Bson base, final Document appended) {
        return new ScoreFusionConstructibleBson(base, appended);
    }

    @Override
    public ScoreFusionOptions combination(final ScoreFusionCombination combination) {
        return newAppended("combination", notNull("combination", combination));
    }

    @Override
    public ScoreFusionOptions scoreDetails(final boolean scoreDetails) {
        return newAppended("scoreDetails", BsonBoolean.valueOf(scoreDetails));
    }

    @Override
    public ScoreFusionOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }

    @Override
    public WeightedScoreFusionCombination avg() {
        return newAppended("method", new BsonString("avg"));
    }
}
```

`ScoreFusionCombination.java`:

```java
package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The way in which the normalized scores produced by the input pipelines of the
 * {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization, ScoreFusionOptions) $scoreFusion}
 * stage are combined into the final score. The server rejects specifying both
 * {@linkplain #weighted(Bson) weights} and an {@linkplain #expression(Bson) expression},
 * which is why they are separate factory methods.
 *
 * @see ScoreFusionOptions#combination(ScoreFusionCombination)
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreFusionCombination extends Bson {
    /**
     * Returns a {@link WeightedScoreFusionCombination} combining the scores using per-pipeline weights.
     *
     * @param weights A document mapping {@linkplain FusionPipeline#getName() pipeline names} to non-negative
     * numeric weights. Pipelines not mentioned have the server-default weight 1.
     * @return The requested {@link WeightedScoreFusionCombination}.
     */
    static WeightedScoreFusionCombination weighted(final Bson weights) {
        return new ScoreFusionConstructibleBson(new Document("weights", notNull("weights", weights)));
    }

    /**
     * Returns a {@link ScoreFusionCombination} combining the scores using a custom expression.
     * The normalized, weighted score of each input pipeline is available to the expression
     * as the variable named after the pipeline, e.g., {@code "$$name"}.
     *
     * @param expression The combination expression.
     * @return The requested {@link ScoreFusionCombination}.
     */
    static ScoreFusionCombination expression(final Bson expression) {
        return new ScoreFusionConstructibleBson(new Document("method", new BsonString("expression"))
                .append("expression", notNull("expression", expression)));
    }
}
```

Note: `new ScoreFusionConstructibleBson(new Document(...))` uses the package-private `(Bson base)` constructor. The `Document` two-key chain in `expression(...)`: `new Document("method", ...).append("expression", ...)` returns `Document` — fine.

`WeightedScoreFusionCombination.java`:

```java
package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;

/**
 * A {@linkplain ScoreFusionCombination#weighted(org.bson.conversions.Bson) weighted} {@link ScoreFusionCombination}.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface WeightedScoreFusionCombination extends ScoreFusionCombination {
    /**
     * Returns a new {@link WeightedScoreFusionCombination} that instructs the server to combine the
     * weighted scores using their average instead of the default combination method.
     *
     * @return A new {@link WeightedScoreFusionCombination}.
     */
    WeightedScoreFusionCombination avg();
}
```

`ScoreFusionOptions.java`:

```java
package com.mongodb.client.model;

import com.mongodb.annotations.Sealed;
import org.bson.conversions.Bson;

/**
 * Represents optional fields of the {@linkplain Aggregates#scoreFusion(java.util.List, ScoreNormalization,
 * ScoreFusionOptions) $scoreFusion} pipeline stage of an aggregation pipeline.
 *
 * @since 5.10
 * @mongodb.server.release 8.2
 */
@Sealed
public interface ScoreFusionOptions extends Bson {
    /**
     * Returns {@link ScoreFusionOptions} that represents server defaults.
     *
     * @return {@link ScoreFusionOptions} that represents server defaults.
     */
    static ScoreFusionOptions scoreFusionOptions() {
        return ScoreFusionConstructibleBson.EMPTY_IMMUTABLE;
    }

    /**
     * Creates a new {@link ScoreFusionOptions} with the combination specified.
     * If not specified, the server combines the normalized scores using its default method.
     *
     * @param combination The way in which the normalized scores are combined.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions combination(ScoreFusionCombination combination);

    /**
     * Creates a new {@link ScoreFusionOptions} with the scoreDetails flag specified.
     * When {@code true}, the server exposes score details via the {@code {$meta: "scoreDetails"}} expression.
     * Server default is {@code false}.
     *
     * @param scoreDetails Whether to include score details.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions scoreDetails(boolean scoreDetails);

    /**
     * Creates a new {@link ScoreFusionOptions} with the specified option in situations when there is no builder method
     * that better satisfies your needs.
     * This method cannot be used to validate the syntax.
     *
     * @param name The option name.
     * @param value The option value.
     * @return A new {@link ScoreFusionOptions}.
     */
    ScoreFusionOptions option(String name, Object value);
}
```

- [ ] **Step 4: Run test to verify it passes.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — PASS.

- [ ] **Step 5: Commit.**

```bash
git add driver-core/src/main/com/mongodb/client/model/ScoreFusionCombination.java driver-core/src/main/com/mongodb/client/model/WeightedScoreFusionCombination.java driver-core/src/main/com/mongodb/client/model/ScoreFusionOptions.java driver-core/src/main/com/mongodb/client/model/ScoreFusionConstructibleBson.java driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy
git commit -m "JAVA-5990 Add ScoreFusionCombination and ScoreFusionOptions"
```

---

### Task 4: `Aggregates.scoreFusion` stage

**Files:**
- Modify: `driver-core/src/main/com/mongodb/client/model/Aggregates.java` (public factories near `vectorSearch` around line 1030; nested stage class near `VectorSearchBson` around line 2310)
- Test: `driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy`

**Interfaces:**
- Consumes: `FusionPipeline.getName()/getPipeline()` (Task 2), `ScoreNormalization.toBsonValue()` (Task 1), `ScoreFusionOptions`/`scoreFusionOptions()` (Task 3).
- Produces: `public static Bson scoreFusion(List<FusionPipeline> pipelines, ScoreNormalization normalization)` and `public static Bson scoreFusion(List<FusionPipeline> pipelines, ScoreNormalization normalization, ScoreFusionOptions options)`. Tasks 5-7 consume these.

- [ ] **Step 1: Write the failing tests.**

```groovy
def 'should render $scoreFusion'() {
    expect:
    toBson(scoreFusion(
            [FusionPipeline.of('p1', match(eq('x', 1))), FusionPipeline.of('p2', match(eq('x', 2)))],
            ScoreNormalization.sigmoid())) ==
            parse('''{$scoreFusion: {input: {
                pipelines: {p1: [{$match: {x: 1}}], p2: [{$match: {x: 2}}]},
                normalization: "sigmoid"}}}''')

    toBson(scoreFusion(
            [FusionPipeline.of('p1', match(eq('x', 1))), FusionPipeline.of('p2', match(eq('x', 2)), limit(5))],
            ScoreNormalization.minMaxScaler(),
            scoreFusionOptions()
                    .combination(ScoreFusionCombination.weighted(new Document('p1', 0.3d).append('p2', 0.7d)).avg())
                    .scoreDetails(true))) ==
            parse('''{$scoreFusion: {input: {
                pipelines: {p1: [{$match: {x: 1}}], p2: [{$match: {x: 2}}, {$limit: 5}]},
                normalization: "minMaxScaler"},
                combination: {weights: {p1: 0.3, p2: 0.7}, method: "avg"},
                scoreDetails: true}}''')

    toBson(scoreFusion(
            [FusionPipeline.of('p1', match(eq('x', 1))), FusionPipeline.of('p2', match(eq('x', 2)))],
            ScoreNormalization.none(),
            scoreFusionOptions().combination(
                    ScoreFusionCombination.expression(new Document('$sum', ['$$p1', '$$p2']))))) ==
            parse('''{$scoreFusion: {input: {
                pipelines: {p1: [{$match: {x: 1}}], p2: [{$match: {x: 2}}]},
                normalization: "none"},
                combination: {method: "expression", expression: {$sum: ["$$p1", "$$p2"]}}}}''')
}

def 'should validate $scoreFusion arguments'() {
    when:
    scoreFusion([], ScoreNormalization.none())

    then:
    thrown(IllegalArgumentException)

    when:
    scoreFusion([FusionPipeline.of('p1', match(eq('x', 1))), FusionPipeline.of('p1', match(eq('x', 2)))],
            ScoreNormalization.none())

    then:
    thrown(IllegalArgumentException)

    when:
    scoreFusion(null, ScoreNormalization.none())

    then:
    thrown(IllegalArgumentException)

    when:
    scoreFusion([FusionPipeline.of('p1', match(eq('x', 1)))], null)

    then:
    thrown(IllegalArgumentException)
}
```

Add static import `com.mongodb.client.model.Aggregates.scoreFusion` to the spec if `Aggregates.*` is not already imported (the spec statically imports individual `Aggregates` methods — follow that pattern).

- [ ] **Step 2: Run tests to verify they fail.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — compilation failure on `scoreFusion`.

- [ ] **Step 3: Implement.** In `Aggregates.java`, add the two factories after the last `vectorSearch` overload (keep the file's Javadoc style):

```java
    /**
     * Creates a {@code $scoreFusion} pipeline stage, which combines the results of the given input pipelines,
     * normalizing and combining the scores they produce. It must be the first stage of an aggregation pipeline
     * run on a collection, and the input pipelines must be scored selection pipelines on the same collection,
     * e.g., starting with a {@link #search(SearchOperator, SearchOptions) $search}
     * or {@link #vectorSearch(FieldSearchPath, Iterable, String, long, VectorSearchOptions) $vectorSearch} stage.
     * You may use the {@code $meta: "score"} expression to extract the combined score of a document.
     *
     * @param pipelines The non-empty input pipelines with unique names.
     * @param normalization The way in which the scores produced by the input pipelines are normalized.
     * @return The {@code $scoreFusion} pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/scoreFusion/ $scoreFusion
     * @mongodb.server.release 8.2
     * @since 5.10
     */
    public static Bson scoreFusion(final List<FusionPipeline> pipelines, final ScoreNormalization normalization) {
        return scoreFusion(pipelines, normalization, ScoreFusionOptions.scoreFusionOptions());
    }

    /**
     * Creates a {@code $scoreFusion} pipeline stage, which combines the results of the given input pipelines,
     * normalizing and combining the scores they produce. It must be the first stage of an aggregation pipeline
     * run on a collection, and the input pipelines must be scored selection pipelines on the same collection,
     * e.g., starting with a {@link #search(SearchOperator, SearchOptions) $search}
     * or {@link #vectorSearch(FieldSearchPath, Iterable, String, long, VectorSearchOptions) $vectorSearch} stage.
     * You may use the {@code $meta: "score"} expression to extract the combined score of a document.
     *
     * @param pipelines The non-empty input pipelines with unique names.
     * @param normalization The way in which the scores produced by the input pipelines are normalized.
     * @param options Optional {@code $scoreFusion} pipeline stage fields.
     * @return The {@code $scoreFusion} pipeline stage.
     * @mongodb.driver.manual reference/operator/aggregation/scoreFusion/ $scoreFusion
     * @mongodb.server.release 8.2
     * @since 5.10
     */
    public static Bson scoreFusion(final List<FusionPipeline> pipelines, final ScoreNormalization normalization,
            final ScoreFusionOptions options) {
        notNull("pipelines", pipelines);
        isTrueArgument("pipelines must not be empty", !pipelines.isEmpty());
        Set<String> names = new HashSet<>();
        for (FusionPipeline pipeline : pipelines) {
            notNull("pipeline", pipeline);
            isTrueArgument("pipeline names must be unique", names.add(pipeline.getName()));
        }
        notNull("normalization", normalization);
        notNull("options", options);
        return new ScoreFusionStage(pipelines, normalization, options);
    }
```

Check the imports at the top of `Aggregates.java`: `java.util.HashSet`, `java.util.Set`, `java.util.List` and static `com.mongodb.assertions.Assertions.isTrueArgument`/`notNull` — most already exist; add any missing.

Add the nested class next to `VectorSearchBson` (match its style):

```java
    private static final class ScoreFusionStage implements Bson {
        private final List<FusionPipeline> pipelines;
        private final ScoreNormalization normalization;
        private final ScoreFusionOptions options;

        ScoreFusionStage(final List<FusionPipeline> pipelines, final ScoreNormalization normalization,
                final ScoreFusionOptions options) {
            this.pipelines = pipelines;
            this.normalization = normalization;
            this.options = options;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument pipelinesDoc = new BsonDocument();
            for (FusionPipeline pipeline : pipelines) {
                BsonArray stages = new BsonArray();
                for (Bson stage : pipeline.getPipeline()) {
                    stages.add(stage.toBsonDocument(documentClass, codecRegistry));
                }
                pipelinesDoc.append(pipeline.getName(), stages);
            }
            BsonDocument specificationDoc = new BsonDocument("input",
                    new BsonDocument("pipelines", pipelinesDoc)
                            .append("normalization", normalization.toBsonValue()));
            specificationDoc.putAll(options.toBsonDocument(documentClass, codecRegistry));
            return new BsonDocument("$scoreFusion", specificationDoc);
        }

        @Override
        public String toString() {
            return "Stage{name=$scoreFusion"
                    + ", pipelines=" + pipelines
                    + ", normalization=" + normalization
                    + ", options=" + options
                    + '}';
        }
    }
```

(`org.bson.BsonArray` import may be missing — add it.)

- [ ] **Step 4: Run tests to verify they pass.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — PASS.

- [ ] **Step 5: Run the module's static checks** (checkstyle/spotbugs catch Javadoc and style errors early): `./gradlew :driver-core:compileJava checkstyleMain` — expected: BUILD SUCCESSFUL. Fix any violations in the files you touched.

- [ ] **Step 6: Commit.**

```bash
git add driver-core/src/main/com/mongodb/client/model/Aggregates.java driver-core/src/test/unit/com/mongodb/client/model/AggregatesSpecification.groovy
git commit -m "JAVA-5990 Add Aggregates.scoreFusion pipeline stage builder"
```

---

### Task 5: Functional end-to-end tests (plain server, 8.2+)

**Files:**
- Modify: `driver-core/src/test/functional/com/mongodb/client/model/AggregatesTest.java`

**Interfaces:**
- Consumes: `Aggregates.scoreFusion(...)` (Task 4), `FusionPipeline.of(...)`, `ScoreNormalization.*`, `scoreFusionOptions()`, `ScoreFusionCombination.*`.

These tests exercise every option against a real server using `$match` + raw `$score` sub-pipelines with chosen score values, so results are deterministic. They are skipped on servers older than 8.2.

- [ ] **Step 1: Write the tests.** Add to `AggregatesTest` (follow the class's existing style; `getCollectionHelper()` comes from `OperationTest`). Add imports: `org.bson.BsonArray` (if needed), static imports `com.mongodb.client.model.Aggregates.match`, `com.mongodb.client.model.Aggregates.scoreFusion`, `com.mongodb.client.model.Aggregates.project`, `com.mongodb.client.model.Filters.exists`, `com.mongodb.client.model.ScoreFusionOptions.scoreFusionOptions`, and `java.util.stream.Collectors.toList`.

```java
    // TODO JAVA-6202 replace the raw $score documents in these tests with the $score builder once available
    private static final Bson SCORE_BY_X = BsonDocument.parse("{$score: {score: '$x'}}");
    private static final Bson SCORE_BY_Y = BsonDocument.parse("{$score: {score: '$y'}}");

    private void insertScoreFusionDocuments() {
        getCollectionHelper().insertDocuments(
                "{_id: 1, x: 10, y: 1}",
                "{_id: 2, x: 5, y: 2}",
                "{_id: 3, x: 1, y: 3}");
    }

    private List<Integer> idsFor(final Bson scoreFusionStage) {
        return getCollectionHelper().aggregate(singletonList(scoreFusionStage)).stream()
                .map(doc -> doc.getInteger("_id"))
                .collect(toList());
    }

    @ParameterizedTest
    @MethodSource("scoreFusionNormalizations")
    public void shouldScoreFusionWithEachNormalization(final ScoreNormalization normalization) {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        List<Integer> ids = idsFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                normalization));
        assertEquals(3, ids.size());
    }

    private static Stream<ScoreNormalization> scoreFusionNormalizations() {
        return Stream.of(ScoreNormalization.none(), ScoreNormalization.sigmoid(), ScoreNormalization.minMaxScaler());
    }

    @Test
    public void shouldScoreFusionWithWeights() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        // weight only the "byY" pipeline: expected order is descending y
        List<Integer> ids = idsFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.none(),
                scoreFusionOptions().combination(
                        ScoreFusionCombination.weighted(new Document("byX", 0).append("byY", 1)))));
        assertEquals(asList(3, 2, 1), ids);
    }

    @Test
    public void shouldScoreFusionWithWeightsAndAvgMethod() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        List<Integer> ids = idsFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.none(),
                scoreFusionOptions().combination(
                        ScoreFusionCombination.weighted(new Document("byX", 1).append("byY", 0)).avg())));
        assertEquals(asList(1, 2, 3), ids);
    }

    @Test
    public void shouldScoreFusionWithExpressionCombination() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        // ignore byX, use 10 * byY: expected order is descending y
        List<Integer> ids = idsFor(scoreFusion(
                asList(
                        FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                        FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                ScoreNormalization.none(),
                scoreFusionOptions().combination(ScoreFusionCombination.expression(
                        new Document("$sum", asList(
                                new Document("$multiply", asList("$$byX", 0)),
                                new Document("$multiply", asList("$$byY", 10))))))));
        assertEquals(asList(3, 2, 1), ids);
    }

    @Test
    public void shouldScoreFusionWithScoreDetails() {
        assumeTrue(serverVersionAtLeast(8, 2));
        insertScoreFusionDocuments();
        List<Document> results = getCollectionHelper().aggregate(asList(
                scoreFusion(
                        asList(
                                FusionPipeline.of("byX", match(exists("x")), SCORE_BY_X),
                                FusionPipeline.of("byY", match(exists("y")), SCORE_BY_Y)),
                        ScoreNormalization.sigmoid(),
                        scoreFusionOptions().scoreDetails(true)),
                project(Projections.fields(
                        Projections.meta("score", "score"),
                        Projections.meta("scoreDetails", "scoreDetails")))));
        assertEquals(3, results.size());
        Document details = (Document) results.get(0).get("scoreDetails");
        Assertions.assertNotNull(details);
        Assertions.assertNotNull(details.get("details"));
    }
```

Note on helper types: `getCollectionHelper().aggregate(...)` in `AggregatesTest` returns `List<Document>` (the class uses a `Document` codec) — verify by looking at an existing test in the file and adjust `idsFor` accordingly (e.g., if it returns `List<BsonDocument>`, map with `doc.getInt32("_id").getValue()`). Also verify the exact `Projections.meta` signature (`meta(String fieldName, String metaFieldName)`); if only single-arg variants exist, use `new Document("score", new Document("$meta", "score"))`-style raw projections.

- [ ] **Step 2: Compile.** `./gradlew :driver-core:compileTestJava` — BUILD SUCCESSFUL.

- [ ] **Step 3: Run against a server if available.**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.AggregatesTest" -Dorg.mongodb.test.uri="mongodb://localhost:27017"`
Expected: PASS (tests skipped if the server is older than 8.2 — report which happened). If no server is reachable, state that clearly; do not claim the tests ran.

- [ ] **Step 4: Commit.**

```bash
git add driver-core/src/test/functional/com/mongodb/client/model/AggregatesTest.java
git commit -m "JAVA-5990 Add functional tests for the \$scoreFusion stage"
```

---

### Task 6: Atlas hybrid-search integration test

**Files:**
- Modify: `driver-core/src/test/functional/com/mongodb/client/model/search/AggregatesSearchIntegrationTest.java`

**Interfaces:**
- Consumes: `Aggregates.scoreFusion(...)`, `FusionPipeline`, `ScoreNormalization`, `ScoreFusionCombination`, `scoreFusionOptions()`.

One realistic hybrid test: one `$vectorSearch` + one `$search` sub-pipeline on `sample_mflix.embedded_movies`, fused with normalization + weights. Runs only when `isAtlasSearchTest()` is true (Atlas CI variant); locally it will be skipped — that is the expected outcome.

- [ ] **Step 1: Write the test.** Add to `AggregatesSearchIntegrationTest`, reusing its existing fixtures (`MFLIX_EMBEDDED_MOVIES_NS`, `QUERY_VECTOR`, `collectionHelpers`, `msgSupplier`, `Asserters`). The class-level `@BeforeEach` already applies `assumeTrue(isAtlasSearchTest())`. Check the class Javadoc header for the search index available on `embedded_movies` and use it for the `$search` sub-pipeline (the vector index is `"sample_mflix__embedded_movies"` as used by the existing `vectorSearch` test); if the header lists no text index on `embedded_movies`, use the index it does list for that collection.

```java
    @Test
    void scoreFusion() {
        assumeTrue(serverVersionAtLeast(8, 2));
        CollectionHelper<BsonDocument> collectionHelper = collectionHelpers.get(MFLIX_EMBEDDED_MOVIES_NS);
        List<Bson> pipeline = asList(
                Aggregates.scoreFusion(
                        asList(
                                FusionPipeline.of("vector", Aggregates.vectorSearch(
                                        fieldPath("plot_embedding"), QUERY_VECTOR, "sample_mflix__embedded_movies", LIMIT,
                                        approximateVectorSearchOptions(LIMIT + 1))),
                                FusionPipeline.of("text",
                                        Aggregates.search(SearchOperator.text(fieldPath("title"), "train"),
                                                searchOptions().index("default")),
                                        Aggregates.limit(LIMIT))),
                        ScoreNormalization.sigmoid(),
                        scoreFusionOptions()
                                .combination(ScoreFusionCombination.weighted(
                                        new Document("vector", 0.7).append("text", 0.3)))
                                .scoreDetails(true)),
                Aggregates.limit(LIMIT));
        List<BsonDocument> results = collectionHelper.aggregate(pipeline);
        Asserters.nonEmpty().accept(results, msgSupplier(pipeline));
    }
```

Add whatever imports the file is missing (`FusionPipeline`, `ScoreNormalization`, `ScoreFusionCombination`, static `scoreFusionOptions`, `org.bson.Document` or use `BsonDocument` for weights — `weighted` accepts any `Bson`).

- [ ] **Step 2: Compile.** `./gradlew :driver-core:compileTestJava` — BUILD SUCCESSFUL.

- [ ] **Step 3: Verify the test is skipped locally (not failing).**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.search.AggregatesSearchIntegrationTest.scoreFusion" -Dorg.mongodb.test.uri="mongodb://localhost:27017"` (only if a local server is available)
Expected: test SKIPPED (assumption `isAtlasSearchTest()` fails). Actual Atlas execution happens in the Evergreen Atlas-search variant.

- [ ] **Step 4: Commit.**

```bash
git add driver-core/src/test/functional/com/mongodb/client/model/search/AggregatesSearchIntegrationTest.java
git commit -m "JAVA-5990 Add Atlas hybrid search integration test for \$scoreFusion"
```

---

### Task 7: Scala wrapper

**Files:**
- Modify: `driver-scala/src/main/scala/org/mongodb/scala/model/Aggregates.scala` (add `scoreFusion` forwarders next to `facet`/`vectorSearch`)
- Modify: `driver-scala/src/main/scala/org/mongodb/scala/model/package.scala` (add type aliases + `FusionPipeline` companion, next to the `Facet` alias around line 343)

**Interfaces:**
- Consumes: the Java API from Tasks 1-4.
- Produces: `org.mongodb.scala.model.Aggregates.scoreFusion(pipelines: Seq[FusionPipeline], normalization: ScoreNormalization[, options: ScoreFusionOptions]): Bson` and aliases `FusionPipeline`, `ScoreNormalization`, `ScoreFusionCombination`, `WeightedScoreFusionCombination`, `ScoreFusionOptions`.

- [ ] **Step 1: Add aliases to `package.scala`** (mirror the `Facet` and `VectorSearchOptions` snippets shown at lines ~339-362 and the search package's alias style; `@Sealed` import already exists in the search package — in `model/package.scala` check whether `Sealed` is imported and follow whatever the file does for other sealed aliases):

```scala
  /**
   * A named aggregation pipeline used as an input to a fusion pipeline stage, e.g., `\$scoreFusion`.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  type FusionPipeline = com.mongodb.client.model.FusionPipeline

  /**
   * A named aggregation pipeline used as an input to a fusion pipeline stage, e.g., `\$scoreFusion`.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  object FusionPipeline {

    /**
     * Construct a new instance
     *
     * @param name     the non-empty pipeline name, unique within the containing stage
     * @param pipeline the non-empty pipeline
     * @return the new FusionPipeline
     */
    def apply(name: String, pipeline: Bson*): FusionPipeline = {
      com.mongodb.client.model.FusionPipeline.of(name, pipeline.asJava)
    }
  }

  /**
   * The way in which the scores produced by the `\$scoreFusion` input pipelines are normalized before being combined.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  type ScoreNormalization = com.mongodb.client.model.ScoreNormalization

  /**
   * The way in which the normalized scores produced by the `\$scoreFusion` input pipelines are combined.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  type ScoreFusionCombination = com.mongodb.client.model.ScoreFusionCombination

  /**
   * A weighted `ScoreFusionCombination`.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  type WeightedScoreFusionCombination = com.mongodb.client.model.WeightedScoreFusionCombination

  /**
   * Represents optional fields of the `\$scoreFusion` pipeline stage of an aggregation pipeline.
   *
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  type ScoreFusionOptions = com.mongodb.client.model.ScoreFusionOptions
```

Scala 2.11 is supported and cannot call static methods on Java interfaces directly from all contexts — check how the codebase handles this for `VectorSearchOptions` (search package exposes only the `type` alias, and users call the Java statics or the search package provides forwarder objects). Mirror exactly what `driver-scala/src/main/scala/org/mongodb/scala/model/search/package.scala` does; if it provides companion forwarder objects for sealed option interfaces, add matching ones for `ScoreNormalization`, `ScoreFusionCombination`, and `ScoreFusionOptions`.

- [ ] **Step 2: Add forwarders to Scala `Aggregates.scala`** (next to `def facet`, using the file's `JAggregates` alias):

```scala
  /**
   * Creates a `\$scoreFusion` pipeline stage, which combines the results of the given input pipelines,
   * normalizing and combining the scores they produce.
   *
   * @param pipelines     the non-empty input pipelines with unique names
   * @param normalization the way in which the scores produced by the input pipelines are normalized
   * @return the `\$scoreFusion` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/ \$scoreFusion]]
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  def scoreFusion(pipelines: Seq[FusionPipeline], normalization: ScoreNormalization): Bson =
    JAggregates.scoreFusion(pipelines.asJava, normalization)

  /**
   * Creates a `\$scoreFusion` pipeline stage, which combines the results of the given input pipelines,
   * normalizing and combining the scores they produce.
   *
   * @param pipelines     the non-empty input pipelines with unique names
   * @param normalization the way in which the scores produced by the input pipelines are normalized
   * @param options       optional `\$scoreFusion` pipeline stage fields
   * @return the `\$scoreFusion` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/ \$scoreFusion]]
   * @note Requires MongoDB 8.2 or greater
   * @since 5.10
   */
  def scoreFusion(pipelines: Seq[FusionPipeline], normalization: ScoreNormalization, options: ScoreFusionOptions): Bson =
    JAggregates.scoreFusion(pipelines.asJava, normalization, options)
```

- [ ] **Step 3: Run the Scala API-surface test and unit tests.**

Run: `./gradlew :driver-scala:test --tests "*ApiAliasAndCompanionSpec*" --tests "*AggregatesSpec*"`
Expected: `ApiAliasAndCompanionSpec` fails if a new public Java class lacks an alias — add whatever it reports missing (that is the test's job), then re-run until PASS.

- [ ] **Step 4: Commit.**

```bash
git add driver-scala/src/main/scala/org/mongodb/scala/model/Aggregates.scala driver-scala/src/main/scala/org/mongodb/scala/model/package.scala
git commit -m "JAVA-5990 Add Scala wrapper for the \$scoreFusion stage builder"
```

---

### Task 8: Full verification

**Files:** none new — fixes only if checks fail.

- [ ] **Step 1: Run the pre-submission checks from AGENTS.md.**

Run: `./gradlew spotlessApply docs :driver-core:check scalaCheck`
Expected: BUILD SUCCESSFUL. Common failure causes to fix: checkstyle Javadoc violations, spotbugs warnings on the new nested stage class (add an exclusion only if a false positive, matching existing exclusion style in `config/spotbugs/exclude.xml`), Scala API-surface expectations, clirr (should not trigger — additions only).

- [ ] **Step 2: Re-run driver-core unit tests.** `./gradlew :driver-core:test --tests "AggregatesSpecification"` — PASS.

- [ ] **Step 3: Commit any fixes.**

```bash
git add -A ':!diff.txt' && git commit -m "JAVA-5990 Address static analysis and API surface checks" || echo "nothing to fix"
```

- [ ] **Step 4: Report.** Summarize: what passed, what was skipped (functional/Atlas tests without a server), and remind that Evergreen patch build should run the Atlas-search variant before merging.
