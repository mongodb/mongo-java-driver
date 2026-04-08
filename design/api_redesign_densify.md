
This report is based on concepts discussed in api_design.md. The Part 1 is AI generated, Part 2 is human-authored.

# Part 1

## Densify: Redesigning an Existing Stage

The `$densify` aggregation stage fills gaps in a sequence of documents. Its MQL parameters are:

- **field** (required): the field to densify.
- **range.step** + optional **range.unit** (required): the interval between inserted values. Step is always required. Unit is required for dates, absent for numbers.
- **range.bounds** (required): determines the extent of densification. Three variants: `"full"` (global min to max), `"partition"` (per-partition min to max), or `[lower, upper]` (explicit).
- **partitionByFields** (optional): field names to group by before densifying.

The current Java API:

```java
// Minimal:
Aggregates.densify("hour",
    DensifyRange.fullRangeWithStep(1))

// With partition:
Aggregates.densify("hour",
    DensifyRange.partitionRangeWithStep(1),
    DensifyOptions.densifyOptions().partitionByFields("city"))

// With explicit bounds and partition:
Aggregates.densify("hour",
    DensifyRange.rangeWithStep(0, 10, 1),
    DensifyOptions.densifyOptions().partitionByFields("city", "sensor"))

// Date variant:
Aggregates.densify("timestamp",
    DensifyRange.fullRangeWithStep(1, MongoTimeUnit.HOUR))
```

### Analysis

**Step 1: Identify the primary parameter.**

The field is required but is a simple string, so it remains a positional parameter on the operation. Step (+ unit) is always required, and is a coherent item: a number, or a number plus a time unit. It cannot be defaulted (the doc prohibits defaults as a source of optionals). It is the most fundamental parameter — without a step, densify has no meaning.

**Step 2: Can we use overloads (pattern 0.0)?**

We have: field × step (number or date) × bounds (3 variants) × partition (present or not). This is at minimum 2 × 3 × 2 = 12 overloads. The doc prohibits this explosion, so we must introduce at least one entity.

**Step 3: Identify which parameters form entities.**

Per the Domain values guidance, prefer primitives unless the domain value is already used extensively outside the API. Step is a number (or number + time unit) — these are primitives or existing Java types, so step does not need a custom domain value and can use overloads. Bounds has three structurally distinct variants, which cannot be represented as primitives and require some form of entity. Partition fields are already just a list of strings.

Step is primary and always required — it anchors whatever entity we create. The remaining parameters (bounds, partition fields) have variants, so they are not suitable as positional parameters at the operation level. They are secondary to step.

Bounds has three variants: full, partition-scoped, and explicit. The "full" variant is the simplest default case, and "partition" is reductive to "full" when no partition fields are supplied (the whole collection is one partition). Explicit bounds `[low, high]` is structurally different. All three are always required (a bounds strategy must exist, even if it defaults to "full"). The three variants are distinct enough to warrant distinct construction methods rather than an enum + nullable values.

Partition fields are the only true optional. They are a list of field names, present or absent. When absent, "full" and "partition" bounds behave identically.

**Step 4: Consider entity coherence.**

An entity should be coherent independently of the operation it parameterizes, and per Domain values, should be preferred only when already used outside the API. Step (a number, or number + time unit) is independently coherent — it represents an interval — but is not used outside this API, so primitives or overloads are preferred over a custom type. However, once we add bounds, the "full" and especially "partition" variants couple the entity to the concept of partitions within a dataset. This means:

- Step alone is coherent.
- Step + bounds begins to encode dataset-level semantics (full vs. per-partition).
- Partition fields are a dataset-level grouping concern.

If bounds and partition are both dataset-level concerns, they arguably belong together (or at least, bounds should not be separated from partition if the "partition" variant exists). This means either: (a) put all three together in one entity, or (b) keep step + bounds together and partition separate, but accept that the "partition" bounds variant references a concept managed elsewhere.

**Step 5: Survey other stages for shared concepts.**

Partition, bounds, and step appear across multiple aggregation stages:

| Stage | Partition | Bounds/Range | Step |
|---|---|---|---|
| `$densify` | `partitionByFields` (optional, list of strings) | `bounds`: full / partition / [low, high] | `step` + optional `unit` |
| `$setWindowFields` | `partitionBy` (optional, expression) | `Window`: documents(low, high) or range(low, high) or timeRange(low, high, unit) | implicit in bounds |
| `$fill` | `partitionBy` or `partitionByFields` (optional) | n/a | n/a |
| `$bucket` | n/a (groupBy is required) | `boundaries` (explicit list) | n/a |

Partition is a recurring concept, but each stage represents it differently (`partitionByFields` as string list vs. `partitionBy` as expression). Bounds/range is also recurring, but with different semantics (densify: how far to fill; window: how wide the window is). Step is unique to densify.

Per the doc's guidance ("options and builders should never be shared across operations"), these should NOT be unified into shared Partition or Bounds types, even though they appear similar. Each stage's parameterization is its own entity.

**Step 6: Determine entity groupings.**

Three options emerge for how to group these into entities:

**Option A: One entity (all together)**

Step, bounds, and partition fields are all part of one range/spec entity:

```java
densify("hour", step(1).fullRange())
densify("hour", step(1).partitionRange().partitionBy("city"))
densify("hour", step(1).range(0, 10))
densify("hour", step(1, HOUR).fullRange())
densify("hour", step(1).range(0, 10).partitionBy("city"))
```

Advantages: single entity, no optionals at the operation level. Discoverable — start from step, autocomplete shows bounds options, then partition. Keeps partition and bounds in the same entity, which is consistent with their semantic coupling (Step 4).
Concerns: the entity mixes interval specification with dataset-level grouping. The chain ordering implies a sequence but there is none. `partitionRange()` without `partitionBy(...)` is expressible but merely equivalent to `fullRange()` — not invalid, but potentially confusing.

**Option B: Two entities (step + bounds, then partition)**

Step and bounds form one entity. Partition fields are a separate varargs optional (pattern 2.1) at the operation level:

```java
densify("hour", step(1).fullRange())
densify("hour", step(1).fullRange(), DensifyOption.partitionBy("city"))
densify("hour", step(1).range(0, 10))
densify("hour", step(1, HOUR).fullRange())
densify("hour", step(1).range(0, 10), DensifyOption.partitionBy("city"))
```

Advantages: step and bounds are tightly related (bounds determine where steps are placed). Partition is a genuinely separate concern — whether to group — and the varargs pattern keeps it extensible. The "partition" bounds variant's reference to partition fields is addressed by the option being visible at the same call site.
Concerns: introduces a `DensifyOption` type for a single current optional. The "partition" bounds variant semantically depends on the partition option, creating a cross-entity dependency.

**Option C: Three entities (all separate)**

Step, bounds, and partition are each separate parameters:

```java
densify("hour", step(1), fullRange())
densify("hour", step(1), partitionRange(), DensifyOption.partitionBy("city"))
densify("hour", step(1), range(0, 10))
densify("hour", step(1, HOUR), fullRange())
densify("hour", step(1), range(0, 10), DensifyOption.partitionBy("city"))
```

Advantages: each parameter is independent and single-purpose. Easy to understand in isolation. Step is independently coherent (Step 4).
Concerns: more positional parameters. The relationship between step and bounds is implicit rather than structural. The operation signature is wider: `densify(String, Step, Bounds, DensifyOption...)`. Bounds defaulting to "full" when absent would reintroduce a default.

### Decision

**Option B** is recommended: step + bounds form one entity; partition fields use varargs at the operation level.

```java
densify("hour", step(1).fullRange())
densify("hour", step(1).partitionRange(), DensifyOption.partitionBy("city"))
densify("hour", step(1).range(0, 10), DensifyOption.partitionBy("city", "sensor"))
densify("hour", step(1, HOUR).fullRange())
```

Rationale:

1. **Cohesion**. Step and bounds are meaningless without each other: step is the interval, bounds determine where it applies. They belong in the same entity. Partition fields are genuinely optional and separable — they govern grouping, not the fill strategy itself.
2. **Follows stated guidance**. The document recommends "use 2.1 Varargs" for operation-level optionals. Partition fields are the only optional, and varargs handles them cleanly.
3. **Cross-entity dependency is manageable**. The "partition range" bounds variant references the concept of partitions, but it is coherent on its own: it means "fill from per-group min to per-group max", which is meaningful even before you specify which fields define the groups. If no partition fields are supplied, partition range degrades to full range — unusual, but not invalid.
4. **Extensibility**. If new options emerge (e.g., a fill strategy, a null-handling flag), they absorb naturally as additional `DensifyOption` variants. Adding methods to the step+bounds entity would be appropriate only for options tightly related to filling, which is unlikely.
5. **Consistency with the current API**. The existing driver already separates partition into `DensifyOptions`, suggesting the original designers had the same instinct. Option B refines this by making the step+bounds entity more fluent and discoverable (chain from step, choose bounds) while keeping the operation-level separation.

Option A was considered but rejected: folding partition into the range entity is more concise, but it mixes the "how to fill" concern (step + bounds) with the "how to group" concern (partition), violating the principle that entities should represent a single coherent concept. It also moves an optional into the entity where it becomes an ignorable method, rather than using the varargs pattern the document recommends.

Option C was rejected because it separates step from bounds, despite their tight interdependence. This widens the operation signature and makes the relationship between parameters implicit rather than structural.

# Part 2 (Human eval)

None of the above seem adequate. We have: field, step, bounds, partitionByFields.

1. Step is coupled to the idea of "densify", and distinct from the concept of a range, which is common to other operations. These should be separate.
2. The time unit applies to all provided numbers.
3. The presence of the time unit is akin to a flag, suggesting an overload.
4. Representing the time unit is a matter of API consistency. We have MongoTimeUnit (vs ChronoUnit), which further precludes `Duration`.
5. Partitioning is closely coupled to the range (that is, the shape of the area that we are densifying).
6. The specifics of the range are not shared with other operations, and might change independently of them.

Before and after:

```java
// Minimal:
Aggregates.densify("test", DensifyRange.fullRangeWithStep(1))
// after:
Aggregates.densify("test", 1, DensifyRange.fullRange())

// With partition:
Aggregates.densify("test",
    DensifyRange.partitionRangeWithStep(1),
    DensifyOptions.densifyOptions().partitionByFields("city"))
// after:
Aggregates.densify("test", 1,
    DensifyRange.partitionRange(1).partitionByFields("city"))

// With explicit bounds and partition:
Aggregates.densify("test",
    DensifyRange.rangeWithStep(0, 10, 1),
    DensifyOptions.densifyOptions().partitionByFields("city", "sensor"))
// after:
Aggregates.densify("test", 1,
    DensifyRange.range(0, 10).partitionByFields("city", "sensor"))

// Date variant:
Aggregates.densify("timestamp",
        DensifyRange.fullRangeWithStep(1, MongoTimeUnit.HOUR))
// after:
Aggregates.densify("timestamp", 1, MongoTimeUnit.HOUR, DensifyRange.fullRange())
```

The original has the benefit of clearly labelling step, but it includes step in the concept of a "DensifyRange". Conversely, the fields, which seem related to the range, are part of an option, even though they are not strictly optional but rather a list that might be empty, and in any case, they can be moved to the DensifyRange. 