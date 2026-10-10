This report is based on concepts discussed in api_design.md

# Research: Java Driver

This section organizes the public API by the distinction drawn above: **creation/configuration** (building entities) vs. **parameterization** (specifying operations), with code snippets illustrating distinctions.

## A. Creation / Configuration

Everything used to build a long-lived entity: clients, connections, credentials, buckets.

### A1. The Builder Pattern

The standard pattern for entity configuration. A `Builder` inner class accumulates settings; `.build()` produces an immutable result.

```java
// The full pattern in one example: builder, nested builders,
// domain value inputs, factory class consuming the result, listener registration.
MongoClient client = MongoClients.create(           // factory class consumes the built settings
    MongoClientSettings.builder()                   // .builder() starts the builder
        .applyConnectionString(new ConnectionString("mongodb://localhost"))
        .readPreference(ReadPreference.secondary()) // domain value as builder input
        .writeConcern(WriteConcern.MAJORITY)         // domain value (static constant)
        .credential(MongoCredential.createCredential("user", "admin", pw)) // domain value (static factory)
        .applyToConnectionPoolSettings(b -> b        // nested builder via callback
            .maxSize(50)
            .maxWaitTime(10, SECONDS))
        .applyToSocketSettings(b -> b               // another nested builder
            .connectTimeout(5, SECONDS))
        .addCommandListener(commandListener)        // listener registration — just another builder method
        .applyToConnectionPoolSettings(b -> b       // listener on nested builder
            .addConnectionPoolListener(poolListener))
        .build());                                  // .build() produces immutable MongoClientSettings
```

**Who uses this:** MongoClientSettings, ClusterSettings, ConnectionPoolSettings, SocketSettings, SslSettings, ServerSettings, AutoEncryptionSettings, ClientSessionOptions, TransactionOptions, Collation, ServerApi.

Structure:
- `X.builder()` → `X.Builder`; `X.builder(existingX)` → copy-constructor builder.
- Builder methods: bare names returning `this` (`.maxSize()`, `.readPreference()`).
- `.build()` → immutable result (`@Immutable`), validated in private constructor.
- Time-related parameters use `method(long value, TimeUnit unit)` signature (`.maxWaitTime(10, SECONDS)`).

Sub-patterns that appear within or alongside this builder pattern:

**Domain value objects as builder inputs.** ReadPreference, WriteConcern, ReadConcern, MongoCredential, and ConnectionString are immutable values created independently, then passed to builders. Their construction idioms vary:

```java
ReadPreference.secondary()                              // static factory
ReadPreference.secondary(tagSet, 5000, MILLISECONDS)    // overloaded static factory (~28 overloads total)
WriteConcern.MAJORITY                                   // static constant
new WriteConcern(2)                                     // constructor
MongoCredential.createCredential("user", "admin", pw)   // static factory (createXxx naming)
new ConnectionString("mongodb://localhost")             // constructor (parses elements from connection URI)
```

Some domain values support `with*` for copy-on-write modification before passing to a builder:
```java
ReadPreference.secondary().withMaxStalenessMS(5000, MILLISECONDS)
WriteConcern.MAJORITY.withWTimeout(5, SECONDS).withJournal(true)
MongoCredential.createCredential(...).withMechanismProperty(KEY, value)
```

**Factory classes** (`MongoClients.create()`, `GridFSBuckets.create()`, `ClientEncryptions.create()`) consume the built settings to produce the entity. This is just the final step of the builder pattern.

**Listener registration** (`addCommandListener()`, `addConnectionPoolListener()`) is just another builder method. Listener interfaces use `default` (empty) methods so users override only the callbacks they need. Event objects are immutable constructor-based data holders.

Note: domain values and `with*` are also used outside the builder context — see A2 below.

Concerns:
1. There are too many variations in the construction of builder inputs. WriteConcern and ReadConcern have public constructors; ReadPreference and MongoCredential use only static factories; ReadConcern is mostly used via constants but also exposes `ReadConcern(ReadConcernLevel)`.
2. ReadPreference has ~28 static factory overloads for (mode × tagSet × maxStaleness).
3. The settings could have been immutable, with no builder required, and wither-style setters.
4. Four immutability strategies coexist across the driver: (a) sealed immutable fluent (Search API, MQL); (b) mutable `return this` (iterables, Options); (c) builder → freeze (Settings); (d) `with*` copy-on-write (domain objects, client hierarchy).

### A2. Copy-on-Write on the Client Hierarchy

MongoCluster, MongoDatabase, MongoCollection, and GridFSBucket use `with*` methods to create new instances with adjusted configuration. This is not the builder pattern — these are runtime configuration adjustments on already-created entities:

```java
MongoCollection<Document> coll = db.getCollection("orders")
    .withReadPreference(ReadPreference.secondary())  // returns new MongoCollection with this ReadPreference
    .withWriteConcern(WriteConcern.MAJORITY)          // returns another new MongoCollection
    .withReadConcern(ReadConcern.MAJORITY)
    .withTimeout(10, SECONDS);
```

This reuses the same domain value objects from A1 (ReadPreference, WriteConcern, etc.), but in a different context. Consistent `with*` naming across all four types. GridFSBucket adds `withChunkSizeBytes()`.

Concerns:

1. This could have been done using existing concepts: `MongoClient.create(oldClient, settingsOverrides)`, which would have been more explicit, or possibly by moving certain settings into operation-level entities: `collection.find(settingsOverrides)`.

## B. Parameterization

Everything used to specify *how an operation will be carried out*. Two major operational flow patterns exist (see Flow above): **chaining** and **list-like** (aggregation pipelines).

### B1. Deferred Operations (fluent iterables / publishers)

For cursor-returning operations (find, distinct, aggregate, watch, listX), `.find()` or similar returns an iterable/publisher object. Chaining configures it. Nothing executes until a terminal method.

The fluent chain mixes two kinds of method:
- **Directive** (parameterization) — define WHAT the operation returns: `filter`, `projection`, `sort`, `limit`, `skip`, `returnKey`, `showRecordId`, `min`, `max`.
- **Configuration** — define HOW the operation executes: `maxTime`, `maxAwaitTime`, `batchSize`, `cursorType`, `noCursorTimeout`, `partial`, `collation`, `allowDiskUse`, `timeoutMode`, `comment`.

Some configuration also exists at the entity level (A2): `withTimeout` on MongoCollection supersedes `maxTime` on FindIterable when set. This is because `withTimeout` uses the new Client Side Operation Timeout (CSOT) system, which replaces the deprecated `maxTimeMS` approach used by `maxTime()`. When CSOT is enabled, legacy `maxTimeMS` settings are ignored.

Both kinds appear on the same chain with no syntactic distinction:

```java
FindIterable<Document> results = collection     // .find() initiates; returns FindIterable
    .find(Filters.eq("status", "active"))       // all chained methods below also return FindIterable
    .projection(Projections.include("name"))    // operational — what fields to return
    .sort(Sorts.descending("age"))              // operational — what order
    .limit(10)                                  // operational — how many
    .collation(Collation.builder()              // configuration — how strings are compared
        .locale("en").build())
    .maxTime(5, SECONDS);                       // configuration — execution time limit

// Terminal: execution happens here.
results.forEach(doc -> process(doc));        // sync — iterates the cursor
Document first = results.first();            // sync — returns first match or null
List<Document> all = results.into(new ArrayList<>());  // sync — collects all into a list
```

```java
// Reactive equivalent: .find() returns a FindPublisher (implements Publisher<Document>).
FindPublisher<Document> results = collection
    .find(Filters.eq("status", "active"))
    .projection(Projections.include("name"))
    .sort(Sorts.descending("age"))
    .limit(10)
    .maxTime(5, SECONDS);

// Terminal: subscription triggers execution.
results.subscribe(subscriber);               // reactive — pushes documents to subscriber
Mono<Document> first = Mono.from(results.first()); // reactive — wraps in Project Reactor Mono
```

Concerns:
1. Although the chaining syntax implies a sequence of transformations, the order of Java method calls on FindIterable does not matter — they all set fields on an underlying command. `.sort(x).limit(10)` is identical to `.limit(10).sort(x)`. The semantic execution order is fixed by the server (filter → sort → skip → limit), regardless of the order you call the Java methods. This contrasts with aggregate pipelines, where the list order of stages determines the execution order.

**AggregateIterable** — operational (what) is the pipeline list; fluent methods are configuration (how):
```java
collection.aggregate(List.of(           // operational — what stages
    Aggregates.match(filter),
    Aggregates.group("$category"),
    Aggregates.sort(sort)))
    .maxTime(5, SECONDS)                // configuration — execution limit
    .allowDiskUse(true);                // configuration — resource usage
```

Concerns:
1. This uses a list-like API, when it would be possible to initiate (and configure) an Aggregate operation off of the `aggregate()` method, and specify ensuing chained operations, as with the Java SE Stream API.
2. Configuration is specified in the ensuing chain. This reverses the idiomatic pattern of using the chain for operations, and specifying configuration in some other way.

**DistinctIterable** — operational is field + `.filter()`; rest is configuration:
```java
collection.distinct("status", String.class)  // operational — what field, what type
    .filter(Filters.gt("count", 10))         // operational — what to match
    .maxTime(5, SECONDS)                     // configuration — execution limit
    .collation(collation);                   // configuration — string comparison
```

**Immediate writes (B2)** — operational is Bson args; configuration is Options object:
```java
collection.updateOne(
    Filters.eq("_id", id),              // operational — what to match
    Updates.set("status", "done"),      // operational — what to change
    new UpdateOptions()                 // configuration — how to execute
        .upsert(true)
        .maxTime(5, SECONDS));
```

Borderline cases: `hint`/`hintString` (optimization directive), `let` (variable bindings for expressions), `comment` (logging metadata). The `explain()` overloads on FindIterable and AggregateIterable are alternative terminals, not configuration.

Sync types: FindIterable, AggregateIterable, DistinctIterable, ChangeStreamIterable, ListCollectionsIterable, ListDatabasesIterable, ListIndexesIterable, ListSearchIndexesIterable, ListCollectionNamesIterable, GridFSFindIterable.
Reactive mirrors: FindPublisher, AggregatePublisher, DistinctPublisher, ChangeStreamPublisher, etc. (identical fluent methods, `Publisher<T>` return types).

Concerns:
1. [AI] Configuration methods (`collation`, `allowDiskUse`, `explain`, `timeoutMode`) are inconsistently available across the iterable family, though this is largely attributable to differences in server-side operations.

### B2. Immediate Operations (static helpers + options)

Write operations (insert, update, delete, findOneAndX) execute immediately and return a result. They take Bson parameters from static factories, and an optional options object:

```java
// Immediate: executes now, returns a result.
UpdateResult result = collection.updateOne(
    Filters.eq("_id", id),                      // static factory produces filter Bson
    Updates.combine(                            // static factory combines two update Bsons into one
        Updates.set("status", "archived"),
        Updates.currentDate("updatedAt")),
    new UpdateOptions()                         // options object for optional settings
        .upsert(true)
        .hint(indexKeys));
```

Concerns:
1. Tracks the server API too closely, using static methods where chaining might have been possible.
2. The upsert boolean might be moved into a new method name: `upsertOne`.
3. [AI] The combine/merge operation has five different names across the static helper classes (see below).
4. [AI] Options handling differs across helpers: Aggregates uses Options classes; Filters uses overloads and `@Nullable` parameters; Updates uses PushOptions only for `pushEach`.

**Static factory helper classes** (shared by both B1 and B2): Filters, Projections, Sorts, Updates, Indexes, Accumulators, Windows. Methods named after the operation: `Filters.eq()`, `Sorts.descending()`, `Updates.set()`.

Each helper class has a "combine" method, but they all use different names for it:
```java
Filters.and(filter1, filter2)            // "and" — semantic name (also: Filters.or)
Updates.combine(update1, update2)        // "combine"
Sorts.orderBy(sort1, sort2)              // "orderBy"
Projections.fields(proj1, proj2)         // "fields"
Indexes.compoundIndex(idx1, idx2)        // "compoundIndex"
// All five do the same thing: merge multiple items of the same type into one Bson.
```

**Options classes** (`new XOptions()`) — mutable self-returning setters, no `.build()`:
```java
new UpdateOptions().upsert(true).hint(indexKeys).collation(collation)
new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).sort(sortBson)
new CreateCollectionOptions().capped(true).sizeInBytes(1048576)
new IndexOptions().unique(true).expireAfter(60L, SECONDS)
```
Used by: UpdateOptions, ReplaceOptions, DeleteOptions, InsertOneOptions, InsertManyOptions, BulkWriteOptions, FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, CountOptions, EstimatedDocumentCountOptions, CreateCollectionOptions, CreateIndexOptions, IndexOptions, DropIndexOptions, DropCollectionOptions, RenameCollectionOptions, GridFSUploadOptions, GridFSDownloadOptions.

### B3. Aggregate Pipeline Operations (list-like)

Aggregation uses a fundamentally different operational flow from find: instead of find's pseudo-chaining, it uses the **list-like** pattern (see Flow above) — a list of stages, each produced by the `Aggregates` static factory class.

```java
// Aggregation: a list of stages, each built with Aggregates.xxx(...)
AggregateIterable<Document> aggResults = collection
    .aggregate(List.of(
        Aggregates.match(                              // stage 1: $match — uses Filters (B2 pattern)
            Filters.eq("status", "active")),
        Aggregates.group("$category",                  // stage 2: $group — field expressions + accumulators
            Accumulators.sum("count", 1),
            Accumulators.avg("avgPrice", "$price")),
        Aggregates.sort(Sorts.descending("count")),    // stage 3: $sort — uses Sorts (B2 pattern)
        Aggregates.limit(10)));                        // stage 4: $limit — plain value

// Terminal: execution happens here (same as B1 find-style).
aggResults.forEach(doc -> process(doc));

// Contrast — find-style chaining for a simpler query:
FindIterable<Document> findResults = collection
    .find(Filters.eq("status", "active"))              // chained, not a list of stages
    .sort(Sorts.descending("count"))
    .limit(10);

// Terminal:
findResults.forEach(doc -> process(doc));
```

The `Aggregates` class is the largest static factory in the API. Its methods fall into several sub-areas, each handling parameters differently:

**Simple stages** — thin wrappers around Bson values from other helpers:
```java
Aggregates.match(filter)          // delegates to Filters
Aggregates.sort(sort)             // delegates to Sorts
Aggregates.project(projection)    // delegates to Projections
Aggregates.limit(n)               // plain int
Aggregates.skip(n)                // plain int
Aggregates.count("fieldName")     // plain string
Aggregates.sample(n)              // plain int
Aggregates.unwind("$arrayField")  // plain string
Aggregates.out("outputCollection")// plain string
Aggregates.replaceRoot(expression)// Bson expression
```

**Stages with structured parameters** — flat positional args, no options:
```java
Aggregates.group(                           // groupBy + accumulators
    "$category",                            //   the group key
    Accumulators.sum("total", "$amount"),    //   accumulators from Accumulators helper
    Accumulators.avg("avg", "$price"))

Aggregates.lookup(                          // simple lookup: all positional
    "inventory",                            //   from collection
    "item",                                 //   local field
    "sku",                                  //   foreign field
    "inventoryDocs")                        //   as field

Aggregates.lookup(                          // pipeline lookup: different set of positional params
    "inventory",                            //   from collection
    let,                                    //   let variables
    pipeline,                               //   sub-pipeline
    "inventoryDocs")                        //   as field
```

**Stages with options objects** — mandatory positional args + optional Options:
```java
Aggregates.bucket(groupBy, boundaries)                        // without options
Aggregates.bucket(groupBy, boundaries, new BucketOptions()    // with options (mutable setter)
    .defaultBucket("other")
    .output(Accumulators.sum("count", 1)))

Aggregates.densify("timestamp",                                // mandatory params
    DensifyRange.partitionRangeWithStep(1, MongoTimeUnit.HOUR),//   DensifyRange uses static factories
    DensifyOptions.densifyOptions().partitionByFields("region"))//   options (named factory, immutable)

Aggregates.geoNear(point, "distance",                          // mandatory params
    GeoNearOptions.geoNearOptions().key("location"))           //   GeoNearOptions (named factory, immutable)

Aggregates.merge("outputCollection", new MergeOptions()        // mandatory + options
    .uniqueIdentifier("_id")
    .whenMatched(MergeOptions.WhenMatched.MERGE))
```

**Stages that delegate to sealed APIs** — see B4 (Search) and B5 (MQL):
```java
Aggregates.search(SearchOperator.text(...))                    // delegates to Search API (B4)
Aggregates.search(SearchOperator.text(...), searchOptions)     // with SearchOptions
Aggregates.vectorSearch(path, vector, index, candidates, limit)// many positionals + VectorSearchOptions

Aggregates.addFields(new Field<>("computed",                   // delegates to MQL Expressions (B5)
    current().getString("name").toLower()))
```

Concerns:
1. [AI] Different stages use different strategies for optional parameters — some use flat positionals only (lookup), some use mutable Options classes (bucket, merge), some use immutable interface-based options with named factories (densify, geoNear), and some delegate to sealed APIs (search). Meanwhile, Filters (used in match stages) never uses options objects. Within a single pipeline, parameter representation varies by stage.

### B4. Search API (Sealed Interface + Chained)

The Search API (`@Beta`, 4.7+) uses a distinct parameterization style within aggregation: sealed interfaces, typed field references, and immutable fluent modification (each call returns a new instance). Contrast with how similar operations are done in other stages:

```java
// A $match stage using the Filters API (B2 pattern):
// — static factory, String field name, positional params, returns Bson
Aggregates.match(
    Filters.and(
        Filters.text("mongodb"),                   // text search via Filters: just a string
        Filters.eq("status", "published")))        // equality: fieldName + value

// A $search stage using the Search API (sealed+immutable pattern):
// — static factory on sealed interface, SearchPath field ref, immutable fluent modification
Aggregates.search(
    SearchOperator.compound()                      // static factory returns sealed interface
        .must(List.of(
            SearchOperator.text(                   // text search via SearchOperator
                fieldPath("title"),                //   SearchPath instead of String fieldName
                "mongodb")
                .fuzzy()                           //   immutable fluent (returns new instance)
                .score(SearchScore.boost(1.5f))))  //   score modification (also immutable fluent)
        .filter(List.of(
            SearchOperator.equals(                 // equality via SearchOperator
                fieldPath("status"),               //   SearchPath
                "published"))),
    SearchOptions.searchOptions()                  // options via named factory (not constructor)
        .index("default"))                         //   immutable fluent chaining
```

Key differences from the B2 (Filters) pattern:
- **Field references**: `SearchPath.fieldPath("title")` vs. plain `String "title"`.
- **Immutability**: `.fuzzy()` returns a new instance; Filters methods return a final Bson.
- **Options**: `SearchOptions.searchOptions()` (named factory, immutable fluent) vs. `new XOptions()` (constructor, mutable).
- **Sealed interfaces**: cannot be implemented externally; `SearchOperator.of(Bson)` escape hatch for unsupported operators.

### B5. MQL Expressions API (Type-Safe Lazy Expressions)

The MQL Expressions API (`@Beta`, 4.9+) is an alternative to raw Bson for representing aggregation expressions. Contrast:

```java
// Without MQL — raw BSON document for a $addFields expression:
Aggregates.addFields(new Field<>("discountedTotal",
    new Document("$multiply", List.of(              // manual $multiply operator
        new Document("$sum", "$items.price"),        // manual $sum
        0.9))))

// With MQL — type-safe fluent expression for the same computation:
Aggregates.addFields(new Field<>("discountedTotal",
    current()                                        // MqlDocument: the current pipeline document
        .getArray("items")                           // → MqlArray<MqlDocument>
        .sum(item -> item.getInteger("price"))       // → MqlInteger (type guides available methods)
        .multiply(of(0.9))))                         // → MqlNumber
```

- **Sealed interface hierarchy**: `MqlValue` → `MqlBoolean`, `MqlString`, `MqlNumber` (→ `MqlInteger`), `MqlDate`, `MqlDocument`, `MqlArray<T>`, `MqlMap<T>`, `MqlEntry<T>`.
- **Created via `MqlValues`**: `of(42)` → `MqlInteger`, `of("text")` → `MqlString`, `current()` → `MqlDocument`.
- **Immutable fluent chaining**: each method returns a new instance. Return types guide available methods (e.g., `MqlString.length()` → `MqlInteger`, not `MqlNumber`).
- **Lazy**: expressions build an AST, evaluated only at BSON serialization time.
- This is the most Java-idiomatic API in the driver (closest to Streams). It can be used inside any aggregation stage that accepts an expression, but it is a fundamentally different parameterization style from the Bson-based static factories.

### B6. Operation Models (Bulk Write specifications)

Two ideas coexist for specifying individual write operations within a bulk:

```java
// Collection-level bulkWrite (older) — constructor-based WriteModel:
collection.bulkWrite(List.of(
    new InsertOneModel<>(new Document("x", 1)),                     // constructor
    new UpdateOneModel<>(eq("_id", id), set("x", 2)),              // constructor with Filters/Updates
    new DeleteOneModel<>(eq("_id", id))));                          // constructor

// Client-level bulkWrite (newer) — static factories on sealed interface:
client.bulkWrite(List.of(
    ClientNamespacedWriteModel.insertOne(                            // static factory
        ns1, new Document("x", 1)),
    ClientNamespacedWriteModel.updateOne(                            // static factory
        ns2, eq("_id", id), set("x", 2),
        ClientUpdateOneOptions.clientUpdateOneOptions()              // named factory for options (not constructor)
            .upsert(true)),
    ClientNamespacedWriteModel.deleteOne(ns1, eq("_id", id))));     // static factory
```

The evolution from constructors to static factories on sealed interfaces is visible here. Per-operation options in the newer API use named factories (`ClientUpdateOneOptions.clientUpdateOneOptions()`), unlike the older options which use constructors (`new UpdateOptions()`).

## C. Operation Flow and Execution

### C1. Overload Matrix on Core Interfaces

MongoCollection, MongoDatabase, MongoCluster, and GridFSBucket expose operations as method overloads along systematic axes:

```java
// The four overloads of insertOne (×session ×options):
collection.insertOne(document);
collection.insertOne(document, new InsertOneOptions());
collection.insertOne(session, document);
collection.insertOne(session, document, new InsertOneOptions());
```

Axes: **ClientSession** (always first if present) × **Options** (always last if present). Some operations add **result class**, **update type** (Bson vs pipeline), or **key type** (String vs Bson). This yields ~120+ overloads on MongoCollection.

Concerns:
1. [AI] Search index methods (createSearchIndex, updateSearchIndex, dropSearchIndex) lack ClientSession overloads.
2. [AI] `estimatedDocumentCount` has no ClientSession overloads (server constraint).
3. [AI] `distinct` requires `resultClass` — no convenience overload using the collection's document type (unlike `find()`).
4. [AI] `listCollectionNames` returns a dedicated `ListCollectionNamesIterable`; `listDatabaseNames` returns generic `MongoIterable<String>`.
5. [AI] GridFS uses type-based overloading for downloads (`openDownloadStream(String)` vs `openDownloadStream(ObjectId)` vs `openDownloadStream(BsonValue)`), consistent with the rest of the API. However, it has redundant `ObjectId`/`BsonValue` overloads across all id-accepting methods (download, delete, rename).

### C2. Reactive Streams / Async

The reactive module (`com.mongodb.reactivestreams.client`) is a near-exact structural mirror of the sync API:

```java
// Sync
InsertOneResult result = collection.insertOne(doc);
for (Document d : collection.find(filter)) { process(d); }

// Reactive — same structure, Publisher<T> wrapping
Publisher<InsertOneResult> pub = collection.insertOne(doc);
collection.find(filter).subscribe(subscriber);
```

- Same interface names (MongoClient, MongoDatabase, MongoCollection), different package.
- Same overloads, same options objects, same model classes.
- Specialized publishers (FindPublisher, AggregatePublisher, etc.) mirror the fluent iterables exactly.
- Internally backed by callback-based `SingleResultCallback<T>` → bridged via Project Reactor `Mono<T>`/`Flux<T>` → exposed as `Publisher<T>`. The callback layer is not public API.
- GridFS also has a reactive variant (GridFSBucket, GridFSUploadPublisher, GridFSDownloadPublisher).

### C3. Results

**Older result classes** — abstract class + static factory:
```java
InsertOneResult.acknowledged(insertedId)    // or .unacknowledged()
UpdateResult.acknowledged(matched, modified, upsertedId)
DeleteResult.acknowledged(deletedCount)
```

**Newer result classes** (client bulk write) — sealed interfaces:
```java
ClientBulkWriteResult result = client.bulkWrite(writes);
result.getInsertedCount();
result.getInsertResults();  // Map<Integer, ClientInsertOneResult>
```

Both are immutable. The evolution from abstract classes to sealed interfaces parallels WriteModel → ClientNamespacedWriteModel.


### AI Research Notes

Overloads were counted from the sync MongoCollection interface. The reactive-streams MongoCollection mirrors this exactly. Deprecated methods (mapReduce) are excluded. All public API files in driver-sync, driver-reactive-streams, and driver-core were examined, including: MongoCluster, MongoDatabase, MongoCollection; all iterables and publishers; Aggregates, Filters, Projections, Sorts, Updates, Accumulators, Indexes, Windows; the search and bulk packages; MongoClientSettings and all connection settings; all Options classes; ReadPreference, WriteConcern, ReadConcern, MongoCredential; GridFSBucket; WriteModel hierarchy; result classes; event/listener interfaces.

