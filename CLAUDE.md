# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

---

## Architecture

### Module dependency order

```
bson  →  driver-core  →  driver-sync / driver-reactive-streams  →  language wrappers
```

When a change touches more than one module, apply it lowest-first (bson before driver-core,
driver-core before driver-sync). Public API lives in driver-sync, driver-reactive-streams, and
the language wrappers. driver-core owns all internals.

### Connection string → cluster creation pipeline

This is the critical path to understand before modifying any connection, cluster, or settings code.

```
ConnectionString                     driver-core: com.mongodb.ConnectionString
  └─ MongoClientSettings.builder()
       .applyConnectionString()      driver-core: com.mongodb.MongoClientSettings
  └─ ClusterSettings.Builder
       .applyConnectionString()      driver-core: com.mongodb.connection.ClusterSettings
         sets ClusterConnectionMode:
           LOAD_BALANCED             loadBalanced=true in URI
           MULTIPLE + srvHost set    mongodb+srv://
           SINGLE                    directConnection=true
           MULTIPLE                  directConnection=false or multiple hosts
  └─ MongoClients.create()           driver-sync: com.mongodb.client.MongoClients
  └─ Clusters.createCluster()        driver-sync: com.mongodb.client.internal.Clusters
  └─ DefaultClusterFactory
       .createCluster()              driver-core: com.mongodb.internal.connection.DefaultClusterFactory
         → LoadBalancedCluster       (LOAD_BALANCED)
         → SingleServerCluster       (SINGLE)
         → MultiServerCluster        (MULTIPLE, no srvHost)
         → DnsMultiServerCluster     (MULTIPLE + srvHost — resolves SRV DNS)
```

`ConnectionString` stores `isSrvProtocol` (bool) and the parsed host list.
`mongodb+srv://` auto-enables TLS and resolves TXT records for extra options.
Cluster environment detection (CosmosDB, DocumentDB) is a logging-only heuristic in
`DefaultClusterFactory.ClusterEnvironment` — it does not affect topology.

### How operations execute (the binding layer)

driver-core is async-first. Sync execution in driver-sync wraps every async operation.

```
MongoCollectionImpl / MongoDatabaseImpl / MongoClusterImpl   (driver-sync internal)
  └─ executeOperation(ReadOperation / WriteOperation)
       └─ ClusterBinding                  binds a Cluster to a ReadPreference
            └─ ConnectionSource           selects a server, provides a Connection
                 └─ CommandProtocolImpl   serializes + sends the wire-protocol message
                      └─ Stream (Socket / Netty / TlsChannel)
```

Key classes:
- `com.mongodb.internal.binding.ClusterBinding` — wraps a `Cluster` for sync execution
- `com.mongodb.internal.connection.Cluster` (interface) — server selection + connection checkout
- `com.mongodb.internal.operation.*` — all concrete operations (FindOperation, AggregateOperation, …)
- `com.mongodb.internal.connection.CommandProtocolImpl` — wire protocol serialization

### Transport / stream selection

`StreamFactoryHelper` selects the I/O backend from `TransportSettings`:

| TransportSettings | Sync backend | Async backend |
|---|---|---|
| `null` (default) | `SocketStreamFactory` | `AsynchronousSocketChannelStreamFactoryFactory` or `TlsChannelStreamFactoryFactory` (when TLS) |
| `NettyTransportSettings` | `NettyStreamFactoryFactory` | `NettyStreamFactoryFactory` |
| `AsyncTransportSettings` | throws | `AsynchronousSocketChannelStreamFactoryFactory` |

### Running a single test class

```bash
./gradlew :driver-core:test --tests "com.mongodb.internal.operation.FindOperationTest"
./gradlew :driver-sync:test --tests "com.mongodb.client.MongoClientSpecification"
./gradlew test -PjavaVersion=11   # run against a different JDK
```

### Scala tests

```bash
./gradlew scalaCheck   # tests against all supported Scala versions (2.11 / 2.12 / 2.13 / 3)
```

### Javadoc validation

```bash
./gradlew docs   # fails if any public API is missing Javadoc
```

### Key conventions not obvious from the code

- **Sync wraps async, not the other way around.** `MongoClusterImpl.executeOperation()` drives async
  operations synchronously via a blocking `SingleResultCallback`. Never add logic to the sync path
  that cannot be mirrored in the async path.
- **`com.mongodb.internal.*` is private API.** Never reference internal packages from public-API
  classes, and never expose internal types through public method signatures.
- **New public packages must declare `@NonNullApi`** in `package-info.java` to opt into non-null by
  default. In older packages without it, every parameter/return is implicitly nullable unless
  annotated `@NonNull`.
- **`Locks.withLock()`** (not `synchronized`) is the required idiom for locking in async connection
  code to avoid deadlocks with the driver's internal thread model.
- **All dependency versions live in `gradle/libs.versions.toml`.** Never hard-code a version string
  in a `build.gradle.kts` file.
