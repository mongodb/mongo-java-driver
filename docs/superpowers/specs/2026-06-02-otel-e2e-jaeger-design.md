# Design: End-to-end OTel trace visualization (driver toggle + Spring Boot + Jaeger)

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454) — trace context propagation to the server
- **Status:** Design
- **Author:** Nabil Hachicha
- **Date:** 2026-06-02
- **Builds on:** `docs/superpowers/specs/2026-06-01-otel-opmsg-propagation-design.md` (the OP_MSG kind-3 POC, already staged) and the running server POC (`~/MongoDB/otel-poc-server/`, mongod `gitVersion b2cb2bf`).

---

## 1. Goal

Make a client→server trace **visible end to end in Jaeger**: a Spring Boot app issues a MongoDB
operation; the driver propagates its sampled W3C `traceparent` to the server via the OP_MSG kind-3
section; the server starts a child span; both client and server spans export to one Jaeger instance
and appear under a single trace.

The POC server does **not** advertise `tracingSupport` in `hello`, so the driver (which gates on
that capability) needs a temporary, removable switch to send the section anyway.

## 2. Components

1. **Driver test-only force toggle** — bypasses the server-capability gate only.
2. **Local Maven publish** — `5.9.0-SNAPSHOT` consumable by the app.
3. **Jaeger** (Docker) — single OTLP collector + UI.
4. **mongod re-config** — export server spans to Jaeger.
5. **Spring Boot app** (host JVM) — Spring Data MongoDB + Micrometer→OTel→Jaeger, wired to the
   driver's internal tracing, with a REST trigger.

---

## 3. Driver: force-propagation toggle

New file `driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java`:

```java
public final class OtelTracePropagationTestToggle {
    // TEST-ONLY (DRIVERS-3454): force sending the OP_MSG OTel trace-context section even when the
    // server did not advertise tracingSupport in hello. Remove before any production use.
    public static volatile boolean FORCE_PROPAGATION = false;

    private OtelTracePropagationTestToggle() {
    }
}
```

`CommandMessage.writeOtelTraceContextSection` gate changes from:

```java
if (!getSettings().isTracingSupported()) {
    return;
}
```

to:

```java
if (!getSettings().isTracingSupported() && !OtelTracePropagationTestToggle.FORCE_PROPAGATION) {
    return;
}
```

The remaining gates are unchanged: a `null` tracing span or a `null`/unsampled `traceParent()` still
omits the section. So the toggle only relaxes the capability check.

**Test** (`CommandMessageOtelTraceContextTest`): new case — `tracingSupported=false` +
`FORCE_PROPAGATION=true` + sampled span ⇒ section IS written. Set/reset `FORCE_PROPAGATION` in a
`try/finally` so the static does not leak into other tests.

**Removal path:** delete the toggle class and revert the one `&&` clause.

## 4. Publish to Maven Local

From the repo root:

```bash
./gradlew publishToMavenLocal -PskipCryptVerify=true
```

Publishes all modules (`bson`, `bson-record-codec`, `driver-core`, `driver-sync`, …) as
`org.mongodb:*:5.9.0-SNAPSHOT`. The app consumes `5.9.0-SNAPSHOT` from `mavenLocal()`.

## 5. Jaeger (Docker)

`jaegertracing/all-in-one:1.62.0` on a dedicated network `otel-poc-net`, OTLP enabled, host ports:

| Port | Purpose |
|------|---------|
| 16686 | Jaeger UI |
| 4317 | OTLP gRPC |
| 4318 | OTLP HTTP |

Run:

```bash
docker network create otel-poc-net 2>/dev/null || true
docker run -d --name jaeger --network otel-poc-net \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/all-in-one:1.62.0
```

## 6. mongod: export server spans to Jaeger

Restart the existing POC mongod container **on `otel-poc-net`** so it can reach Jaeger by name, and
point its OTLP exporter at Jaeger:

```bash
docker rm -f otel-poc-mongod-run
docker run -d --name otel-poc-mongod-run --platform linux/arm64 --network otel-poc-net \
  -v "$HOME/MongoDB/otel-poc-server/dist-test:/opt/mongo:ro" \
  -v "$HOME/MongoDB/otel-poc-server/data:/data/db" \
  -p 27017:27017 \
  otel-poc-mongod \
  /opt/mongo/bin/mongod --dbpath /data/db --bind_ip_all --port 27017 \
    --setParameter opentelemetryHttpEndpoint=http://jaeger:4318/v1/traces \
    --setParameter openTelemetryExportIntervalMillis=1000
```

`featureFlagTracing` is already enabled in this build. **Verification & fallback:** confirm via
server logs that the OTLP endpoint is accepted and exports succeed; if the exact endpoint form is
rejected, fall back to the already-working file exporter
(`opentelemetryTraceDirectory=/data/db/otel-traces`) and note that server spans are then inspected as
JSONL rather than in the Jaeger UI.

## 7. Spring Boot app (host JVM)

Maven project at `~/MongoDB/otel-poc-server/otel-poc-client/` — kept outside the driver repo so it is
not entangled with the driver build.

**Stack:** Spring Boot 3.3.x, Java 17, Maven wrapper.

**Dependencies:** `spring-boot-starter-web`, `spring-boot-starter-data-mongodb`,
`spring-boot-starter-actuator`, `micrometer-tracing-bridge-otel`, `opentelemetry-exporter-otlp`.
`pom.xml` adds `mavenLocal()` (via a `<repository>`) and overrides `<mongodb.version>5.9.0-SNAPSHOT</mongodb.version>`.

**Critical wiring bean** — activates the driver's *internal* tracing (the path that sets
`operationContext.tracingSpan`, which `traceParent()` reads):

```java
@Bean
MongoClientSettingsBuilderCustomizer tracingCustomizer(ObservationRegistry registry) {
    return builder -> builder.observabilitySettings(
            MicrometerObservabilitySettings.builder()
                    .observationRegistry(registry)
                    .build());
}
```

**Toggle activation** — set before any operation runs:

```java
@PostConstruct
void enableForcedPropagation() {
    OtelTracePropagationTestToggle.FORCE_PROPAGATION = true;
}
```

**Trigger** — a `@RestController`:

```java
@GetMapping("/ping")
String ping() {
    mongoTemplate.getCollection("ping").insertOne(new Document("at", new Date()));
    long n = mongoTemplate.getCollection("ping").countDocuments();
    return "ok, count=" + n;
}
```

Hitting `/ping` creates an HTTP server span (root, sampled), under which the driver creates a mongo
command span (exported to Jaeger; its context is the propagated `traceparent`), and the server
creates its child span (exported to Jaeger).

**`application.properties`:**

```properties
spring.application.name=otel-poc-client
spring.data.mongodb.uri=mongodb://localhost:27017/test
management.tracing.sampling.probability=1.0
management.otlp.tracing.endpoint=http://localhost:4318/v1/traces
# Suppress Spring's auto Mongo command instrumentation so the driver's internal tracer is the
# ONLY source of Mongo command spans (avoids duplicate/competing spans).
management.metrics.enable.mongodb=false
spring.autoconfigure.exclude=org.springframework.boot.actuate.autoconfigure.metrics.mongo.MongoMetricsAutoConfiguration
```

**Suppressing Spring's auto Mongo instrumentation (firm decision).** Spring Boot's actuator
auto-registers a Mongo command listener via `MongoMetricsAutoConfiguration`. To guarantee the
driver's own internal tracer is the single source of Mongo command spans (and the one performing
propagation), we **exclude** `org.springframework.boot.actuate.autoconfigure.metrics.mongo.MongoMetricsAutoConfiguration`
and set `management.metrics.enable.mongodb=false`. Only our `MongoClientSettingsBuilderCustomizer`
(§7 wiring) then contributes Mongo observability.

## 8. End-to-end run & verification

1. Start Jaeger (§5). 2. Restart mongod on the network with OTLP (§6). 3. Publish the driver (§4).
4. `./mvnw spring-boot:run` (§7). 5. `curl localhost:8080/ping`. 6. Open **http://localhost:16686**,
select service `otel-poc-client`, open the latest trace.

**Pass criteria:** one trace contains the HTTP span, the driver mongo command span, **and** a
`mongod` server span; the server span's `traceId` equals the client trace, and its parent is the
client mongo span's id. Negative check: with `FORCE_PROPAGATION=false`, the server span is absent
from the client trace (server starts its own unrelated trace, if any).

## 9. Scope guards

- Test-only toggle; not a public API; removed before any real merge.
- Single mongod (no replica set/sharding), `find`/`insert` only.
- No auth/TLS on the local mongod.
- The app lives outside the driver repo; it is not committed to the driver repo.

## 10. Open questions / risks

- Exact accepted form of `opentelemetryHttpEndpoint` (full `/v1/traces` URL vs base) — verify via
  logs; file-exporter fallback documented (§6).
- (Resolved) Spring's auto Mongo instrumentation is suppressed via autoconfigure-exclude + metric disable (§7).
- Spring Boot 3.3.x pins an older driver; overriding `mongodb.version` to `5.9.0-SNAPSHOT` assumes API
  compatibility (it is, the API is unchanged by this POC).
