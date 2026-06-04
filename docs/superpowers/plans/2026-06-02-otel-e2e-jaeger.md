# End-to-end OTel Trace Visualization (driver toggle + Spring Boot + Jaeger) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** See a single client→server trace in Jaeger: a Spring Boot app's MongoDB operation, the driver's propagated `traceparent` (forced on via a test toggle), and the POC server's child span, all exported to one Jaeger instance.

**Architecture:** Add a test-only static toggle in the driver to bypass the server-capability gate; publish the driver to Maven Local; run Jaeger in Docker; reconnect the POC mongod to export OTLP to Jaeger; build a Spring Boot app (Spring Data MongoDB + Micrometer→OTel→Jaeger) that wires the driver's internal tracing via `MongoClientSettingsBuilderCustomizer`, flips the toggle, and exposes a REST trigger.

**Tech Stack:** Java 8 driver-core (toggle), Gradle (publish), Docker (Jaeger + mongod), Spring Boot 3.3.x / Java 17 / Maven (app), Micrometer Tracing + OpenTelemetry OTLP.

**Spec:** `docs/superpowers/specs/2026-06-02-otel-e2e-jaeger-design.md`

> **Standing instruction:** the user wants driver changes STAGED, not committed. For driver tasks, replace any `git commit` with `git add` (stage only). The Spring Boot app lives OUTSIDE the driver repo (`~/MongoDB/otel-poc-server/otel-poc-client/`) and is not added to driver git at all.

> **Environment facts (already true):**
> - POC mongod binaries extracted at `~/MongoDB/otel-poc-server/dist-test/`; Docker image `otel-poc-mongod` built; container `otel-poc-mongod-run` currently running on the default bridge with port 27017.
> - `featureFlagTracing` is enabled in that build; `opentelemetryHttpEndpoint` is a startup setParameter.
> - Driver OP_MSG kind-3 propagation POC is already staged (gate lives in `CommandMessage.writeOtelTraceContextSection`).

---

## File map

| File | Responsibility | Change |
|---|---|---|
| `driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java` | Test-only force switch | Create |
| `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java` | OP_MSG encoding gate | Modify (one clause) |
| `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java` | Toggle behavior test | Modify (add 1 test) |
| `~/MongoDB/otel-poc-server/otel-poc-client/pom.xml` | App build, driver override, mavenLocal | Create |
| `~/MongoDB/otel-poc-server/otel-poc-client/src/main/java/com/example/otelpoc/OtelPocApplication.java` | Main + toggle activation | Create |
| `~/MongoDB/otel-poc-server/otel-poc-client/src/main/java/com/example/otelpoc/MongoTracingConfig.java` | `MongoClientSettingsBuilderCustomizer` wiring | Create |
| `~/MongoDB/otel-poc-server/otel-poc-client/src/main/java/com/example/otelpoc/PingController.java` | REST trigger | Create |
| `~/MongoDB/otel-poc-server/otel-poc-client/src/main/resources/application.properties` | URI, sampling, OTLP endpoint | Create |

---

## Task 1: Driver test-only force toggle

**Files:**
- Create: `driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java`
- Modify: `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java`
- Test: `driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java`

- [ ] **Step 1: Write the failing test**

Add this method to `CommandMessageOtelTraceContextTest` (it reuses the existing `buildCommandMessage`, `buildOperationContext`, `encodeToBytes`, `containsOtelSection`, and `TRACEPARENT` helpers/constants already in the class). Add the import `import com.mongodb.internal.observability.micrometer.OtelTracePropagationTestToggle;` at the top.

```java
    @Test
    void writesSectionWhenForcedEvenIfCapabilityAbsent() {
        OtelTracePropagationTestToggle.FORCE_PROPAGATION = true;
        try {
            CommandMessage message = buildCommandMessage(false); // server did NOT advertise tracingSupport
            TraceContext traceContext = () -> TRACEPARENT;
            Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
            OperationContext operationContext = buildOperationContext(span);

            byte[] encoded = encodeToBytes(message, operationContext);

            assertTrue(containsOtelSection(encoded, TRACEPARENT),
                    "With FORCE_PROPAGATION the section must be sent even when the server did not advertise support");
        } finally {
            OtelTracePropagationTestToggle.FORCE_PROPAGATION = false;
        }
    }
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.CommandMessageOtelTraceContextTest.writesSectionWhenForcedEvenIfCapabilityAbsent" -PskipCryptVerify=true`
Expected: FAIL — `OtelTracePropagationTestToggle` does not exist.

- [ ] **Step 3: Create the toggle class**

Create `driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java`:

```java
/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.observability.micrometer;

/**
 * TEST-ONLY switch (DRIVERS-3454): when {@code true}, the driver writes the OP_MSG OpenTelemetry
 * trace-context section even if the server did not advertise {@code tracingSupport} in its
 * {@code hello} response. The sampled-{@code traceparent} requirement still applies.
 *
 * <p>This exists only to exercise end-to-end propagation against a server that does not yet advertise
 * the capability. Remove before any production use.</p>
 */
public final class OtelTracePropagationTestToggle {
    public static volatile boolean FORCE_PROPAGATION = false;

    private OtelTracePropagationTestToggle() {
    }
}
```

- [ ] **Step 4: Relax the capability gate in CommandMessage**

In `driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java`, add the import (alphabetically within the `com.mongodb.internal.observability.micrometer` group, next to the existing `Span` import):

```java
import com.mongodb.internal.observability.micrometer.OtelTracePropagationTestToggle;
```

In `writeOtelTraceContextSection`, change the first guard from:

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

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.connection.CommandMessageOtelTraceContextTest" -PskipCryptVerify=true`
Expected: PASS (all cases, including the new one and the existing `omitsSectionWhenCapabilityAbsent` which does NOT set the toggle, so it still passes).

- [ ] **Step 6: Stage (do NOT commit)**

```bash
git add driver-core/src/main/com/mongodb/internal/observability/micrometer/OtelTracePropagationTestToggle.java \
        driver-core/src/main/com/mongodb/internal/connection/CommandMessage.java \
        driver-core/src/test/unit/com/mongodb/internal/connection/CommandMessageOtelTraceContextTest.java
```

---

## Task 2: Publish the driver to Maven Local

**Files:** none (build action)

- [ ] **Step 1: Publish all modules as 5.9.0-SNAPSHOT**

Run from the repo root:
```bash
./gradlew publishToMavenLocal -PskipCryptVerify=true
```
Expected: BUILD SUCCESSFUL.

- [ ] **Step 2: Verify the artifacts landed in the local repo**

Run:
```bash
ls ~/.m2/repository/org/mongodb/mongodb-driver-sync/5.9.0-SNAPSHOT/ \
   ~/.m2/repository/org/mongodb/mongodb-driver-core/5.9.0-SNAPSHOT/ \
   ~/.m2/repository/org/mongodb/bson/5.9.0-SNAPSHOT/
```
Expected: each directory contains a `*-5.9.0-SNAPSHOT.jar`. If `bson-record-codec` is referenced transitively, confirm it too:
```bash
ls ~/.m2/repository/org/mongodb/bson-record-codec/5.9.0-SNAPSHOT/ 2>/dev/null || echo "(bson-record-codec not published — fine unless the app fails to resolve it)"
```

---

## Task 3: Start Jaeger in Docker

**Files:** none (infra)

- [ ] **Step 1: Create the shared network and run Jaeger**

```bash
docker network create otel-poc-net 2>/dev/null || true
docker rm -f jaeger 2>/dev/null || true
docker run -d --name jaeger --network otel-poc-net \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/all-in-one:1.62.0
```

- [ ] **Step 2: Verify the UI is up**

```bash
until curl -sf http://localhost:16686/ >/dev/null; do sleep 1; done; echo "Jaeger UI reachable"
```
Expected: `Jaeger UI reachable`.

---

## Task 4: Reconnect mongod to export OTLP to Jaeger

**Files:** none (infra)

- [ ] **Step 1: Recreate the mongod container on the shared network with the OTLP endpoint**

```bash
cd ~/MongoDB/otel-poc-server
docker rm -f otel-poc-mongod-run 2>/dev/null || true
docker run -d --name otel-poc-mongod-run --platform linux/arm64 --network otel-poc-net \
  -v "$PWD/dist-test:/opt/mongo:ro" \
  -v "$PWD/data:/data/db" \
  -p 27017:27017 \
  otel-poc-mongod \
  /opt/mongo/bin/mongod --dbpath /data/db --bind_ip_all --port 27017 \
    --setParameter opentelemetryHttpEndpoint=http://jaeger:4318/v1/traces \
    --setParameter openTelemetryExportIntervalMillis=1000
```

- [ ] **Step 2: Wait for readiness and confirm the endpoint was accepted**

```bash
until docker logs otel-poc-mongod-run 2>&1 | grep -q "Waiting for connections" || ! docker ps -q --filter name=otel-poc-mongod-run | grep -q .; do sleep 1; done
docker ps --filter name=otel-poc-mongod-run --format '{{.Status}}'
docker logs otel-poc-mongod-run 2>&1 | grep -iE "opentelemetry|otel|trace|export|endpoint" | tail -15
```
Expected: container `Up`; no fatal OTLP/endpoint parse error in logs.

- [ ] **Step 3: Fallback if the endpoint form is rejected**

If mongod refuses to start or logs an OTLP endpoint error, re-run Step 1 replacing the endpoint with the file exporter (already proven working):
```
    --setParameter opentelemetryTraceDirectory=/data/db/otel-traces
```
and note in the final report that server spans are inspected as JSONL under `~/MongoDB/otel-poc-server/data/otel-traces/` rather than in the Jaeger UI. Then continue.

---

## Task 5: Scaffold the Spring Boot app (pom + properties + main)

**Files (all under `~/MongoDB/otel-poc-server/otel-poc-client/`, OUTSIDE the driver repo):**
- Create: `pom.xml`
- Create: `src/main/resources/application.properties`
- Create: `src/main/java/com/example/otelpoc/OtelPocApplication.java`

- [ ] **Step 1: Create the project directory and Maven wrapper**

```bash
mkdir -p ~/MongoDB/otel-poc-server/otel-poc-client/src/main/java/com/example/otelpoc
mkdir -p ~/MongoDB/otel-poc-server/otel-poc-client/src/main/resources
```

- [ ] **Step 2: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.5</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>otel-poc-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>

    <properties>
        <java.version>17</java.version>
        <!-- consume the locally published driver POC -->
        <mongodb.version>5.9.0-SNAPSHOT</mongodb.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-mongodb</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-tracing-bridge-otel</artifactId>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

> Maven consults the local `~/.m2` repository by default, so the `5.9.0-SNAPSHOT` artifacts published in Task 2 resolve without any extra `<repository>` entry.

- [ ] **Step 3: Create `src/main/resources/application.properties`**

```properties
spring.application.name=otel-poc-client
server.port=8080
spring.data.mongodb.uri=mongodb://localhost:27017/test
management.tracing.sampling.probability=1.0
management.otlp.tracing.endpoint=http://localhost:4318/v1/traces
# Suppress Spring's auto Mongo command instrumentation: the driver's internal tracer is the ONLY
# source of Mongo command spans (no duplicate/competing spans).
management.metrics.enable.mongodb=false
spring.autoconfigure.exclude=org.springframework.boot.actuate.autoconfigure.metrics.mongo.MongoMetricsAutoConfiguration
```

- [ ] **Step 4: Create the main class with toggle activation**

`src/main/java/com/example/otelpoc/OtelPocApplication.java`:

```java
package com.example.otelpoc;

import com.mongodb.internal.observability.micrometer.OtelTracePropagationTestToggle;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OtelPocApplication {
    public static void main(String[] args) {
        // TEST-ONLY: force the driver to send the OP_MSG trace-context section even though this POC
        // server does not advertise tracingSupport in hello. Set before any MongoClient is used.
        OtelTracePropagationTestToggle.FORCE_PROPAGATION = true;
        SpringApplication.run(OtelPocApplication.class, args);
    }
}
```

- [ ] **Step 5: Generate the Maven wrapper**

```bash
cd ~/MongoDB/otel-poc-server/otel-poc-client
mvn -N wrapper:wrapper -Dmaven=3.9.9 2>&1 | tail -3 || echo "(if 'mvn' is absent, install it or run with a system Maven in Task 7)"
```
Expected: `mvnw` and `.mvn/` created. If no system `mvn` exists, skip — Task 7 notes the alternative.

---

## Task 6: Tracing wiring bean + REST trigger

**Files (under `~/MongoDB/otel-poc-server/otel-poc-client/src/main/java/com/example/otelpoc/`):**
- Create: `MongoTracingConfig.java`
- Create: `PingController.java`

- [ ] **Step 1: Create the `MongoClientSettingsBuilderCustomizer` wiring**

`MongoTracingConfig.java`:

```java
package com.example.otelpoc;

import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoTracingConfig {

    /**
     * Activates the driver's INTERNAL tracing (the path that sets operationContext.tracingSpan, which
     * traceParent() reads for OP_MSG propagation) and routes its spans through Spring's OTel-bridged
     * ObservationRegistry so they export to Jaeger.
     */
    @Bean
    public MongoClientSettingsBuilderCustomizer tracingCustomizer(final ObservationRegistry registry) {
        return builder -> builder.observabilitySettings(
                MicrometerObservabilitySettings.builder()
                        .observationRegistry(registry)
                        .build());
    }
}
```

- [ ] **Step 2: Create the REST trigger**

`PingController.java`:

```java
package com.example.otelpoc;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class PingController {

    private final MongoTemplate mongoTemplate;

    public PingController(final MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/ping")
    public String ping() {
        mongoTemplate.getCollection("ping").insertOne(new Document("at", new Date()));
        long count = mongoTemplate.getCollection("ping").countDocuments();
        return "ok, count=" + count + "\n";
    }
}
```

- [ ] **Step 3: Compile the app to verify wiring resolves**

```bash
cd ~/MongoDB/otel-poc-server/otel-poc-client
./mvnw -q -DskipTests compile 2>&1 | tail -20 || mvn -q -DskipTests compile 2>&1 | tail -20
```
Expected: BUILD SUCCESS. If compilation fails to resolve `MicrometerObservabilitySettings` or `OtelTracePropagationTestToggle`, confirm Task 2 published `mongodb-driver-core` (the toggle and settings live there) and that `mongodb.version` is `5.9.0-SNAPSHOT`.

---

## Task 7: End-to-end run and Jaeger verification

**Files:** none (run + verify). Prereqs: Tasks 1–6 done; Jaeger (Task 3) and mongod-on-network (Task 4) running.

- [ ] **Step 1: Start the Spring Boot app**

```bash
cd ~/MongoDB/otel-poc-server/otel-poc-client
./mvnw spring-boot:run 2>&1 | tee /tmp/otel-poc-client.log &
# wait until it is listening on 8080
until curl -sf http://localhost:8080/ping >/dev/null 2>&1; do sleep 2; done
echo "app up"
```
(If there is no `mvnw`, use `mvn spring-boot:run`.) Expected: `app up`.

- [ ] **Step 2: Generate a traced operation**

```bash
curl -s http://localhost:8080/ping
```
Expected: `ok, count=<n>`.

- [ ] **Step 3: Confirm the client exported a trace to Jaeger**

```bash
sleep 3
curl -s "http://localhost:16686/api/services" | tr ',' '\n' | grep -i "otel-poc-client" && echo "client service present in Jaeger"
```
Expected: `otel-poc-client` appears in the services list.

- [ ] **Step 4: Confirm a linked server span exists in the same trace**

```bash
TRACE_JSON=$(curl -s "http://localhost:16686/api/traces?service=otel-poc-client&limit=1")
echo "$TRACE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); \
spans=d['data'][0]['spans']; procs=d['data'][0]['processes']; \
print('services in trace:', sorted({procs[s['processID']]['serviceName'] for s in spans})); \
print('span names:', [s['operationName'] for s in spans])"
```
Expected: the services set includes BOTH `otel-poc-client` and the mongod service (e.g. `mongod`), proving the client span and the server span share one trace.

- [ ] **Step 5: Visual confirmation**

Open **http://localhost:16686**, select service `otel-poc-client`, open the most recent trace. Confirm a span tree: HTTP `GET /ping` → driver mongo command span → `mongod` server span (same traceId; server span's parent is the client mongo span).

- [ ] **Step 6: Negative check (capability gate still works)**

Stop the app, set the toggle off by editing `OtelPocApplication.main` to `FORCE_PROPAGATION = false` (or comment the line), `./mvnw spring-boot:run` again, `curl /ping`, and confirm in Jaeger the new client trace has **no** `mongod` span (server starts its own unrelated trace). Restore the line afterward.

- [ ] **Step 7: Record the result**

If server spans appear in Jaeger: success — note it. If Task 4 used the file-exporter fallback, instead show the server span and matching traceId from the latest `~/MongoDB/otel-poc-server/data/otel-traces/*.jsonl` (grep for the client's traceId) and note that server spans are file-based rather than in the Jaeger UI.

---

## Self-Review

**Spec coverage:**
- §3 toggle → Task 1 (class + gate + test). §4 publish → Task 2. §5 Jaeger → Task 3. §6 mongod OTLP (+ file fallback) → Task 4 (Steps 1–3). §7 app: deps/version override → Task 5 Step 2; properties → Task 5 Step 3; toggle activation → Task 5 Step 4; `MongoClientSettingsBuilderCustomizer` wiring → Task 6 Step 1; REST trigger → Task 6 Step 2. §8 run & verification → Task 7. §10 risks: OTLP endpoint form → Task 4 Step 3 fallback; duplicate Spring command spans → acceptable, see note below; driver-version override → Task 5 Step 2 + Task 6 Step 3 troubleshooting.

**Duplicate-span suppression (spec §7, firm):** Spring's auto Mongo command instrumentation is suppressed in `application.properties` (Task 5 Step 3) via `spring.autoconfigure.exclude=…MongoMetricsAutoConfiguration` + `management.metrics.enable.mongodb=false`, so the driver's internal tracer is the only source of Mongo command spans. If startup logs show the excluded class still contributing (version drift), the implementer confirms the exact actuator Mongo autoconfig class for Spring Boot 3.3.5 and excludes that instead.

**Placeholder scan:** no TBD/TODO; every code/file step has full content; the only conditional is the documented OTLP-endpoint fallback (Task 4 Step 3) and the no-`mvnw` alternative (system `mvn`).

**Type/name consistency:** `OtelTracePropagationTestToggle.FORCE_PROPAGATION` (Tasks 1, 5); `MicrometerObservabilitySettings.builder().observationRegistry(...)` and `.observabilitySettings(...)` (Task 6, matches driver API verified in spec §7); package `com.example.otelpoc` and class names match the file map; `mongodb.version=5.9.0-SNAPSHOT` consistent (Tasks 2, 5, 6).

**Staging vs commit:** Task 1 stages only (driver repo); Tasks 5–6 create files outside the driver repo (never added to driver git).
