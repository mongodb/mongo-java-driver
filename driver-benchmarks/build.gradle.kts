/*
 * Copyright 2016-present MongoDB, Inc.
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

plugins {
    id("application")
    id("java-library")
    id("project.base")
}

application {
    mainClass = "com.mongodb.benchmark.benchmarks.BenchmarkSuite"
    applicationDefaultJvmArgs = listOf(
        "-Dorg.mongodb.benchmarks.data=${System.getProperty("org.mongodb.benchmarks.data")}",
        "-Dorg.mongodb.benchmarks.output=${System.getProperty("org.mongodb.benchmarks.output")}")
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/resources")) }
    }
}

dependencies {
    api(project(":driver-sync"))
    api(project(":driver-reactive-streams"))
    api(project(":mongodb-crypt"))

    // OpenTelemetry overhead suite (com.mongodb.benchmark.benchmarks.otel): the harness, not the driver,
    // installs the OpenTelemetry SDK and sampler, as required by the OpenTelemetry spec's Benchmarking section.
    implementation(platform(libs.micrometer.observation.bom))
    implementation(libs.micrometer.observation)
    implementation(platform(libs.micrometer.tracing.integration.test.bom))
    implementation(libs.micrometer.tracing.bridge.otel)
    implementation(platform(libs.opentelemetry.bom))
    implementation(libs.opentelemetry.sdk.trace)
    implementation(platform(libs.project.reactor.bom))
    implementation(libs.project.reactor.core)

    implementation(platform(libs.netty.bom))
    implementation(libs.bundles.netty)

    implementation(libs.logback.classic)
    implementation(libs.jmh.core)
    annotationProcessor(libs.jmh.generator.annprocess)

}

tasks.register<JavaExec>("jmh") {
    group = "benchmark"
    description = "Run JMH benchmarks."
    mainClass = "org.openjdk.jmh.Main"
    classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("runNetty") {
    group = "application"
    description = "Run the Netty main class."
    mainClass.set("com.mongodb.benchmark.benchmarks.netty.BenchmarkNettyProviderSuite")
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs.toList()
}

tasks.register<JavaExec>("runOtel") {
    group = "benchmark"
    description = "Run the OpenTelemetry overhead suite (DRIVERS-3620) for one tracing configuration."
    mainClass.set("com.mongodb.benchmark.benchmarks.otel.OtelBenchmarkSuite")
    classpath = sourceSets["main"].runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs.toList() +
        (System.getProperty("org.mongodb.benchmarks.jvmArgs")?.split(" ")?.filter { it.isNotBlank() } ?: emptyList())
    systemProperties(System.getProperties().filterKeys { it.toString().startsWith("org.mongodb.benchmarks.") }
        .mapKeys { it.key.toString() })
}

tasks.withType<Javadoc>().configureEach {
    enabled = false
}
