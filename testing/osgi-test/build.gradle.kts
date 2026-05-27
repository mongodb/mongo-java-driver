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
plugins {
    id("project.base")
    id("checkstyle")
    id("conventions.testing-base")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.jupiter.platform.launcher)
    // AssertJ used here for infrastructure assertions (isDirectory, hasSize, containsExactly)
    // which are significantly more readable than JUnit 5 equivalents for this test.
    testImplementation(libs.assertj)
    testImplementation(libs.felix.framework)

    // These JARs are scanned by buildSystemPackagesFromClasspath() to export packages
    // from the Felix system bundle, satisfying non-optional imports from bundles under test.
    testImplementation(libs.reactive.streams)
    testImplementation(platform(libs.project.reactor.bom))
    testImplementation(libs.project.reactor.core)
    testImplementation(platform(libs.kotlin.bom))
    testImplementation(libs.kotlin.stdlib.jdk8)
    testImplementation(libs.kotlin.reflect)
    testImplementation(platform(libs.kotlinx.coroutines.bom))
    testImplementation(libs.kotlinx.coroutines.core)
    testImplementation(libs.kotlinx.coroutines.reactive)
    testImplementation(libs.findbugs.jsr)
    testImplementation(libs.jna)
}

tasks.test {
    dependsOn(
        ":bson:jar",
        ":bson-record-codec:jar",
        ":mongodb-crypt:jar",
        ":driver-core:jar",
        ":bson-scala:jar",
        ":driver-sync:jar",
        ":driver-reactive-streams:jar",
        ":driver-scala:jar",
        ":driver-kotlin-sync:jar",
        ":driver-kotlin-coroutine:jar",
        ":driver-kotlin-extensions:jar"
    )
    systemProperty("projectRoot", rootProject.projectDir.absolutePath)
}
