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
package project

plugins {
    id("java-library")
    id("checkstyle")
    id("project.base")
    id("conventions.bnd")
    id("conventions.javadoc")
    id("conventions.optional")
    id("conventions.publishing")
    id("conventions.spotbugs")
    id("conventions.spotless")
    id("conventions.testing-junit")
}

dependencies { "optionalApi"(libs.slf4j) }

logger.info("Compiling ${project.name} using JDK${DEFAULT_JAVA_VERSION}")

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    toolchain { languageVersion = JavaLanguageVersion.of(DEFAULT_JAVA_VERSION) }

    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
}

sourceSets["main"].java { srcDir("src/main") }
