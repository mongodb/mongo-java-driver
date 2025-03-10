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
import dev.panuszewski.gradle.pluginMarker

plugins {
    id("java-library")
    `kotlin-dsl`
    alias(libs.plugins.spotless)
    alias(libs.plugins.detekt) apply false
}

repositories {
    gradlePluginPortal()
    mavenCentral()
    google()
}

// Dependencies needed for the configuration of the plugins
// Uses `pluginMarker` from the `typesafe-conventions` plugin, see `settings.gradle.kts`
dependencies {
    implementation(pluginMarker(libs.plugins.bnd))
    implementation(pluginMarker(libs.plugins.detekt))
    implementation(pluginMarker(libs.plugins.dokka))
    implementation(pluginMarker(libs.plugins.kotlin.gradle))
    implementation(pluginMarker(libs.plugins.spotbugs))
    implementation(pluginMarker(libs.plugins.spotless))
    implementation(pluginMarker(libs.plugins.test.logger))
}

// Spotless configuration for `buildSrc` code.
spotless {
    kotlinGradle {
        target("**/*.gradle.kts")
        ktfmt("0.39").dropboxStyle().configure { it.setMaxWidth(120) }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(
            "../config/mongodb.license", "(package|group|plugins|import|buildscript|rootProject|@Suppress)")
    }

    kotlin {
        target("**/*.kt")
        ktfmt().dropboxStyle().configure { it.setMaxWidth(120) }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("../config/mongodb.license"))
    }

    java {
        palantirJavaFormat()
        target("src/*/java/**/*.java")
        removeUnusedImports()
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(rootProject.file("../config/mongodb.license"))
    }
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of("17")) } }

tasks.findByName("check")?.dependsOn("spotlessCheck")
