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

// Spotless configuration for `buildSrc` code.
spotless {
    kotlinGradle {
        target("**/*.gradle.kts")
        ktfmt("0.39").dropboxStyle().configure {
            it.setMaxWidth(120)
            it.setRemoveUnusedImport(true)
        }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile(
            "../config/mongodb.license", "(package|group|plugins|import|buildscript|rootProject|@Suppress)")
    }

    kotlin {
        target("**/*.kt")
        ktfmt().dropboxStyle().configure {
            it.setMaxWidth(120)
            it.setRemoveUnusedImport(true)
        }
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
