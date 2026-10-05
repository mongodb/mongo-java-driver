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
package conventions

import libs

// Dokka, the documentation engine for Kotlin
// https://plugins.gradle.org/plugin/org.jetbrains.dokka
plugins {
    alias(libs.plugins.dokka)
    id("conventions.publishing")
}

// Create a generic `docs` task
tasks.register("docs") {
    group = "documentation"
    dependsOn("dokkaHtml")
}

val dokkaOutputDir: Provider<Directory> = rootProject.layout.buildDirectory.dir("docs/${base.archivesName.get()}")

tasks.dokkaHtml.configure {
    outputDirectory.set(dokkaOutputDir.get().asFile)
    moduleName.set(base.archivesName.get())
}

val cleanDokka by tasks.register<Delete>("cleanDokka") { delete(dokkaOutputDir) }

// Ensure dokka is used for the javadoc
afterEvaluate {
    tasks.named<Jar>("javadocJar").configure {
        dependsOn("cleanDokka", "dokkaHtml")
        archiveClassifier.set("javadoc")
        from(dokkaOutputDir)
    }
}
