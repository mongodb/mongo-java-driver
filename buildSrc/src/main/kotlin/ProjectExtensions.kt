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
import org.gradle.api.Project
import org.gradle.api.java.archives.Manifest
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.withType

object ProjectExtensions {

    /**
     * Extension function to get and validate the current scala version
     *
     * See: gradle.properties for `supportedScalaVersions` and `defaultScalaVersion`
     */
    fun Project.scalaVersion(): String {
        val supportedScalaVersions = (project.property("supportedScalaVersions") as String).split(",")
        val scalaVersion: String =
            (project.findProperty("scalaVersion") ?: project.property("defaultScalaVersion")) as String

        if (!supportedScalaVersions.contains(scalaVersion)) {
            throw UnsupportedOperationException(
                """Scala version: $scalaVersion is not a supported scala version.
                |Supported versions: $supportedScalaVersions
            """
                    .trimMargin())
        }

        return scalaVersion
    }

    /** Extension function to configure the maven publication */
    fun Project.configureMavenPublication(configure: MavenPublication.() -> Unit = {}) {
        val publishing = extensions.getByName("publishing") as org.gradle.api.publish.PublishingExtension
        publishing.publications.named<MavenPublication>("maven") { configure() }
    }

    /** Extension function to configure the jars manifest */
    fun Project.configureJarManifest(configure: Manifest.() -> Unit = {}) {
        tasks.withType<Jar> { manifest { afterEvaluate { configure() } } }
    }
}
