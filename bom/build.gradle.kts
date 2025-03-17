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
import ProjectExtensions.configureMavenPublication
import groovy.util.Node
import groovy.util.NodeList

plugins {
    id("java-platform")
    id("project.base")
    id("conventions.publishing")
    id("conventions.spotless")
}

base.archivesName.set("mongodb-driver-bom")

dependencies {
    constraints {
        api(project(":mongodb-crypt"))
        api(project(":driver-core"))
        api(project(":bson"))
        api(project(":bson-record-codec"))

        api(project(":driver-sync"))
        api(project(":driver-reactive-streams"))

        api(project(":bson-kotlin"))
        api(project(":bson-kotlinx"))
        api(project(":driver-kotlin-coroutine"))
        api(project(":driver-kotlin-sync"))

        api(project(":bson-scala"))
        api(project(":driver-scala"))
    }
}

/*
 * Handle the multiple versions of Scala we support as defined in `gradle.properties`
 */
val defaultScalaVersion: String = project.findProperty("defaultScalaVersion")!!.toString()
val scalaVersions: List<String>? = project.findProperty("supportedScalaVersions")?.toString()?.split(",")

assert(!scalaVersions.isNullOrEmpty()) {
    "Scala versions must be provided as a comma-separated list in the 'supportedScalaVersions' project property"
}

/*
 * Apply the Java Platform plugin to create the BOM
 * Modify the generated POM to include all supported versions of Scala for driver-scala or bson-scala.
 */
configureMavenPublication {
    components.findByName("javaPlatform")?.let { from(it) }

    pom {
        name.set("bom")
        description.set(
            "This Bill of Materials POM simplifies dependency management when referencing multiple MongoDB Java Driver artifacts in projects using Gradle or Maven.")

        withXml {
            val pomXml: Node = asNode()

            val dependencyManagementNode = pomXml.getNode("dependencyManagement")
            assert(dependencyManagementNode != null) {
                "<dependencyManagement> node not found in the generated BOM POM"
            }
            val dependenciesNode = dependencyManagementNode.getNode("dependencies")
            assert(dependenciesNode != null) { "<dependencies> node not found in the generated BOM POM" }

            val existingScalaDeps =
                dependenciesNode!!
                    .children()
                    .map { it as Node }
                    .filter { it.getNode("artifactId")?.text()?.contains("scala") ?: false }

            existingScalaDeps.forEach {
                val groupId: String = it.getNode("groupId")!!.text()
                val originalArtifactId: String = it.getNode("artifactId")!!.text()
                val artifactVersion: String = it.getNode("version")!!.text()

                // Add multiple versions with Scala suffixes for each Scala-related dependency.
                scalaVersions!!.forEach { scalaVersion ->
                    if (scalaVersion != defaultScalaVersion) {
                        // Replace scala version suffix
                        val newArtifactId: String = originalArtifactId.replace(defaultScalaVersion, scalaVersion)
                        val dependencyNode = dependenciesNode.appendNode("dependency")
                        dependencyNode.appendNode("groupId", groupId)
                        dependencyNode.appendNode("artifactId", newArtifactId)
                        dependencyNode.appendNode("version", artifactVersion)
                    }
                }
            }
        }
    }
}

/*
 * Validate the BOM file.
 */
tasks.withType<GenerateMavenPom> {
    doLast {
        pom.withXml {
            val pomXml: Node = asNode()
            val dependenciesNode = pomXml.getNode("dependencyManagement").getNode("dependencies")
            assert(dependenciesNode!!.children().isNotEmpty()) {
                "BOM must contain more then one <dependency> element:\n$destination"
            }

            dependenciesNode
                .children()
                .map { it as Node }
                .forEach {
                    val groupId: String = it.getNode("groupId")!!.text()
                    assert(groupId.startsWith("org.mongodb")) {
                        "BOM must contain only 'org.mongodb' dependencies, but found '$groupId':\n$destination"
                    }

                    /*
                     * The <scope> and <optional> tags should be omitted in BOM dependencies.
                     * This ensures that consuming projects have the flexibility to decide whether a dependency is optional in their context.
                     *
                     * The BOM's role is to provide version information, not to dictate inclusion or exclusion of dependencies.
                     */
                    assert(it.getNode("scope") == null) {
                        "BOM must not contain <scope> elements in dependency:\n$destination"
                    }
                    assert(it.getNode("optional") == null) {
                        "BOM must not contain <optional> elements in dependency:\n$destination"
                    }
                }
        }
    }
}

/** A node lookup helper. */
private fun Node?.getNode(nodeName: String): Node? {
    val found = this?.get(nodeName)
    if (found is NodeList && found.isNotEmpty()) {
        return found[0] as Node
    }
    return null
}
