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
import java.time.Duration
import org.cyclonedx.model.*

plugins {
    id("eclipse")
    id("idea")
    alias(libs.plugins.nexus.publish)
    id("org.cyclonedx.bom") version "2.3.1"
}

val nexusUsername: Provider<String> = providers.gradleProperty("nexusUsername")
val nexusPassword: Provider<String> = providers.gradleProperty("nexusPassword")

nexusPublishing {
    packageGroup.set("org.mongodb")
    repositories {
        sonatype {
            username.set(nexusUsername)
            password.set(nexusPassword)

            // central portal URLs
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }

    connectTimeout.set(Duration.ofMinutes(5))
    clientTimeout.set(Duration.ofMinutes(30))

    transitionCheckOptions {
        // We have many artifacts and Maven Central can take a long time on its compliance checks.
        // Set the timeout for waiting for the repository to close to a comfortable 50 minutes.
        maxRetries.set(300)
        delayBetween.set(Duration.ofSeconds(10))
    }
}

tasks.cyclonedxBom {
    setGroup("org.mongodb")

    // includeConfigs is the list of configuration names to include when generating the BOM (leave empty to include every configuration), regex is supported
    setIncludeConfigs(listOf("runtimeClasspath","baseline"))
    // skipConfigs is a list of configuration names to exclude when generating the BOM, regex is supported
    //setSkipConfigs(listOf("(?i)(.*(compile|test|checkstyle|codenarc|spotbugs|detekt|analysis|zinc|dokka|commonizer|implementation|annotation).*)"))
    // skipProjects is a list of project names to exclude when generating the BOM
    setSkipProjects(listOf(rootProject.name, "bom"))
    // Specified the type of project being built. Defaults to 'library'
    setProjectType("library")
    // Specified the version of the CycloneDX specification to use. Defaults to '1.6'
    setSchemaVersion("1.5")
    // Boms destination directory. Defaults to 'build/reports'
    setDestination(project.file("./"))
    // The file name for the generated BOMs (before the file format suffix). Defaults to 'bom'
    setOutputName("sbom")
    // The file format generated, can be xml, json or all for generating both. Defaults to 'all'
    setOutputFormat("json")
    // Include BOM Serial Number. Defaults to 'true'
    //setIncludeBomSerialNumber(false)
    // Include License Text. Defaults to 'true'
    //setIncludeLicenseText(true)
    // Include resolution of full metadata for components including licenses. Defaults to 'true'
    //setIncludeMetadataResolution(true)
    // Override component version. Defaults to the project version
    //setComponentVersion("2.0.0")
    // Override component name. Defaults to the project name
    //setComponentName("my-component")

    // declaration of the Object from OrganizationalContact
    var organizationalContact1 = OrganizationalContact()

    // setting the Name[String], Email[String] and Phone[String] of the Object
    organizationalContact1.setName("MongoDB, Inc.")

    // passing data to the plugin
    setOrganizationalEntity { oe ->
        oe.name = "MongoDB, Inc."
        oe.urls = listOf("www.mongodb.com")
        oe.addContact(organizationalContact1)
    }

    // Configure VCS external reference with proper HTTPS URL
    setVCSGit { vcs ->
        vcs.url = "https://github.com/mongodb/mongo-java-driver.git"
    }
}
