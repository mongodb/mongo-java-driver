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

import config.createPom

plugins {
    id("java-library")
    id("maven-publish")
    id("signing")
}

val signingKey: Provider<String> = providers.gradleProperty("signingKey")
val signingPassword: Provider<String> = providers.gradleProperty("signingPassword")
val nexusUsername: Provider<String> = providers.gradleProperty("nexusUsername")
val nexusPassword: Provider<String> = providers.gradleProperty("nexusPassword")

tasks.withType<AbstractPublishToMaven>().configureEach {
    // Gradle warns about some signing tasks using publishing task outputs without explicit
    // dependencies. Here's a quick fix.
    dependsOn(tasks.withType<Sign>())
    mustRunAfter(tasks.withType<Sign>())

    doLast {
        logger.lifecycle("[task: ${name}] ${publication.groupId}:${publication.artifactId}:${publication.version}")
    }
}

java {
    withSourcesJar()
    withJavadocJar()
}

val mavenName: String by project.extra
val mavenDescription: String by project.extra
val mavenUrl: String? by project.extra
val mavenArtifactId: String? by project.extra
val localBuildRepo: String = "${rootProject.buildDir}/repo"

val sonatypeRepositoryReleaseUrl: Provider<String> = provider {
    if (version.toString().endsWith("SNAPSHOT")) {
        "https://oss.sonatype.org/content/repositories/snapshots/"
    } else {
        "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
    }
}

publishing {
    repositories {
        maven {
            url = uri(sonatypeRepositoryReleaseUrl)
            if (nexusUsername.isPresent && nexusPassword.isPresent) {
                credentials {
                    username = nexusUsername.get()
                    password = nexusPassword.get()
                }
            }
        }

        // publish to local dir, for artifact tracking and testing
        maven {
            url = uri(localBuildRepo)
            name = "LocalBuild"
        }
    }

    publications {
        create<MavenPublication>("mavenJava") {
            components.findByName("scala").let { from(it) }
            components.findByName("java").let { from(it) }
        }
    }

    publications.withType<MavenPublication>().configureEach {
        createPom {
            name.set(provider { mavenName })
            description.set(provider { mavenDescription })
        }

        if (signingKey.isPresent && signingPassword.isPresent) {
            signing.sign(this)
        }

        afterEvaluate {
            mavenArtifactId.let { artifactId = it }
            mavenUrl.let { pom.url.set(it) }
        }
    }
}

tasks.named<Delete>("clean") {
    delete.add(localBuildRepo)
}

tasks.withType<Jar> {
    afterEvaluate {
        manifest {
            attributes(
                mapOf(
                    "-exportcontents" to "*;-noimport:=true",
                    "Build-Version" to project.findProperty("gitVersion"),
                    "Bundle-Version" to project.version,
                    "Bundle-Name" to project.findProperty("archivesBaseName"),
                    "Bundle-SymbolicName" to "${group}.${project.findProperty("archivesBaseName")}",
                    "Automatic-Module-Name" to project.extra.get("automaticModuleName"),
                    "Import-Package" to project.extra.get("importPackage")
                )
            )
        }
    }
}

signing {
    if (signingKey.isPresent && signingPassword.isPresent) {
        logger.debug("[${project.displayName}] Signing is enabled")
        useInMemoryPgpKeys(signingKey.get(), signingPassword.get())
    }
}

// workaround for https://github.com/gradle/gradle/issues/16543
inline fun <reified T : Task> TaskContainer.provider(taskName: String): Provider<T> =
    providers.provider { taskName }.flatMap { named<T>(it) }
