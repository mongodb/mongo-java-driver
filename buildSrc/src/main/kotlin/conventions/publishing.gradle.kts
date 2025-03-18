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

// Provides the publishing configuration for the build
//
// Note: Further configuration can be achieved using the `project.configureMavenPublication` and
// `project.configureJarManifest` helpers.
// See: `ProjectExtensions.kt` for more information
plugins {
    id("conventions.git-version")
    id("maven-publish")
    id("signing")
}

val signingKey: Provider<String> = providers.gradleProperty("signingKey")
val signingPassword: Provider<String> = providers.gradleProperty("signingPassword")
val nexusUsername: Provider<String> = providers.gradleProperty("nexusUsername")
val nexusPassword: Provider<String> = providers.gradleProperty("nexusPassword")
@Suppress("UNCHECKED_CAST") val gitVersion: Provider<String> = project.findProperty("gitVersion") as Provider<String>

tasks.withType<AbstractPublishToMaven>().configureEach {
    // Gradle warns about some signing tasks using publishing task outputs without explicit
    // dependencies. Here's a quick fix.
    dependsOn(tasks.withType<Sign>())
    mustRunAfter(tasks.withType<Sign>())

    doLast {
        logger.lifecycle("[task: ${name}] ${publication.groupId}:${publication.artifactId}:${publication.version}")
    }
}

val localBuildRepo: Provider<Directory> = rootProject.layout.buildDirectory.dir("repo")

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
        // `./gradlew publishMavenPublicationToLocalBuildRepository`
        maven {
            url = uri(localBuildRepo.get())
            name = "LocalBuild"
        }
    }

    publications.create<MavenPublication>("maven") {
        components.findByName("java")?.let { from(it) }

        pom {
            url.set("https://www.mongodb.com/")
            scm {
                url.set("https://github.com/mongodb/mongo-java-driver")
                connection.set("scm:https://github.com/mongodb/mongo-java-driver.git")
                developerConnection.set("scm:https://github.com/mongodb/mongo-java-driver.git")
            }

            developers {
                developer {
                    name.set("Various")
                    organization.set("MongoDB")
                }
            }

            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
        }

        // Ensure get the final set `base.archivesName` not the default one (project name).
        afterEvaluate { artifactId = base.archivesName.get() }
    }
}

tasks.withType<Jar> {
    manifest {
        attributes["-exportcontents"] = "*;-noimport:=true"
    }

    afterEvaluate {
        manifest {
            if (attributes.containsKey("-nomanifest")) {
                attributes.remove("-exportcontents")
            } else {
                attributes["Bundle-Version"] = project.version
                attributes["Bundle-SymbolicName"] = "${project.findProperty("group")}.${project.findProperty("archivesBaseName")}"
                attributes["Build-Version"] = gitVersion.get()
                attributes["Bundle-Name"] = base.archivesName.get()
            }
        }
    }
}

signing {
    if (signingKey.isPresent && signingPassword.isPresent) {
        logger.debug("[${project.displayName}] Signing is enabled")
        useInMemoryPgpKeys(signingKey.get(), signingPassword.get())
    }
}

tasks.named<Delete>("clean") { delete.add(localBuildRepo) }

tasks.withType<GenerateModuleMetadata> { enabled = false }

tasks.register("publishSnapshots") {
    group = "publishing"
    description = "Publishes snapshots to Sonatype"

    if (version.toString().endsWith("-SNAPSHOT")) {
        dependsOn(tasks.withType<PublishToMavenRepository>())
    }
}

tasks.register("publishArchives") {
    group = "publishing"
    description = "Publishes a release and uploads to Sonatype / Maven Central"

    val currentGitVersion = gitVersion.get()
    val gitVersionMatch = currentGitVersion == version
    doFirst {
        if (!gitVersionMatch) {
            val cause =
                """
                Version mismatch:
                =================

                 $version != $currentGitVersion

                 The project version does not match the git tag.
                """.trimMargin()
            throw GradleException(cause)
        } else {
            println("Publishing: ${project.name} : $currentGitVersion")
        }
    }
    if (gitVersionMatch) {
        dependsOn(tasks.withType<PublishToMavenRepository>())
    }
}

// workaround for https://github.com/gradle/gradle/issues/16543
inline fun <reified T : Task> TaskContainer.provider(taskName: String): Provider<T> =
    providers.provider { taskName }.flatMap { named<T>(it) }
