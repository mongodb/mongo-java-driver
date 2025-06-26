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

plugins {
    id("eclipse")
    id("idea")
    alias(libs.plugins.nexus.publish)
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
