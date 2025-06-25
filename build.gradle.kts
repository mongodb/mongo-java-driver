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
    id("eclipse")
    id("idea")
    alias(libs.plugins.nexus.publish)
}

val nexusUsername: Provider<String> = providers.gradleProperty("nexusUsername")
val nexusPassword: Provider<String> = providers.gradleProperty("nexusPassword")

nexusPublishing {
    packageGroup = "org.mongodb"
    repositories {
        sonatype {
            username = nexusUsername
            password = nexusPassword

            // central portal URLs
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }
}
