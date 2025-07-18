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

pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

include(":bom")

include(":bson")
include(":bson-kotlin")
include(":bson-kotlinx")
include(":bson-record-codec")
include(":bson-scala")

include(":driver-core")
include(":driver-sync")
include(":driver-legacy")
include(":driver-reactive-streams")
include(":mongodb-crypt")

include(":driver-kotlin-coroutine")
include(":driver-kotlin-extensions")
include(":driver-kotlin-sync")
include(":driver-scala")

include(":driver-benchmarks")
include(":driver-lambda")
include(":driver-workload-executor")
if (providers.gradleProperty("includeGraalvm").isPresent) {
    include(":graalvm-native-image-app")
}
