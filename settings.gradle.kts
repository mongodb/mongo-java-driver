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


include(":bson")
include(":bson-record-codec")
include(":driver-benchmarks")
include(":driver-workload-executor")
include(":driver-lambda")
include(":driver-core")
include(":driver-legacy")
include(":driver-sync")
include(":driver-reactive-streams")
include(":bson-kotlin")
include(":bson-kotlinx")
include(":driver-kotlin-sync")
include(":driver-kotlin-coroutine")
include(":bson-scala")
include(":driver-scala")
include("util:spock")
include("util:taglets")
include(":graalvm-native-image-app")
