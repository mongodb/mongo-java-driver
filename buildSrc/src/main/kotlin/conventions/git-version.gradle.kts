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

// Provides the current git version for the build

val gitVersion: Provider<String> =
    providers
        .exec { commandLine("git", "describe", "--tags", "--always", "--dirty") }
        .standardOutput
        .asText
        .map { it.trim().removePrefix("r") }

// Allows access to gitVersion extension to other conventions
extensions.add("gitVersion", gitVersion)

// Debug task that outputs the gitVersion.
tasks.register("gitVersion") { doLast { println("Git version: ${gitVersion.get()}") } }
