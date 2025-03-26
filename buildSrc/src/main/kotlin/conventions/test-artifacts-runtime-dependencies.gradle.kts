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

plugins { id("java-library") }

// Also include test runtime dependencies
dependencies {
    testRuntimeClasspath(platform(libs.netty.bom))
    testRuntimeClasspath(libs.netty.tcnative.boringssl.static)
    listOf("linux-x86_64", "linux-aarch_64", "osx-x86_64", "osx-aarch_64", "windows-x86_64").forEach { arch ->
        testRuntimeClasspath(variantOf(libs.netty.tcnative.boringssl.static) { classifier(arch) })
    }
}
