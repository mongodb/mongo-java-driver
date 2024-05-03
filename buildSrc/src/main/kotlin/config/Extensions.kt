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

package config

import org.gradle.api.Project
import org.gradle.api.plugins.ExtraPropertiesExtension

object Extensions {

    /**
     * Extension function to allow easy setting of multiple items to `project.extra`
     */
    fun ExtraPropertiesExtension.setAll(valueMap: Map<String, String>) {
        valueMap.forEach { set(it.key, it.value) }
    }

    /**
     * Extension function to get and validate the current scala version
     */
    fun Project.scalaVersion(): String {
        val supportedScalaVersions = (project.property("supportedScalaVersions") as String).split(",")
        val scalaVersion: String = (project.findProperty("scalaVersion") ?: project.property("defaultScalaVersion")) as String

        if (!supportedScalaVersions.contains(scalaVersion)) {
            throw UnsupportedOperationException("""Scala version: $scalaVersion is not a supported scala version.
                |Supported versions: $supportedScalaVersions
            """.trimMargin())
        }

        return scalaVersion
    }
}
