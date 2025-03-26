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

// The CodeNarc plugin performs quality checks on your project’s Groovy source files
// https://docs.gradle.org/current/userguide/codenarc_plugin.html
plugins { id("codenarc") }

codenarc {
    toolVersion = "1.6.1"
    reportFormat = if (project.buildingWith("xmlReports.enabled")) "xml" else "html"
}
