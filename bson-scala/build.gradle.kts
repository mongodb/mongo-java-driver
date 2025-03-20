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
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication

plugins { id("project.scala") }

base.archivesName.set("mongo-scala-bson")

dependencies { api(project(path = ":bson", configuration = "default")) }

configureMavenPublication {
    pom {
        name.set("Mongo Scala BSON Library")
        description.set("A Scala wrapper / extension to the BSON library")
        url.set("https://bsonspec.org")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.bson.scala"
    attributes["Bundle-SymbolicName"] = "org.mongodb.scala.mongo-scala-bson"
    attributes["Import-Package"] = "!scala.*,*"
}
