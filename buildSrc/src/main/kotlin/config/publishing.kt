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

import org.gradle.api.publish.maven.MavenPom
import org.gradle.api.publish.maven.MavenPublication

/**
 * Creates and configures the pom
 *
 * Also allows overrides via the `configure` parameter
 */
fun MavenPublication.createPom(configure: MavenPom.() -> Unit = {}): Unit = pom {
    url.set("https://www.mongodb.com/")
    scm {
        url.set("https://github.com/mongodb/mongo-java-driver")
        connection.set("scm:https://github.com/mongodb/mongo-java-driver.git")
        developerConnection.set("scm:https://github.com/mongodb/mongo-java-driver.git")
    }

    developers {
        developer {
            id.set("Various")
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
    configure()
}
