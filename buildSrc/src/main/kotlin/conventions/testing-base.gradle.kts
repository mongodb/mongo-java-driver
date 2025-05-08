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

import com.adarshr.gradle.testlogger.theme.ThemeType
import project.DEFAULT_JAVA_VERSION

// Default test configuration for projects
//
// Utilizes the test-logger plugin:
// https://plugins.gradle.org/plugin/com.adarshr.test-logger
plugins {
    id("java-library")
    id("com.adarshr.test-logger")
}

tasks.withType<Test> {
    maxHeapSize = "4g"
    maxParallelForks = 1

    useJUnitPlatform()

    // Pass any `org.mongodb.*` system settings
    systemProperties =
        System.getProperties()
            .map { (key, value) -> Pair(key.toString(), value) }
            .filter { it.first.startsWith("org.mongodb.") }
            .toMap()

    // Convert any ssl based properties
    if (project.buildingWith("ssl.enabled")) {
        if (project.hasProperty("ssl.keyStoreType")) {
            systemProperties(
                mapOf(
                    "javax.net.ssl.keyStoreType" to project.property("ssl.keyStoreType"),
                    "javax.net.ssl.keyStore" to project.property("ssl.keyStore"),
                    "javax.net.ssl.keyStorePassword" to project.property("ssl.keyStorePassword")))
        }
        if (project.hasProperty("ssl.trustStoreType")) {
            systemProperties(
                mapOf(
                    "javax.net.ssl.trustStoreType" to project.property("ssl.trustStoreType"),
                    "javax.net.ssl.trustStore" to project.property("ssl.trustStore"),
                    "javax.net.ssl.trustStorePassword" to project.property("ssl.trustStorePassword")))
        }
        if (project.hasProperty("ocsp.property")) {
            systemProperties(
                mapOf(
                    "org.mongodb.test.ocsp.tls.should.succeed" to project.property("ocsp.tls.should.succeed"),
                    "java.security.properties" to file(project.property("ocsp.property").toString()),
                    "com.sun.net.ssl.checkRevocation" to project.property("ssl.checkRevocation"),
                    "jdk.tls.client.enableStatusRequestExtension" to
                        project.property("client.enableStatusRequestExtension"),
                    "jdk.tls.client.protocols" to project.property("client.protocols")))
        }
    }

    // Convert gssapi properties
    if (project.buildingWith("gssapi.enabled")) {
        systemProperties(
            mapOf(
                "sun.security.krb5.debug" to project.property("sun.security.krb5.debug"),
                "javax.security.auth.useSubjectCredsOnly" to "false",
                "java.security.krb5.kdc" to project.property("krb5.kdc"),
                "java.security.krb5.realm" to project.property("krb5.realm"),
                "java.security.auth.login.config" to project.property("auth.login.config"),
            ))
    }

    // Allow testing with an alternative JDK version
    val testJavaVersion: Int = findProperty("javaVersion")?.toString()?.toInt() ?: DEFAULT_JAVA_VERSION
    javaLauncher.set(javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(testJavaVersion) })
}

// Pretty test output
testlogger {
    theme = ThemeType.STANDARD
    showExceptions = true
    showStackTraces = true
    showFullStackTraces = false
    showCauses = true
    slowThreshold = 2000
    showSummary = true
    showSimpleNames = false
    showPassed = true
    showSkipped = true
    showFailed = true
    showOnlySlow = false
    showStandardStreams = false
    showPassedStandardStreams = true
    showSkippedStandardStreams = true
    showFailedStandardStreams = true
    logLevel = LogLevel.LIFECYCLE
}

dependencies {
    testImplementation(libs.assertj)
}
