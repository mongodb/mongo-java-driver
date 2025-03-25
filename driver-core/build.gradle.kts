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

plugins {
    id("project.java")
    id("conventions.test-artifacts")
    id("conventions.testing-mockito")
    id("conventions.testing-junit")
    id("conventions.testing-spock-exclude-slow")
    alias(libs.plugins.build.config)
}

base.archivesName.set("mongodb-driver-core")

buildConfig {
    className("MongoDriverVersion")
    packageName("com.mongodb.internal.build")
    useJavaOutput()
    buildConfigField("String", "NAME", "\"mongo-java-driver\"")
    buildConfigField("String", "VERSION", "\"${(project.findProperty("gitVersion") as Provider<*>?)?.get()}\"")
}

dependencies {
    api(project(path = ":bson", configuration = "default"))
    implementation(project(path = ":bson-record-codec", configuration = "default"))
    compileOnly(libs.graal.sdk)

    optionalImplementation(project(path = ":bson-kotlin", configuration = "default"))
    optionalImplementation(project(path = ":bson-kotlinx", configuration = "default"))
    optionalImplementation(project(path = ":mongodb-crypt", configuration = "default"))
    optionalImplementation(libs.jnr.unixsocket)
    optionalApi(platform(libs.netty.bom))
    optionalApi(libs.bundles.netty)

    // Optionally depend on both AWS SDK v2 and v1.
    // The driver will choose: v2 or v1 or fallback to built-in functionality
    optionalImplementation(libs.bundles.aws.java.sdk.v1)
    optionalImplementation(libs.bundles.aws.java.sdk.v2)

    optionalImplementation(libs.snappy.java)
    optionalImplementation(libs.zstd.jni)

    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(libs.reflections)
    testImplementation(libs.netty.tcnative.boringssl.static)
    listOf("linux-x86_64", "linux-aarch_64", "osx-x86_64", "osx-aarch_64", "windows-x86_64").forEach { arch ->
        testImplementation("${libs.netty.tcnative.boringssl.static.get()}::$arch")
    }
}

configureMavenPublication {
    pom {
        name.set("MongoDB Java Driver Core")
        description.set(
            "Shared components for the Synchronous and Reactive Streams implementations of the MongoDB Java Driver.")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.driver.core"
    attributes["Bundle-SymbolicName"] = "org.mongodb.driver-core"
    attributes["Import-Package"] =
        listOf(
                "!sun.misc.*", // Used by DirectBufferDeallocator only for java 8
                "!sun.nio.ch.*", // Used by DirectBufferDeallocator only for java 8
                "!javax.annotation.*", // Brought in by com.google.code.findbugs:annotations
                "!com.oracle.svm.core.annotate.*", // this dependency is provided by the GraalVM
                // runtime
                "io.netty.*;resolution:=optional",
                "com.amazonaws.*;resolution:=optional",
                "software.amazon.awssdk.*;resolution:=optional",
                "org.xerial.snappy.*;resolution:=optional",
                "com.github.luben.zstd.*;resolution:=optional",
                "org.slf4j.*;resolution:=optional",
                "jnr.unixsocket.*;resolution:=optional",
                "com.mongodb.internal.crypt.capi.*;resolution:=optional",
                "jdk.net.*;resolution:=optional", // Used by SocketStreamHelper & depends on JDK
                // version
                "org.bson.codecs.record.*;resolution:=optional", // Depends on JDK version
                "org.bson.codecs.kotlin.*;resolution:=optional",
                "org.bson.codecs.kotlinx.*;resolution:=optional",
                "*" // import all that is not excluded or modified before
                )
            .joinToString(",")
}
