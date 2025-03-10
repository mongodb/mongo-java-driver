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
    id("conventions.testing-spock")
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
    implementation(project(path = ":bson-kotlin", configuration = "default")) // TODO optional
    implementation(project(path = ":bson-kotlinx", configuration = "default")) // TODO optional
    api(project(path = ":mongodb-crypt")) // TODO optional

    implementation(libs.jnr.unixsocket) // TODO optional
    api(platform(libs.netty.bom)) // TODO optional
    api(libs.bundles.netty) // TODO optional
    compileOnly(libs.graal.sdk)

    // Optionally depend on both AWS SDK v2 and v1.
    // The driver will use v2 is present, v1 if present, or built-in functionality if neither are
    // present
    implementation(libs.bundles.aws.java.sdk.v1) // TODO optional
    implementation(libs.bundles.aws.java.sdk.v2) // TODO optional

    implementation(libs.snappy.java) // TODO optional
    implementation(libs.zstd.jni) // TODO optional

    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(libs.reflections)

    testRuntimeOnly(libs.netty.tcnative.boringssl)
    listOf("linux-x86_64", "linux-aarch_64", "osx-x86_64", "osx-aarch_64", "windows-x86_64").forEach { arch ->
        testRuntimeOnly(variantOf(libs.netty.tcnative.boringssl) { classifier(arch) })
    }
}

configurations.create("consumableTestRuntimeOnly") {
    extendsFrom(configurations.testRuntimeOnly.get())
    setCanBeConsumed(true)
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
