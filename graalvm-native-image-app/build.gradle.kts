/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Note requires a Gradle project flag `-PincludeGraalvm` (see settings.gradle.kts).

plugins {
    id("application")
    id("java-library")
    id("project.base")
    id("conventions.test-artifacts")
    alias(libs.plugins.graalvm.buildtools)
}

application {
    mainClass = "com.mongodb.internal.graalvm.NativeImageApp"
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/main/resources")) }
    }
}

dependencies {
    // we intentionally depend here on the driver artifacts instead of depending on compiled classes
    implementation(project(path = ":bson", configuration = "archives"))
    implementation(project(path = ":driver-core", configuration = "archives"))
    implementation(project(path = ":driver-sync", configuration = "archives"))
    implementation(project(path = ":driver-legacy", configuration = "archives"))
    implementation(project(path = ":driver-reactive-streams", configuration = "archives"))
    implementation(project(path = ":mongodb-crypt", configuration =  "archives"))
    implementation(project(path = ":mongodb-crypt", configuration =  "runtimeElements"))

    implementation(project(path = ":driver-core", configuration = "testArtifacts"))
    implementation(project(path = ":driver-sync", configuration = "testArtifacts"))
    implementation(project(path = ":driver-legacy", configuration = "testArtifacts"))
    implementation(project(path = ":driver-reactive-streams", configuration = "testArtifacts"))

    implementation(libs.slf4j)
    implementation(libs.jna)
    implementation(libs.graal.sdk.nativeimage)
    implementation(libs.jetbrains.annotations)
    implementation(libs.logback.classic)
    implementation(platform(libs.project.reactor.bom))
    implementation(libs.project.reactor.core)
}

tasks.withType<Javadoc>().configureEach {
    enabled = false
}


@Suppress("UNCHECKED_CAST")
val systemPropertiesForRunningNativeApp: Map<String, Any?> = (System.getProperties().toMap() as Map<String, Any?>)
    .filterKeys { it.startsWith("org.mongodb.") }
tasks.named<JavaExec>("run") { systemProperties = systemPropertiesForRunningNativeApp }

// see https://graalvm.github.io/native-build-tools/latest/gradle-plugin.html
graalvmNative {
    metadataRepository {
        enabled.set(false)
    }
    agent {
        // Executing the `run` Gradle task with the tracing agent
        // https://www.graalvm.org/latest/reference-manual/native-image/metadata/AutomaticMetadataCollection/
        // requires running Gradle with GraalVM despite the toolchain for the task already being GraalVM.
        // The same is true about executing the `metadataCopy` Gradle task.
        // This may be a manifestation of an issue with the `org.graalvm.buildtools.native` plugin.
        enabled.set(false)
        defaultMode.set("direct")
        val taskExecutedWithAgentAttached = "run"
        modes {
            direct {
                // see https://www.graalvm.org/latest/reference-manual/native-image/metadata/ExperimentalAgentOptions
                options.add("config-output-dir=${rootProject.file("build/native/agent-output/$taskExecutedWithAgentAttached").path}")
                // `experimental-configuration-with-origins` produces
                // `graalvm-native-image-app/build/native/agent-output/run/reflect-origins.txt`
                // and similar files that explain the origin of each of the reachability metadata piece.
                // However, for some reason, the actual reachability metadata is not generated when this option is enabled,
                // so enable it manually if you need an explanation for a specific reachability metadata entry,
                // and expect the build to fail.
                // options.add("experimental-configuration-with-origins")

                // `experimental-class-define-support` does not seem to do what it is supposed to do.
                // We need this option to work if we want to support `UnixServerAddress` in native image.
                // Unfortunately, the tracing agent neither generates the metadata in
                // `graalvm-native-image-app/src/main/resources/META-INF/native-image/proxy-config.json`,
                // nor does it extract the bytecode of the generated classes to
                // `graalvm-native-image-app/src/main/resources/META-INF/native-image/agent-extracted-predefined-classes`.
                options.add("experimental-class-define-support")
            }
        }
        metadataCopy {
            inputTaskNames.add(taskExecutedWithAgentAttached)
            outputDirectories.add("src/main/resources/META-INF/native-image")
            mergeWithExisting.set(false)
        }
    }
    binaries {
        configureEach {
            buildArgs.add("--strict-image-heap")
            buildArgs.add("-H:+UnlockExperimentalVMOptions")
            // see class initialization and other reports in `graalvm/build/native/nativeCompile/reports`
            buildArgs.add("--diagnostics-mode")
            // see the "registerResource" entries in the `native-image` built-time output,
            // informing us on the resources included in the native image being built
            buildArgs.add("-H:Log=registerResource:5")
        }
        named("main") {
            val mainClassName = application.mainClass.get()
            imageName = mainClassName.substring(mainClassName.lastIndexOf('.') + 1)
            sharedLibrary.set(false)
            runtimeArgs.addAll(systemPropertiesForRunningNativeApp.entries
                .stream()
                .map {"-D${it.key}=${it.value}" }
                .toList())
            quickBuild.set(true)
            // See the "Apply" entries in the `native-image` built-time output, informing us on
            // the build configuration files (https://www.graalvm.org/latest/reference-manual/native-image/overview/BuildConfiguration/)
            // and the reachability metadata files (https://www.graalvm.org/latest/reference-manual/native-image/metadata/)
            // which are applied at build time.
            verbose.set(true)
        }
    }
}

// By configuring the toolchains for the `org.graalvm.buildtools.native` plugin
// conditionally, we avoid Gradle errors caused by it failing to locate an installed GraalVM
// for Java SE older than 21. One situation when this is relevant is building from an IDE,
// where the `DEFAULT_JDK_VERSION` is likely used.
val minRequiredGraalVMJavaVersion = 21
val graalJavaVersion: Int = findProperty("javaVersion")?.toString()?.toInt() ?: minRequiredGraalVMJavaVersion
val javaLanguageVersion: JavaLanguageVersion = JavaLanguageVersion.of(graalJavaVersion)

if (graalJavaVersion >= minRequiredGraalVMJavaVersion) {
    // `JvmVendorSpec.GRAAL_VM` matches only GraalVM Community (https://github.com/graalvm/graalvm-ce-builds/releases),
    // and does not match any other GraalVM distribution.
    // That is, Gradle fails to locate any other installed distribution of GraalVM.
    // Furthermore, there is no other way to express via the Gradle toolchain functionality
    // that GraalVM must be used. The documentation of the `org.graalvm.buildtools.native` plugin
    // says the following about this limitation:
    // "be aware that the toolchain detection cannot distinguish between GraalVM JDKs
    // and standard JDKs without Native Image support:
    // if you have both installed on the machine, Gradle may randomly pick one or the other".
    // Fortunately, `JvmVendorSpec.GRAAL_VM` makes things less hideous than that.
    //
    // The documentation of the `org.graalvm.buildtools.native` plugin mentions
    // the environment variable `GRAALVM_HOME` as an alternative to Gradle toolchain functionality.
    // I was unable to find a way to stop relying on the toolchain specification requiring `JvmVendorSpec.GRAAL_VM`
    // even with `GRAALVM_HOME`.
    val graalVendor = JvmVendorSpec.GRAAL_VM
    graalvmNative {
        agent {
            java {
                toolchain {
                    // TODO - errors saying its immutable.
                    // languageVersion.set(javaLanguageVersion)
                    // vendor.set(graalVendor)
                }
            }
        }
        binaries {
            configureEach {
                javaLauncher.set(javaToolchains.launcherFor {
                    languageVersion.set(javaLanguageVersion)
                    vendor.set(graalVendor)
                })
            }
        }
    }
}
