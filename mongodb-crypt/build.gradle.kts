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
import de.undercouch.gradle.tasks.download.Download
import org.gradle.api.GradleException
import java.io.ByteArrayOutputStream
import javax.inject.Inject
import org.gradle.process.ExecOperations

plugins {
    id("project.java")
    alias(libs.plugins.download)
}

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(libs.jna)
}

configureMavenPublication {
    pom {
        name.set("MongoCrypt")
        description.set("MongoDB client-side crypto support")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "com.mongodb.crypt.capi"
    attributes["Bundle-Name"] = "MongoCrypt"
    attributes["Bundle-SymbolicName"] = "com.mongodb.crypt.capi"
    attributes["Import-Package"] = "org.slf4j.*;resolution:=optional,org.bson.*"
    attributes["-exportcontents"] = "com.mongodb.*;-noimport:=true"
    attributes["Private-Package"] = ""
}

/*
 * Jna copy or download resources
 */
val jnaDownloadsDir = rootProject.file("build/jnaLibs/downloads/").path
val jnaResourcesDir = rootProject.file("build/jnaLibs/resources/").path
val jnaLibPlatform: String =
    if (com.sun.jna.Platform.RESOURCE_PREFIX.startsWith("darwin")) "darwin" else com.sun.jna.Platform.RESOURCE_PREFIX
val jnaLibsPath: String = System.getProperty("jnaLibsPath", "${jnaResourcesDir}${jnaLibPlatform}")
val jnaResources: String = System.getProperty("jna.library.path", jnaLibsPath)

// Download jnaLibs that match the libmongocrypt release version to jnaResourcesBuildDir.
val downloadRevision = "1.18.1"
val downloadUrlBase = "https://github.com/mongodb/libmongocrypt/releases/download/$downloadRevision"

/**
 * Maps a JNA platform key (the directory consumed by `jna.library.path`) to the libmongocrypt GitHub release tarball
 * that ships its native library, plus the path of that library inside the tarball. The tarball name and its internal
 * layout differ per platform, so both must be tracked explicitly.
 *
 * libmongocrypt's signature assets replace the `.tar.gz` suffix with `.asc` (e.g.
 * `libmongocrypt-linux-x86_64-glibc_2_7-nocrypto-1.18.1.asc`).
 */
data class CryptBinary(val jnaPlatform: String, val tarball: String, val libPathInTarball: String) {
    val signature: String = tarball.removeSuffix(".tar.gz") + ".asc"
}

val cryptBinaries: List<CryptBinary> =
    listOf(
        CryptBinary(
            "linux-x86-64",
            "libmongocrypt-linux-x86_64-glibc_2_7-nocrypto-$downloadRevision.tar.gz",
            "lib64/libmongocrypt.so"),
        CryptBinary(
            "linux-s390x",
            "libmongocrypt-linux-s390x-glibc_2_7-nocrypto-$downloadRevision.tar.gz",
            "lib64/libmongocrypt.so"),
        CryptBinary(
            "linux-ppc64le",
            "libmongocrypt-linux-ppc64le-glibc_2_17-nocrypto-$downloadRevision.tar.gz",
            "lib64/libmongocrypt.so"),
        CryptBinary(
            "linux-aarch64",
            "libmongocrypt-linux-arm64-glibc_2_17-nocrypto-$downloadRevision.tar.gz",
            "lib64/libmongocrypt.so"),
        CryptBinary("win32-x86-64", "libmongocrypt-windows-x86_64-$downloadRevision.tar.gz", "bin/mongocrypt.dll"),
        CryptBinary("darwin", "libmongocrypt-macos-universal-$downloadRevision.tar.gz", "lib/libmongocrypt.dylib"))

sourceSets { main { java { resources { srcDirs(jnaResourcesDir) } } } }

/**
 * Public key used to sign libmongocrypt release tarballs. See:
 * https://www.mongodb.com/docs/manual/tutorial/verify-mongodb-packages/#std-label-verify-pkgs
 */
val libmongocryptPublicKeyUrl = "https://pgp.mongodb.com/libmongocrypt.pub"
val libmongocryptPublicKeyFile = "libmongocrypt.pub"

tasks.register<Download>("downloadJava") {
    src(
        cryptBinaries.flatMap { listOf("$downloadUrlBase/${it.tarball}", "$downloadUrlBase/${it.signature}") } +
            libmongocryptPublicKeyUrl)
    dest(jnaDownloadsDir)
    overwrite(true)
    /* Skip URLs whose remote artifact hasn't changed since the last download. */
    onlyIfModified(true)
}

/*
 * Verify the signature of every downloaded libmongocrypt tarball before extracting it.
 * Per DRIVERS-3441, drivers that bundle libmongocrypt must verify GPG signatures of
 * release tarballs against the official MongoDB libmongocrypt signing key.
 *
 * The keyring is kept under `build/` so this task does not touch the developer's
 * system GPG keyring and so `./gradlew clean` resets the trust state.
 */
val skipCryptVerify = providers.gradleProperty("skipCryptVerify").map { it.toBoolean() }.orElse(false)

abstract class VerifyLibmongocryptTask : DefaultTask() {
    @get:Inject abstract val execOps: ExecOperations

    @get:InputFiles abstract val tarballs: ConfigurableFileCollection
    @get:InputFiles abstract val signatures: ConfigurableFileCollection
    @get:InputFile abstract val publicKey: RegularFileProperty
    @get:Input abstract val skipVerify: Property<Boolean>
    @get:OutputDirectory abstract val gnupgHome: DirectoryProperty

    @TaskAction
    fun verify() {
        if (skipVerify.get()) {
            logger.warn(
                "SKIPPING libmongocrypt signature verification because -PskipCryptVerify=true was set. " +
                    "Do not use this for release builds.")
            return
        }

        try {
            execOps.exec {
                commandLine("gpg", "--version")
                standardOutput = ByteArrayOutputStream()
            }
        } catch (e: Exception) {
            throw GradleException(
                "gpg is required to verify libmongocrypt tarballs since 1.18.0 but was not found on PATH. " +
                        "Install gpg (e.g. `apt-get install gnupg`, `brew install gnupg`, Gpg4win on Windows), " +
                        "or pass -PskipCryptVerify=true for offline development builds.",
                e
            )
        }

        val home =
            gnupgHome.get().asFile.apply {
                deleteRecursively()
                mkdirs()
                // GPG refuses to use a homedir with permissions broader than the owner.
                setReadable(false, false)
                setReadable(true, true)
                setWritable(false, false)
                setWritable(true, true)
                setExecutable(false, false)
                setExecutable(true, true)
            }

        execOps.exec { commandLine("gpg", "--homedir", home.path, "--batch", "--import", publicKey.get().asFile.path) }

        val tarballList = tarballs.files.toList()
        val signatureList = signatures.files.toList()
        check(tarballList.size == signatureList.size) {
            "Expected each tarball to have a matching signature: ${tarballList.size} tarballs vs ${signatureList.size} signatures."
        }
        tarballList.zip(signatureList).forEach { (tarball, signature) ->
            execOps.exec {
                commandLine("gpg", "--homedir", home.path, "--batch", "--verify", signature.path, tarball.path)
            }
        }
    }
}

tasks.register<VerifyLibmongocryptTask>("verifyJava") {
    dependsOn("downloadJava")
    tarballs.from(cryptBinaries.map { "$jnaDownloadsDir/${it.tarball}" })
    signatures.from(cryptBinaries.map { "$jnaDownloadsDir/${it.signature}" })
    publicKey.set(file("$jnaDownloadsDir/$libmongocryptPublicKeyFile"))
    skipVerify.set(skipCryptVerify)
    gnupgHome.set(layout.buildDirectory.dir("jnaLibs/gnupg"))
}

tasks.register<Copy>("unzipJava") {
    /*
       Clean up the directory first if the task is not UP-TO-DATE.
       This can happen if the download revision has been changed and the archives are downloaded again.
    */
    doFirst {
        println("Clearing $jnaResourcesDir before extraction")
        delete(jnaResourcesDir)
    }
    cryptBinaries.forEach { spec ->
        from(tarTree(resources.gzip("$jnaDownloadsDir/${spec.tarball}"))) {
            include(spec.libPathInTarball)
            eachFile { path = "${spec.jnaPlatform}/${name}" }
            includeEmptyDirs = false
        }
    }
    into(jnaResourcesDir)
    dependsOn("downloadJava", "verifyJava")

    doLast {
        println("Extracted libmongocrypt $downloadRevision binaries to $jnaResourcesDir:")
        fileTree(jnaResourcesDir).files.sortedBy { it.path }.forEach { println("  $it") }
    }
}

// The `processResources` task (defined by the `java-library` plug-in) consumes files in the main
// source set.
// Add a dependency on `unzipJava`. `unzipJava` adds libmongocrypt libraries to the main source set.
tasks.processResources { mustRunAfter(tasks.named("unzipJava")) }

tasks.register("downloadJnaLibs") { dependsOn("downloadJava", "unzipJava") }

tasks.test {
    systemProperty("jna.debug_load", "true")
    systemProperty("jna.library.path", jnaResources)
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }

    doFirst {
        println("jna.library.path contents:")
        println(fileTree(jnaResources) { this.setIncludes(listOf("*.*")) }.files.joinToString(",\n  ", "  "))
    }
    dependsOn("downloadJnaLibs", "downloadJava", "unzipJava")
}

tasks.withType<AbstractPublishToMaven> {
    description =
        """$description
        | System properties:
        | =================
        |
        | jnaLibsPath    : Custom local JNA library path for inclusion into the build (rather than downloading from the libmongocrypt GitHub release)
        | gitRevision    : Optional Git Revision to download the built resources for from the libmongocrypt GitHub release.
    """.trimMargin()
}

tasks.withType<Jar> {
    // NOTE this enables depending on the mongocrypt from driver-core
    dependsOn("downloadJnaLibs")
}
