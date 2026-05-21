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
import java.io.ByteArrayOutputStream
import javax.inject.Inject
import org.gradle.api.GradleException
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
// When -DjnaLibsPath is set, the user wants to use a pre-existing local copy of the libmongocrypt
// binaries instead of fetching them from the libmongocrypt GitHub release, so we skip the whole
// download / verify / extract chain.
val userSuppliedJnaLibsPath: String? = System.getProperty("jnaLibsPath")
val jnaLibsPath: String = userSuppliedJnaLibsPath ?: "${jnaResourcesDir}${jnaLibPlatform}"
val jnaResources: String = System.getProperty("jna.library.path", jnaLibsPath)

// Download the libmongocrypt per-platform tarballs (and their signatures) to jnaDownloadsDir.
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

tasks.register<Download>("downloadCryptLibs") {
    src(
        cryptBinaries.flatMap { listOf("$downloadUrlBase/${it.tarball}", "$downloadUrlBase/${it.signature}") } +
            libmongocryptPublicKeyUrl)
    dest(jnaDownloadsDir)
    /* Reuse already-downloaded files. Useful for offline builds and reduces network churn. */
    overwrite(false)
    onlyIfModified(true)

    /* Bypass entirely when the caller has supplied a local libmongocrypt directory. */
    onlyIf { userSuppliedJnaLibsPath == null }
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

    /*
     * Scratch keyring directory. Marked @Internal (not @OutputDirectory) because GnuPG leaves a
     * `S.gpg-agent` Unix domain socket inside it, which Gradle's output snapshotter cannot fingerprint
     * (`IOException: not a regular file`). The directory is genuinely ephemeral - nothing downstream
     * consumes it, and re-running gpg from scratch every time is cheap.
     */
    @get:Internal abstract val gnupgHome: DirectoryProperty

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
                e)
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

        execOps.exec {
            commandLine("gpg", "--homedir", home.path, "--batch", "--quiet", "--import", publicKey.get().asFile.path)
        }

        try {
            // Pair tarballs with signatures by basename; ConfigurableFileCollection.files is an
            // unordered Set, so zipping the two collections could mismatch pairs.
            val signaturesByName = signatures.files.associateBy { it.name }
            tarballs.files.forEach { tarball ->
                val signatureName = tarball.name.removeSuffix(".tar.gz") + ".asc"
                val signature =
                    signaturesByName[signatureName]
                        ?: throw GradleException(
                            "Missing signature $signatureName for ${tarball.name}; expected it next to the tarball.")
                execOps.exec {
                    commandLine(
                        "gpg",
                        "--homedir",
                        home.path,
                        "--batch",
                        "--quiet",
                        "--trust-model",
                        "always",
                        "--verify",
                        signature.path,
                        tarball.path)
                }
            }
        } finally {
            // Shut down gpg-agent so its leftover Unix domain socket does not accumulate or confuse
            // a subsequent run that reuses the homedir.
            try {
                execOps.exec {
                    commandLine("gpgconf", "--homedir", home.path, "--kill", "gpg-agent")
                    isIgnoreExitValue = true
                }
            } catch (_: Exception) {
                // Best-effort cleanup; not a build failure.
            }
        }
    }
}

tasks.register<VerifyLibmongocryptTask>("verifyCryptLibs") {
    dependsOn("downloadCryptLibs")
    tarballs.from(cryptBinaries.map { "$jnaDownloadsDir/${it.tarball}" })
    signatures.from(cryptBinaries.map { "$jnaDownloadsDir/${it.signature}" })
    publicKey.set(file("$jnaDownloadsDir/$libmongocryptPublicKeyFile"))
    skipVerify.set(skipCryptVerify)
    gnupgHome.set(layout.buildDirectory.dir("jnaLibs/gnupg"))

    /* Bypass entirely when the caller has supplied a local libmongocrypt directory. */
    onlyIf { userSuppliedJnaLibsPath == null }

    /* Always re-verify: gpg is cheap and never trusting a cached "signature was good" decision is safer. */
    outputs.upToDateWhen { false }
}

tasks.register<Copy>("extractCryptLibs") {
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
    dependsOn("downloadCryptLibs", "verifyCryptLibs")

    /* Bypass entirely when the caller has supplied a local libmongocrypt directory. */
    onlyIf { userSuppliedJnaLibsPath == null }

    doLast {
        println("Extracted libmongocrypt $downloadRevision binaries to $jnaResourcesDir:")
        fileTree(jnaResourcesDir).files.sortedBy { it.path }.forEach { println("  $it") }
    }
}

// The `processResources` task (defined by the `java-library` plug-in) consumes files in the main
// source set.
// Add a dependency on `extractCryptLibs`, which adds libmongocrypt libraries to the main source
// set.
tasks.processResources { mustRunAfter(tasks.named("extractCryptLibs")) }

tasks.register("downloadJnaLibs") { dependsOn("downloadCryptLibs", "verifyCryptLibs", "extractCryptLibs") }

tasks.test {
    systemProperty("jna.debug_load", "true")
    systemProperty("jna.library.path", jnaResources)
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }

    doFirst {
        println("jna.library.path contents:")
        println(fileTree(jnaResources) { this.setIncludes(listOf("*.*")) }.files.joinToString(",\n  ", "  "))
    }
    dependsOn("downloadJnaLibs")
}

tasks.withType<AbstractPublishToMaven> {
    description =
        """$description
        | System properties:
        | =================
        |
        | jnaLibsPath     : Custom local JNA library path for inclusion into the build (rather than downloading the libmongocrypt GitHub release).
        |
        | Project properties:
        | ===================
        |
        | skipCryptVerify : Pass -PskipCryptVerify=true to skip GPG verification of downloaded libmongocrypt tarballs.
        |                   Intended for offline development; do not use for release builds.
    """.trimMargin()
}

tasks.withType<Jar> {
    // NOTE this enables depending on the mongocrypt from driver-core
    dependsOn("downloadJnaLibs")
}
