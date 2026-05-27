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

tasks.clean { delete(rootProject.file("build/jnaLibs")) }

val jnaLibPlatform: String =
    if (com.sun.jna.Platform.RESOURCE_PREFIX.startsWith("darwin")) "darwin" else com.sun.jna.Platform.RESOURCE_PREFIX
// When -DjnaLibsPath is set, the user wants to use a pre-existing local copy of the libmongocrypt
// binaries instead of fetching them from the libmongocrypt GitHub release, so we skip the whole
// download / verify / extract chain.
val userSuppliedJnaLibsPath: String? = System.getProperty("jnaLibsPath")
val jnaLibsPath: String = userSuppliedJnaLibsPath ?: "${jnaResourcesDir}/${jnaLibPlatform}"
val jnaResources: String = System.getProperty("jna.library.path", jnaLibsPath)

// Download the libmongocrypt per-platform tarballs (and their signatures) to jnaDownloadsDir.
// To upgrade: change downloadRevision, run `./gradlew clean downloadJnaLibs`, and verify the build.
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
    quiet(true)

    /* Bypass entirely when the caller has supplied a local libmongocrypt directory. */
    onlyIf { userSuppliedJnaLibsPath == null }

    doFirst {
        val missing = cryptBinaries.filter { !file("$jnaDownloadsDir/${it.tarball}").exists() }
        if (missing.isNotEmpty()) {
            logger.lifecycle("Downloading libmongocrypt $downloadRevision binaries:")
            missing.forEach { logger.lifecycle("  ${it.tarball}") }
        }
    }
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
    @get:Input abstract val expectedFingerprint: Property<String>
    @get:OutputFile abstract val verificationStamp: RegularFileProperty

    /* Scratch keyring directory. Marked @Internal (not @OutputDirectory) because the directory is
     * genuinely ephemeral - nothing downstream consumes it. */
    @get:Internal abstract val gnupgHome: DirectoryProperty

    @TaskAction
    fun verify() {
        if (skipVerify.get()) {
            logger.warn(
                "SKIPPING libmongocrypt signature verification because -PskipCryptVerify=true was set. " +
                    "Do not use this for release builds.")
            verificationStamp.get().asFile.writeText("Skipped verification at ${System.currentTimeMillis()}")
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
            commandLine(
                "gpg",
                "--homedir",
                home.path,
                "--batch",
                "--quiet",
                "--no-autostart",
                "--import",
                publicKey.get().asFile.path)
            standardOutput = ByteArrayOutputStream()
            errorOutput = ByteArrayOutputStream()
        }

        val fingerprintOutput = ByteArrayOutputStream()
        execOps.exec {
            commandLine(
                "gpg",
                "--homedir",
                home.path,
                "--batch",
                "--no-autostart",
                "--with-colons",
                "--fingerprint",
                expectedFingerprint.get())
            standardOutput = fingerprintOutput
            errorOutput = ByteArrayOutputStream()
        }
        val expected = expectedFingerprint.get()
        if (!fingerprintOutput.toString().contains("fpr:::::::::$expected:")) {
            throw GradleException(
                "Imported libmongocrypt signing key fingerprint does not match expected value $expected. " +
                    "The downloaded public key may have been rotated.")
        }

        // Pair tarballs with signatures by basename; ConfigurableFileCollection.files is an
        // unordered Set, so zipping the two collections could mismatch pairs.
        val signaturesByName = signatures.files.associateBy { it.name }
        tarballs.files.forEach { tarball ->
            val signatureName = tarball.name.removeSuffix(".tar.gz") + ".asc"
            val signature =
                signaturesByName[signatureName]
                    ?: throw GradleException(
                        "Missing signature $signatureName for ${tarball.name}; expected it next to the tarball.")
            val verifyErr = ByteArrayOutputStream()
            try {
                execOps.exec {
                    commandLine(
                        "gpg",
                        "--homedir",
                        home.path,
                        "--batch",
                        "--quiet",
                        "--no-autostart",
                        "--trust-model",
                        "always",
                        "--verify",
                        signature.path,
                        tarball.path)
                    standardOutput = ByteArrayOutputStream()
                    errorOutput = verifyErr
                }
            } catch (e: Exception) {
                throw GradleException(
                    "GPG signature verification failed for ${tarball.name}:\n${verifyErr.toString().trim()}", e)
            }
        }

        verificationStamp
            .get()
            .asFile
            .writeText(
                "verified=${System.currentTimeMillis()}\n" + "tarballs=${tarballs.files.joinToString { it.name }}\n")
    }
}

tasks.register<VerifyLibmongocryptTask>("verifyCryptLibs") {
    dependsOn("downloadCryptLibs")
    tarballs.from(cryptBinaries.map { "$jnaDownloadsDir/${it.tarball}" })
    signatures.from(cryptBinaries.map { "$jnaDownloadsDir/${it.signature}" })
    publicKey.set(file("$jnaDownloadsDir/$libmongocryptPublicKeyFile"))
    skipVerify.set(skipCryptVerify)
    expectedFingerprint.set("F2F5BF4ABF517E039AFCADAA81F1404DEBACA586")
    gnupgHome.set(layout.buildDirectory.dir("jnaLibs/gnupg"))
    verificationStamp.set(layout.buildDirectory.file("jnaLibs/verified.stamp"))

    /* Bypass entirely when the caller has supplied a local libmongocrypt directory. */
    onlyIf { userSuppliedJnaLibsPath == null }
}

tasks.register<Sync>("extractCryptLibs") {
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
}

// The `processResources` task (defined by the `java-library` plug-in) consumes files in the main
// source set. Extraction must complete first so the native libraries are present.
tasks.processResources { dependsOn("extractCryptLibs") }

tasks.register("downloadJnaLibs") { dependsOn("downloadCryptLibs", "verifyCryptLibs", "extractCryptLibs") }

tasks.test {
    systemProperty("jna.debug_load", "true")
    systemProperty("jna.library.path", jnaResources)
    useJUnitPlatform()
    testLogging { events("passed", "skipped", "failed") }

    doFirst {
        logger.lifecycle("jna.library.path contents:")
        logger.lifecycle(
            fileTree(jnaResources) { this.setIncludes(listOf("**/*.*")) }.files.joinToString(",\n  ", "  "))
    }
    dependsOn("downloadJnaLibs")
}

tasks.withType<AbstractPublishToMaven> {
    description =
        """$description
        | System properties:
        | =================
        |
        | jnaLibsPath     : Custom local JNA library path to use at runtime (bypasses downloading/verifying/extracting libmongocrypt release artifacts).
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
