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
package com.mongodb.osgi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.felix.framework.FrameworkFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.launch.Framework;

class OsgiBundleResolutionTest {

    private static final Path PROJECT_ROOT = Paths.get(System.getProperty("projectRoot", "../.."));

    private static final String[] BUNDLE_MODULES = {
        "bson",
        "bson-record-codec",
        "mongodb-crypt",
        "driver-core",
        "bson-scala",
        "driver-sync",
        "driver-reactive-streams",
        "driver-scala",
        "driver-kotlin-sync",
        "driver-kotlin-coroutine",
        "driver-kotlin-extensions"
    };

    // JARs on the test classpath whose packages are exported from the Felix system bundle,
    // satisfying non-optional imports from the bundles under test.
    private static final String[] SYSTEM_PACKAGE_JAR_PREFIXES = {
        "reactive-streams",
        "reactor-core",
        "kotlin-stdlib",
        "kotlin-reflect",
        "kotlinx-coroutines-core",
        "kotlinx-coroutines-reactive",
        "jsr305",
        "jna"
    };

    // Eagerly computed — the classpath is fixed for the lifetime of the test JVM.
    private static final String SYSTEM_PACKAGES = buildSystemPackagesFromClasspath();

    @TempDir
    private Path cacheDir;

    private Framework framework;

    @BeforeEach
    void startFramework() throws BundleException {
        Map<String, String> config = new HashMap<>();
        config.put("org.osgi.framework.storage", cacheDir.toString());
        config.put("org.osgi.framework.storage.clean", "onFirstInit");
        config.put("felix.log.level", "1");
        if (!SYSTEM_PACKAGES.isEmpty()) {
            config.put("org.osgi.framework.system.packages.extra", SYSTEM_PACKAGES);
        }

        framework = new FrameworkFactory().newFramework(config);
        framework.start();
    }

    @AfterEach
    void stopFramework() throws BundleException, InterruptedException {
        if (framework != null) {
            framework.stop();
            FrameworkEvent event = framework.waitForStop(10_000);
            if (event.getType() == FrameworkEvent.WAIT_TIMEDOUT) {
                throw new IllegalStateException("OSGi framework did not stop within 10 seconds");
            }
        }
    }

    @Test
    void bundlesResolveWithoutOptionalDependencies() throws Exception {
        List<Bundle> installed = installAllBundles(framework.getBundleContext());

        for (Bundle bundle : installed) {
            try {
                bundle.start();
            } catch (BundleException e) {
                // Fail immediately on the first resolution error. Bundles are wired by
                // Import-Package, so an unresolved bundle (e.g. driver-core missing a
                // required import) leaves its exported packages unsatisfied for all
                // downstream bundles. Collecting further failures would only add
                // cascading noise — the first message identifies the root cause.
                fail(formatBundleFailure(bundle, e));
            }
        }
    }

    @Test
    void bundlesReportCorrectSymbolicNames() throws Exception {
        List<Bundle> installed = installAllBundles(framework.getBundleContext());

        List<String> symbolicNames = installed.stream()
                .map(Bundle::getSymbolicName)
                .collect(Collectors.toList());

        assertThat(symbolicNames).containsExactly(
                "org.mongodb.bson",
                "org.mongodb.bson-record-codec",
                "com.mongodb.crypt.capi",
                "org.mongodb.driver-core",
                "org.mongodb.scala.mongo-scala-bson",
                "org.mongodb.driver-sync",
                "org.mongodb.driver-reactivestreams",
                "org.mongodb.scala.mongo-scala-driver",
                "org.mongodb.mongodb-driver-kotlin-sync",
                "org.mongodb.mongodb-driver-kotlin-coroutine",
                "org.mongodb.mongodb-driver-kotlin-extensions");
    }

    private List<Bundle> installAllBundles(final BundleContext ctx) throws Exception {
        List<Bundle> installed = new ArrayList<>();
        for (String module : BUNDLE_MODULES) {
            File jar = findBundleJar(module);
            try (InputStream is = Files.newInputStream(jar.toPath())) {
                Bundle bundle = ctx.installBundle("file:" + jar.getAbsolutePath(), is);
                installed.add(bundle);
            }
        }
        return installed;
    }

    // Parses Felix's error message format to extract the missing package name.
    private static String formatBundleFailure(final Bundle bundle, final BundleException e) {
        String msg = e.getMessage();
        StringBuilder sb = new StringBuilder();
        sb.append("\n\n====================================================================\n");
        sb.append("BUNDLE RESOLUTION FAILURE: ").append(bundle.getSymbolicName()).append("\n");
        sb.append("====================================================================\n");

        if (msg != null && msg.contains("missing requirement")) {
            int pkgStart = msg.indexOf("osgi.wiring.package=");
            if (pkgStart >= 0) {
                String remainder = msg.substring(pkgStart + "osgi.wiring.package=".length());
                int pkgEnd = remainder.indexOf(')');
                String missingPackage = pkgEnd >= 0 ? remainder.substring(0, pkgEnd) : remainder;
                sb.append("Missing required package: ").append(missingPackage).append("\n\n");
                sb.append("FIX: Add '").append(missingPackage).append(".*;resolution:=optional' to the\n");
                sb.append("     Import-Package list in the module's build.gradle.kts\n");
            }
        }

        sb.append("\nFull error: ").append(msg);
        sb.append("\n====================================================================\n");
        return sb.toString();
    }

    private static String buildSystemPackagesFromClasspath() {
        Set<String> packages = new LinkedHashSet<>();
        String classpath = System.getProperty("java.class.path", "");

        for (String entry : classpath.split(File.pathSeparator)) {
            File file = new File(entry);
            String name = file.getName();
            if (!matchesAnyPrefix(name)) {
                continue;
            }
            if (!file.isFile() || !name.endsWith(".jar")) {
                continue;
            }
            try (JarFile jar = new JarFile(file)) {
                Manifest manifest = jar.getManifest();
                if (manifest == null) {
                    continue;
                }
                String version = manifest.getMainAttributes().getValue("Bundle-Version");
                if (version == null) {
                    version = "0.0.0";
                }
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry jarEntry = entries.nextElement();
                    String entryName = jarEntry.getName();
                    if (entryName.endsWith(".class") && entryName.contains("/")) {
                        String pkg = entryName.substring(0, entryName.lastIndexOf('/')).replace('/', '.');
                        packages.add(pkg + ";version=\"" + version + "\"");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read classpath JAR: " + file, e);
            }
        }

        return String.join(",", packages);
    }

    private static boolean matchesAnyPrefix(final String fileName) {
        for (String prefix : SYSTEM_PACKAGE_JAR_PREFIXES) {
            if (fileName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static File findBundleJar(final String module) {
        Path libsDir = PROJECT_ROOT.resolve(module).resolve("build").resolve("libs");
        assertThat(libsDir)
                .as("Build output directory for module '%s' must exist. Run ./gradlew jar first.", module)
                .isDirectory();

        try (Stream<Path> files = Files.list(libsDir)) {
            List<File> candidates = files
                    .filter(p -> p.getFileName().toString().endsWith(".jar"))
                    .filter(p -> !p.getFileName().toString().contains("-test"))
                    .filter(p -> !p.getFileName().toString().contains("-sources"))
                    .filter(p -> !p.getFileName().toString().contains("-javadoc"))
                    .map(Path::toFile)
                    .collect(Collectors.toList());

            assertThat(candidates)
                    .as("Expected exactly one main JAR in %s", libsDir)
                    .hasSize(1);

            return candidates.get(0);
        } catch (IOException e) {
            return fail("Failed to list JARs in " + libsDir + ": " + e.getMessage());
        }
    }
}
