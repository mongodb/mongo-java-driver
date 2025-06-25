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

package com.mongodb.internal.connection;

import com.mongodb.MongoDriverInformation;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.build.MongoDriverVersion;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.Locks.withLock;
import static com.mongodb.internal.connection.FaasEnvironment.getFaasEnvironment;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.nio.file.Paths.get;

/**
 * Represents metadata of the current MongoClient.
 *
 * Metadata is used to identify the client in the server logs and metrics.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public class ClientMetadata {
    private static final String SEPARATOR = "|";
    private static final int MAXIMUM_CLIENT_METADATA_ENCODED_SIZE = 512;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final String applicationName;
    private BsonDocument clientMetadataBsonDocument;
    private DriverInformation driverInformation;

    public ClientMetadata(@Nullable final String applicationName, final MongoDriverInformation mongoDriverInformation) {
        this.applicationName = applicationName;
        withLock(readWriteLock.writeLock(), () -> {
            this.driverInformation = DriverInformation.from(
                    mongoDriverInformation.getDriverNames(),
                    mongoDriverInformation.getDriverVersions(),
                    mongoDriverInformation.getDriverPlatforms());
            this.clientMetadataBsonDocument = createClientMetadataDocument(applicationName, driverInformation);
        });
    }

    /**
     * Returns mutable BsonDocument that represents the client metadata.
     */
    public BsonDocument getBsonDocument() {
        return withLock(readWriteLock.readLock(), () -> clientMetadataBsonDocument);
    }

    public void append(final MongoDriverInformation mongoDriverInformationToAppend) {
        withLock(readWriteLock.writeLock(), () -> {
            this.driverInformation.append(
                    mongoDriverInformationToAppend.getDriverNames(),
                    mongoDriverInformationToAppend.getDriverVersions(),
                    mongoDriverInformationToAppend.getDriverPlatforms());
            this.clientMetadataBsonDocument = createClientMetadataDocument(applicationName, driverInformation);
        });
    }

    private static BsonDocument createClientMetadataDocument(@Nullable final String applicationName,
                                                             final DriverInformation driverInformation) {
        if (applicationName != null) {
            isTrueArgument("applicationName UTF-8 encoding length <= 128",
                    applicationName.getBytes(StandardCharsets.UTF_8).length <= 128);
        }

        // client fields are added in "preservation" order:
        BsonDocument client = new BsonDocument();
        tryWithLimit(client, d -> putAtPath(d, "application.name", applicationName));

        // required fields:
        tryWithLimit(client, d -> {
            putAtPath(d, "driver.name", driverInformation.getInitialDriverName());
            putAtPath(d, "driver.version", driverInformation.getInitialDriverVersion());
        });
        tryWithLimit(client, d -> putAtPath(d, "os.type", getOperatingSystemType(getOperatingSystemName())));
        // full driver information:
        tryWithLimit(client, d -> {
            putAtPath(d, "driver.name", listToString(driverInformation.getAllDriverNames()));
            putAtPath(d, "driver.version", listToString(driverInformation.getAllDriverVersions()));
        });

        // optional fields:
        FaasEnvironment faasEnvironment =  getFaasEnvironment();
        ClientMetadata.ContainerRuntime containerRuntime = ClientMetadata.ContainerRuntime.determineExecutionContainer();
        ClientMetadata.Orchestrator orchestrator = ClientMetadata.Orchestrator.determineExecutionOrchestrator();

        tryWithLimit(client, d -> putAtPath(d, "platform", driverInformation.getInitialDriverPlatform()));
        tryWithLimit(client, d -> putAtPath(d, "platform", listToString(driverInformation.getAllDriverPlatforms())));
        tryWithLimit(client, d -> putAtPath(d, "os.name", getOperatingSystemName()));
        tryWithLimit(client, d -> putAtPath(d, "os.architecture", getProperty("os.arch", "unknown")));
        tryWithLimit(client, d -> putAtPath(d, "os.version", getProperty("os.version", "unknown")));

        tryWithLimit(client, d -> putAtPath(d, "env.name", faasEnvironment.getName()));
        tryWithLimit(client, d -> putAtPath(d, "env.timeout_sec", faasEnvironment.getTimeoutSec()));
        tryWithLimit(client, d -> putAtPath(d, "env.memory_mb", faasEnvironment.getMemoryMb()));
        tryWithLimit(client, d -> putAtPath(d, "env.region", faasEnvironment.getRegion()));

        tryWithLimit(client, d -> putAtPath(d, "env.container.runtime", containerRuntime.getName()));
        tryWithLimit(client, d -> putAtPath(d, "env.container.orchestrator", orchestrator.getName()));

        return client;
    }

    private static void putAtPath(final BsonDocument d, final String path, @Nullable final String value) {
        if (value == null) {
            return;
        }
        putAtPath(d, path, new BsonString(value));
    }

    private static void putAtPath(final BsonDocument d, final String path, @Nullable final Integer value) {
        if (value == null) {
            return;
        }
        putAtPath(d, path, new BsonInt32(value));
    }

    /**
     * Assumes valid documents (or not set) on path. No-op if value is null.
     */
    private static void putAtPath(final BsonDocument d, final String path, @Nullable final BsonValue value) {
        if (value == null) {
            return;
        }
        String[] split = path.split("\\.", 2);
        String first = split[0];
        if (split.length == 1) {
            d.append(first, value);
        } else {
            BsonDocument child;
            if (d.containsKey(first)) {
                child = d.getDocument(first);
            } else {
                child = new BsonDocument();
                d.append(first, child);
            }
            String rest = split[1];
            putAtPath(child, rest, value);
        }
    }

    private static void tryWithLimit(final BsonDocument document, final Consumer<BsonDocument> modifier) {
        try {
            BsonDocument temp = document.clone();
            modifier.accept(temp);
            if (!clientMetadataDocumentTooLarge(temp)) {
                modifier.accept(document);
            }
        } catch (Exception e) {
            // do nothing. This could be a SecurityException, or any other issue while building the document
        }
    }

    static boolean clientMetadataDocumentTooLarge(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer(MAXIMUM_CLIENT_METADATA_ENCODED_SIZE);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        return buffer.getPosition() > MAXIMUM_CLIENT_METADATA_ENCODED_SIZE;
    }

    private enum ContainerRuntime {
        DOCKER("docker") {
            @Override
            boolean isCurrentRuntimeContainer() {
                try {
                    return Files.exists(get(File.separator + ".dockerenv"));
                } catch (Exception e) {
                    return false;
                    // NOOP. This could be a SecurityException.
                }
            }
        },
        UNKNOWN(null);

        @Nullable
        private final String name;

        ContainerRuntime(@Nullable final String name) {
            this.name = name;
        }

        @Nullable
        public String getName() {
            return name;
        }

        boolean isCurrentRuntimeContainer() {
            return false;
        }

        static ClientMetadata.ContainerRuntime determineExecutionContainer() {
            for (ClientMetadata.ContainerRuntime allegedContainer : ClientMetadata.ContainerRuntime.values()) {
                if (allegedContainer.isCurrentRuntimeContainer()) {
                    return allegedContainer;
                }
            }
            return UNKNOWN;
        }
    }

    private enum Orchestrator {
        K8S("kubernetes") {
            @Override
            boolean isCurrentOrchestrator() {
                return FaasEnvironment.getEnv("KUBERNETES_SERVICE_HOST") != null;
            }
        },
        UNKNOWN(null);

        @Nullable
        private final String name;

        Orchestrator(@Nullable final String name) {
            this.name = name;
        }

        @Nullable
        public String getName() {
            return name;
        }

        boolean isCurrentOrchestrator() {
            return false;
        }

        static ClientMetadata.Orchestrator determineExecutionOrchestrator() {
            for (ClientMetadata.Orchestrator alledgedOrchestrator : ClientMetadata.Orchestrator.values()) {
                if (alledgedOrchestrator.isCurrentOrchestrator()) {
                    return alledgedOrchestrator;
                }
            }
            return UNKNOWN;
        }
    }

    private static String listToString(final List<String> listOfStrings) {
        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (String val : listOfStrings) {
            if (i > 0) {
                stringBuilder.append(SEPARATOR);
            }
            stringBuilder.append(val);
            i++;
        }
        return stringBuilder.toString();
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public static String getOperatingSystemType(final String operatingSystemName) {
        if (nameStartsWith(operatingSystemName, "linux")) {
            return "Linux";
        } else if (nameStartsWith(operatingSystemName, "mac")) {
            return "Darwin";
        } else if (nameStartsWith(operatingSystemName, "windows")) {
            return  "Windows";
        } else if (nameStartsWith(operatingSystemName, "hp-ux", "aix", "irix", "solaris", "sunos")) {
            return "Unix";
        } else {
            return "unknown";
        }
    }

    private static String getOperatingSystemName() {
        return getProperty("os.name", "unknown");
    }

    private static boolean nameStartsWith(final String name, final String... prefixes) {
        for (String prefix : prefixes) {
            if (name.toLowerCase().startsWith(prefix.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Holds driver information of client.driver field
     * in {@link ClientMetadata#clientMetadataBsonDocument}.
     */
    private static class DriverInformation {
        private final List<String> driverNames;
        private final List<String> driverVersions;
        private final List<String> driverPlatforms;
        private final String initialPlatform;

        DriverInformation() {
            this.driverNames = new ArrayList<>();
            driverNames.add(MongoDriverVersion.NAME);

            this.driverVersions = new ArrayList<>();
            driverVersions.add(MongoDriverVersion.VERSION);

            this.initialPlatform = format("Java/%s/%s", getProperty("java.vendor", "unknown-vendor"),
                    getProperty("java.runtime.version", "unknown-version"));
            this.driverPlatforms = new ArrayList<>();
            driverPlatforms.add(initialPlatform);
        }

        static DriverInformation from(final List<String> driverNames,
                                      final List<String> driverVersions,
                                      final List<String> driverPlatforms) {
            DriverInformation driverInformation = new DriverInformation();
            return driverInformation.append(driverNames, driverVersions, driverPlatforms);
        }

        DriverInformation append(final List<String> driverNames,
                                 final List<String> driverVersions,
                                 final List<String> driverPlatforms) {
            this.driverNames.addAll(driverNames);
            this.driverVersions.addAll(driverVersions);
            this.driverPlatforms.addAll(driverPlatforms);
            return this;
        }

        public String getInitialDriverPlatform() {
            return initialPlatform;
        }

        public String getInitialDriverName() {
            return MongoDriverVersion.NAME;
        }

        public String getInitialDriverVersion() {
            return MongoDriverVersion.VERSION;
        }

        public List<String> getAllDriverNames() {
            return driverNames;
        }

        public List<String> getAllDriverVersions() {
            return driverVersions;
        }

        public List<String> getAllDriverPlatforms() {
            return driverPlatforms;
        }
    }
}
