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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.internal.build.MongoDriverVersion;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.client.CrudTestHelper.repeat;
import static com.mongodb.client.WithWrapper.withWrapper;
import static com.mongodb.internal.connection.ClientMetadata.getOperatingSystemType;
import static java.util.Optional.ofNullable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.md#test-plan">spec</a>
 *
 * <p>
 * NOTE: This class also contains tests that aren't categorized as Prose tests.
 */
public class ClientMetadataTest {
    private static final String APP_NAME = "app name";
    private static final MongoDriverInformation EMPTY_MONGO_DRIVER_INFORMATION = MongoDriverInformation.builder().build();

    @Test
    public void test01ValidAws() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8")
                .withEnvironmentVariable("AWS_REGION", "us-east-2")
                .withEnvironmentVariable("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'aws.lambda', 'memory_mb': 1024, 'region': 'us-east-2'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test02ValidAzure() {
        withWrapper()
                .withEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME", "node")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'azure.func'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test03ValidGcp() {
        withWrapper()
                .withEnvironmentVariable("K_SERVICE", "servicename")
                .withEnvironmentVariable("FUNCTION_MEMORY_MB", "1024")
                .withEnvironmentVariable("FUNCTION_TIMEOUT_SEC", "60")
                .withEnvironmentVariable("FUNCTION_REGION", "us-central1")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse(
                            "{'name': 'gcp.func', 'timeout_sec': 60, 'memory_mb': 1024, 'region': 'us-central1'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test04ValidVercel() {
        withWrapper()
                .withEnvironmentVariable("VERCEL", "1")
                .withEnvironmentVariable("VERCEL_REGION", "cdg1")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'vercel', 'region': 'cdg1'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test05InvalidMultipleProviders() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8")
                .withEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME", "node")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test06InvalidLongString() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8")
                .withEnvironmentVariable("AWS_REGION", repeat(512, "a"))
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'aws.lambda'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test07InvalidWrongTypes() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8")
                .withEnvironmentVariable("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "big")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'aws.lambda'}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test08NotLambda() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "EC2")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    public void test09ValidContainerAndFaasProvider() {
        withWrapper()
                .withEnvironmentVariable("AWS_EXECUTION_ENV", "AWS_Lambda_java8")
                .withEnvironmentVariable("AWS_REGION", "us-east-2")
                .withEnvironmentVariable("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024")
                .withEnvironmentVariable("KUBERNETES_SERVICE_HOST", "1")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'name': 'aws.lambda', 'memory_mb': 1024, 'region': 'us-east-2', 'container': {'orchestrator': 'kubernetes'}}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    // Additional tests, not specified as prose tests:

    @Test
    void testKubernetesMetadataIncluded() {
        withWrapper()
                .withEnvironmentVariable("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc.cluster.local")
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.put("env", BsonDocument.parse("{'container': {'orchestrator': 'kubernetes'}}"));
                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);

                    performHello();
                });
    }

    @Test
    void testDockerMetadataIncluded() {
        try (MockedStatic<Files> pathsMockedStatic = Mockito.mockStatic(Files.class)) {
            Path path = Paths.get(File.separator + ".dockerenv");
            pathsMockedStatic.when(() -> Files.exists(path)).thenReturn(true);

            withWrapper()
                    .run(() -> {
                        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                        expected.put("env", BsonDocument.parse("{'container': {'runtime': 'docker'}}"));
                        BsonDocument actual = createActualClientMetadataDocument();
                        assertEquals(expected, actual);

                        performHello();
                    });
        }
    }

    @Test
    void testDockerAndKubernetesMetadataIncluded() {
        try (MockedStatic<Files> pathsMockedStatic = Mockito.mockStatic(Files.class)) {
            Path path = Paths.get(File.separator + "/.dockerenv");
            pathsMockedStatic.when(() -> Files.exists(path)).thenReturn(true);

            withWrapper()
                    .withEnvironmentVariable("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc.cluster.local")
                    .run(() -> {
                        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                        expected.put("env", BsonDocument.parse("{'container': {'runtime': 'docker', 'orchestrator': 'kubernetes'}}"));
                        BsonDocument actual = createActualClientMetadataDocument();
                        assertEquals(expected, actual);

                        performHello();
                    });
        }
    }

    @Test
    public void testLimitForDriverVersion() {
        // should create client metadata document and exclude the extra driver info if its too verbose
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion(repeat(512, "a"))
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("driver", expectedBase.get("driver"));

        BsonDocument actual = new ClientMetadata(APP_NAME, driverInfo).getBsonDocument();
        assertEquals(expected, actual);
    }

    @Test
    public void testLimitForPlatform() {
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform(repeat(512, "a"))
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("platform", expectedBase.get("platform"));

        BsonDocument actual = new ClientMetadata(APP_NAME, driverInfo).getBsonDocument();
        assertEquals(expected, actual);
    }

    @Test
    public void testLimitForOsName() {
        withWrapper()
                .withSystemProperty("os.name", repeat(512, "a"))
                .run(() -> {
                    BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
                    expected.getDocument("os").remove("name");

                    BsonDocument actual = createActualClientMetadataDocument();
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void testApplicationNameUnderLimit() {
        String applicationName = repeat(126, "a") + "\u00A0";
        BsonDocument client = new ClientMetadata(applicationName, EMPTY_MONGO_DRIVER_INFORMATION).getBsonDocument();
        assertEquals(applicationName, client.getDocument("application").getString("name").getValue());
    }

    @Test
    public void testApplicationNameOverLimit() {
        String applicationName = repeat(127, "a") + "\u00A0";
        assertThrows(IllegalArgumentException.class, () -> new ClientMetadata(applicationName, EMPTY_MONGO_DRIVER_INFORMATION));
    }

    @ParameterizedTest
    @CsvSource({
            APP_NAME + ", " + true,
            APP_NAME + ", " + false,
            ", " + true, // null appName
            ", " + false,
    })
    public void testCreateClientMetadataDocument(@Nullable final String appName, final boolean hasDriverInfo) {
        MongoDriverInformation driverInformation = hasDriverInfo ? createDriverInformation() : EMPTY_MONGO_DRIVER_INFORMATION;
        ClientMetadata clientMetadata = new ClientMetadata(appName, driverInformation);
        assertEquals(
                createExpectedClientMetadataDocument(appName, driverInformation),
                clientMetadata.getBsonDocument());
    }

    public static java.util.stream.Stream<Arguments> provideDriverInformation() {
        return Stream.of(
                Arguments.of("1.0", "Framework", "Framework Platform"),
                Arguments.of("1.0", "Framework", null),
                Arguments.of(null, "Framework", "Framework Platform"),
                Arguments.of(null, null, "Framework Platform"),
                Arguments.of(null, "Framework", null)
        );
    }


    @ParameterizedTest
    @MethodSource("provideDriverInformation")
    void testUpdateClientMetadataDocument(@Nullable final String driverVersion,
                                          @Nullable final String driverName,
                                          @Nullable final String driverPlatform) {
        //given
        MongoDriverInformation initialDriverInformation = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();

        ClientMetadata clientMetadata = new ClientMetadata(null, initialDriverInformation);
        BsonDocument initialClientMetadataDocument = clientMetadata.getBsonDocument();
        assertEquals(
                createExpectedClientMetadataDocument(null, initialDriverInformation),
                initialClientMetadataDocument);

        MongoDriverInformation.Builder builder;
        builder = MongoDriverInformation.builder();
        ofNullable(driverName).ifPresent(builder::driverName);
        ofNullable(driverVersion).ifPresent(builder::driverVersion);
        ofNullable(driverPlatform).ifPresent(builder::driverPlatform);
        MongoDriverInformation metadataToAppend = builder.build();

        //We pass metadataToAppend to a builder and prepend with initial driver information.
        MongoDriverInformation expectedUpdatedMetadata = MongoDriverInformation.builder(metadataToAppend)
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();

        //when
        clientMetadata.append(metadataToAppend);
        BsonDocument updatedClientMetadata = clientMetadata.getBsonDocument();

        //then
        assertEquals(
                createExpectedClientMetadataDocument(null, expectedUpdatedMetadata),
                updatedClientMetadata);
        assertNotEquals(updatedClientMetadata, initialClientMetadataDocument);
    }

    @ParameterizedTest
    @CsvSource({
            "unknown,       unknown",
            "Linux OS,      Linux",
            "Mac OS X,      Darwin",
            "Windows 10,    Windows",
            "HP-UX OS,      Unix",
            "AIX OS,        Unix",
            "Irix OS,       Unix",
            "Solaris OS,    Unix",
            "SunOS,         Unix",
            "Some Other OS, unknown",
    })
    public void testApplicationName(final String input, final String expected) {
        assertEquals(expected, getOperatingSystemType(input));
    }

    private void performHello() {
        CollectionHelper<Document> collectionHelper = new CollectionHelper<>(
                new DocumentCodec(),
                new MongoNamespace(ClusterFixture.getDefaultDatabaseName(), "test"));
        collectionHelper.hello();
    }

    private BsonDocument createActualClientMetadataDocument() {
        return new ClientMetadata(APP_NAME, EMPTY_MONGO_DRIVER_INFORMATION).getBsonDocument();
    }

    private static MongoDriverInformation createDriverInformation() {
        return MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();
    }

    private static BsonDocument createExpectedClientMetadataDocument(
            @Nullable final String appName,
            @Nullable final MongoDriverInformation driverInformation) {
        BsonDocument driverDocument = new BsonDocument()
                .append("name", new BsonString(MongoDriverVersion.NAME))
                .append("version", new BsonString(MongoDriverVersion.VERSION));
        BsonDocument osDocument = new BsonDocument()
                .append("type", new BsonString(getOperatingSystemType(System.getProperty("os.name"))))
                .append("name", new BsonString(System.getProperty("os.name")))
                .append("architecture", new BsonString(System.getProperty("os.arch")))
                .append("version", new BsonString(System.getProperty("os.version")));
        BsonDocument clientDocument = new BsonDocument();
        if (appName != null) {
            clientDocument.append("application", new BsonDocument("name", new BsonString(appName)));
        }
        clientDocument
                .append("driver", driverDocument)
                .append("os", osDocument)
                .append("platform", new BsonString("Java/" + System.getProperty("java.vendor") + "/"
                        + System.getProperty("java.runtime.version")));
        if (driverInformation != null) {
            driverDocument.append("name", new BsonString(join(
                    driverDocument.getString("name").getValue(),
                    driverInformation.getDriverNames())));
            driverDocument.append("version", new BsonString(join(
                    driverDocument.getString("version").getValue(),
                    driverInformation.getDriverVersions())));
            clientDocument.append("platform", new BsonString(join(
                    clientDocument.getString("platform").getValue(),
                    driverInformation.getDriverPlatforms())));
        }
        return clientDocument;
    }

    static BsonDocument createExpectedClientMetadataDocument(final String appName) {
        return createExpectedClientMetadataDocument(appName, null);
    }

    private static String join(final String first, final List<String> rest) {
        String separator = "|";
        ArrayList<String> result = new ArrayList<>();
        result.add(first);
        result.addAll(rest);
        return String.join(separator, result);
    }
}
