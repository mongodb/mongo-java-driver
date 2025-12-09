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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.client.DriverInformation;
import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isAuthenticated;
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.assertions.Assertions.assertTrue;
import static java.util.Optional.ofNullable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/tests/README.md">Prose tests</a>
 */
public abstract class AbstractClientMetadataProseTest {

    private TestCommandListener commandListener;
    private TestConnectionPoolListener connectionPoolListener;

    protected abstract MongoClient createMongoClient(@Nullable MongoDriverInformation driverInformation,
                                                     MongoClientSettings mongoClientSettings);

    @BeforeEach
    public void setUp() {
        assumeFalse(isLoadBalanced());
        assumeFalse(isAuthenticated());

        commandListener = new TestCommandListener();
        connectionPoolListener = new TestConnectionPoolListener();
        InternalStreamConnection.setRecordEverything(true);
    }

    @AfterEach
    public void tearDown() {
        InternalStreamConnection.setRecordEverything(false);
    }

    @DisplayName("Test 1: Test that the driver updates metadata")
    @ParameterizedTest(name = "{index} => {arguments}")
    @MethodSource("provideDriverInformation")
    void testThatTheDriverUpdatesMetadata(final DriverInformation driverInformation) {
        //given
         try (MongoClient mongoClient = createMongoClient(getInitialMongoDriverInformation(), getMongoClientSettings())) {
             sleep(5); // wait for connection to become idle
             BsonDocument initialClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            BsonDocument generatedDriverInformation = initialClientMetadata.getDocument("driver");
            String generatedDriverName = generatedDriverInformation.get("name").asString().getValue();
            String generatedVersionName = generatedDriverInformation.get("version").asString().getValue();
            String generatedPlatformName = initialClientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(driverInformation));

            //then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);
            BsonDocument updatedDriverInformation = updatedClientMetadata.getDocument("driver");

            String driverName = driverInformation.getDriverName();
            String driverVersion = driverInformation.getDriverVersion();
            String driverPlatform = driverInformation.getDriverPlatform();
            String expectedDriverName = driverName == null ? generatedDriverName : generatedDriverName + "|" + driverName;
            String expectedDriverVersion = driverVersion == null ? generatedVersionName : generatedVersionName + "|" + driverVersion;
            String expectedDriverPlatform = driverPlatform == null ? generatedPlatformName : generatedPlatformName + "|" + driverPlatform;

            assertEquals(expectedDriverName, updatedDriverInformation.getString("name").getValue());
            assertTrue(updatedDriverInformation.getString("version").getValue().endsWith(expectedDriverVersion));
            assertTrue(updatedClientMetadata.getString("platform").getValue().endsWith(expectedDriverPlatform));

            assertEquals(
                    withRemovedKeys(updatedClientMetadata, "driver", "platform"),
                    withRemovedKeys(initialClientMetadata, "driver", "platform"));
        }
    }

    @DisplayName("Test 2: Multiple Successive Metadata Updates")
    @ParameterizedTest(name = "{index} => {arguments}")
    @MethodSource("provideDriverInformation")
    void testMultipleSuccessiveMetadataUpdates(final DriverInformation driverInformation) {
        //given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettings())) {

            mongoClient.appendMetadata(getInitialMongoDriverInformation());

            BsonDocument initialClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            BsonDocument generatedDriverInformation = initialClientMetadata.getDocument("driver");
            String generatedDriverName = generatedDriverInformation.get("name").asString().getValue();
            String generatedVersionName = generatedDriverInformation.get("version").asString().getValue();
            String generatedPlatformName = initialClientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(driverInformation));

            //then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);
            BsonDocument updatedDriverInformation = updatedClientMetadata.getDocument("driver");

            String driverName = driverInformation.getDriverName();
            String driverVersion = driverInformation.getDriverVersion();
            String driverPlatform = driverInformation.getDriverPlatform();
            String expectedDriverName = driverName == null ? generatedDriverName : generatedDriverName + "|" + driverName;
            String expectedDriverVersion = driverVersion == null ? generatedVersionName : generatedVersionName + "|" + driverVersion;
            String expectedDriverPlatform = driverPlatform == null ? generatedPlatformName : generatedPlatformName + "|" + driverPlatform;

            assertEquals(expectedDriverName, updatedDriverInformation.getString("name").getValue());
            assertTrue(updatedDriverInformation.getString("version").getValue().endsWith(expectedDriverVersion));
            assertTrue(updatedClientMetadata.getString("platform").getValue().endsWith(expectedDriverPlatform));

            assertEquals(
                    withRemovedKeys(updatedClientMetadata, "driver", "platform"),
                    withRemovedKeys(initialClientMetadata, "driver", "platform"));
        }
    }

    @DisplayName("Test 3: Multiple Successive Metadata Updates with Duplicate Data")
    @ParameterizedTest(name = "{index} => {arguments}")
    @MethodSource("provideDriverAndFrameworkInformation")
    void testMultipleSuccessiveMetadataUpdatesWithDuplicateData(final DriverInformation driverInformation) {
        //given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettings())) {
            mongoClient.appendMetadata(getInitialMongoDriverInformation());

            BsonDocument initialClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            BsonDocument generatedDriverInformation = initialClientMetadata.getDocument("driver");
            String generatedDriverName = generatedDriverInformation.get("name").asString().getValue();
            String generatedVersionName = generatedDriverInformation.get("version").asString().getValue();
            String generatedPlatformName = initialClientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(driverInformation));

            //then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);
            BsonDocument updatedDriverInformation = updatedClientMetadata.getDocument("driver");

            String expectedDriverName = generatedDriverName;
            String expectedDriverVersion = generatedVersionName;
            String expectedDriverPlatform = generatedPlatformName;

            if (!(driverInformation.equals(INITIAL_DRIVER_INFORMATION))) {
                expectedDriverName = generatedDriverName + "|" + driverInformation.getDriverName();
                expectedDriverVersion = generatedVersionName + "|" + driverInformation.getDriverVersion();
                expectedDriverPlatform = generatedPlatformName + "|" + driverInformation.getDriverPlatform();
            }

            assertEquals(expectedDriverName, updatedDriverInformation.getString("name").getValue());
            assertTrue(updatedDriverInformation.getString("version").getValue().endsWith(expectedDriverVersion));
            assertTrue(updatedClientMetadata.getString("platform").getValue().endsWith(expectedDriverPlatform));
        }
    }

    @DisplayName("Test 4: Multiple Metadata Updates with Duplicate Data")
    @Test
    void testMultipleMetadataUpdatesWithDuplicateData() {
        // given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettings())) {
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("library", "1.2", "Library Platform")));

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("framework", "2.0", "Framework Platform")));
            BsonDocument clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("library", "1.2", "Library Platform")));

            // then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);
        }
    }

    @DisplayName("Test 5: Metadata is not appended if identical to initial metadata")
    @Test
    void testMetadataIsNotAppendedIfIdenticalToInitialMetadata() {
        // given
        MongoDriverInformation initialWrappingLibraryDriverInformation = getInitialMongoDriverInformation();
        try (MongoClient mongoClient = createMongoClient(initialWrappingLibraryDriverInformation, getMongoClientSettings())) {
            //when
            BsonDocument clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("library", "1.2", "Library Platform")));

            // then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);
        }
    }

    @DisplayName("Test 6: Metadata is not appended if identical to initial metadata (separated by non-identical metadata)")
    @Test
    void testMetadataIsNotAppendedIfIdenticalToInitialMetadataSeparatedByNonIdenticalMetadata() {
        // given
        MongoDriverInformation initialWrappingLibraryDriverInformation = getInitialMongoDriverInformation();
        try (MongoClient mongoClient = createMongoClient(initialWrappingLibraryDriverInformation, getMongoClientSettings())) {
            //when
            BsonDocument clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("library", "1.2", "Library Platform")));

            // then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("framework", "1.2", "Library Platform")));

            clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(new DriverInformation("library", "1.2", "Library Platform")));

            updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);
        }
    }

    @DisplayName("Test 7: Empty strings are considered unset when appending duplicate metadata")
    @ParameterizedTest(name = "{index} => {arguments}")
    @MethodSource("provideDriverInformationWithNullsAndEmptyStrings")
    void testEmptyStringsAreConsideredUnsetWhenAppendingDuplicateMetadata(
            final DriverInformation initialDriverInformation,
            final DriverInformation updatedDriverInformation) {
        // given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettings())) {
            //when
            mongoClient.appendMetadata(getMongoDriverInformation(initialDriverInformation));

            BsonDocument clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(updatedDriverInformation));

            // then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);
        }
    }

    @DisplayName("Test 8: Empty strings are considered unset when appending metadata identical to initial metadata")
    @ParameterizedTest(name = "{index} => {arguments}")
    @MethodSource("provideDriverInformationWithNullsAndEmptyStrings")
    void testEmptyStringsAreConsideredUnsetWhenAppendingMetadataIdenticalToInitialMetadata(
            final DriverInformation initialDriverInformation,
            final DriverInformation updatedDriverInformation) {
        // given
        try (MongoClient mongoClient = createMongoClient(getMongoDriverInformation(initialDriverInformation), getMongoClientSettings())) {
            //when
            BsonDocument clientMetaData = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            // then
            sleep(5); // wait for connection to become idle
            mongoClient.appendMetadata(getMongoDriverInformation(updatedDriverInformation));

            // then
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient)
                    .orElseThrow(AbstractClientMetadataProseTest::failOnEmptyMetadata);

            assertEquals(clientMetaData, updatedClientMetadata);
        }
    }

    public static Stream<Arguments> provideDriverInformation() {
        return Stream.of(
                Arguments.of(new DriverInformation("framework", "2.0", "Framework Platform")),
                Arguments.of(new DriverInformation("framework", "2.0", null)),
                Arguments.of(new DriverInformation("framework", null,  "Framework Platform")),
                Arguments.of(new DriverInformation("framework", null,  null))
        );
    }

    public static Stream<Arguments> provideDriverAndFrameworkInformation() {
        return Stream.of(
                Arguments.of(new DriverInformation("library", "1.2", "Library Platform")),
                Arguments.of(new DriverInformation("framework", "1.2", "Library Platform")),
                Arguments.of(new DriverInformation("library", "2.0", "Library Platform")),
                Arguments.of(new DriverInformation("library", "1.2", "Framework Platform")),
                Arguments.of(new DriverInformation("framework", "2.0", "Library Platform")),
                Arguments.of(new DriverInformation("framework", "1.2", "Framework Platform")),
                Arguments.of(new DriverInformation("library", "2.0", "Framework Platform"))
        );
    }

    public static Stream<Arguments> provideDriverInformationWithNullsAndEmptyStrings() {
        return Stream.of(
                Arguments.of(new DriverInformation(null, "1.2", "Library Platform"), new DriverInformation("", "1.2", "Library Platform")),
                Arguments.of(new DriverInformation("library", null, "Library Platform"), new DriverInformation("library", "", "Library Platform")),
                Arguments.of(new DriverInformation("library", "1.2", null), new DriverInformation("library", "1.2", ""))
        );
    }


    private MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder ->
                        builder.maxConnectionIdleTime(1, TimeUnit.MILLISECONDS))
                .build();
    }

    private Optional<BsonDocument> executePingAndCaptureMetadataHandshake(final MongoClient mongoClient) {
        commandListener.reset();
        mongoClient.getDatabase("admin")
                .runCommand(BsonDocument.parse("{ping: 1}"));

        List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents("isMaster");

        if (commandStartedEvents.isEmpty()) {
            return Optional.empty();
        }
        CommandStartedEvent event = commandStartedEvents.get(0);
        BsonDocument helloCommand = event.getCommand();
        return Optional.of(helloCommand.getDocument("client"));
    }

    protected MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return Fixture.getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .applyToConnectionPoolSettings(builder ->
                        builder.addConnectionPoolListener(connectionPoolListener));
    }

    private static BsonDocument withRemovedKeys(final BsonDocument updatedClientMetadata,
                                                final String... keysToFilter) {
        BsonDocument clone = updatedClientMetadata.clone();
        for (String keyToRemove : keysToFilter) {
            clone.remove(keyToRemove);
        }
        return clone;
    }

    private static final DriverInformation INITIAL_DRIVER_INFORMATION = new DriverInformation("library", "1.2", "Library Platform");

    private static MongoDriverInformation getInitialMongoDriverInformation() {
        return getMongoDriverInformation(INITIAL_DRIVER_INFORMATION);
    }

    private static MongoDriverInformation getMongoDriverInformation(final DriverInformation driverInformation) {
        MongoDriverInformation.Builder builder = MongoDriverInformation.builder();
        ofNullable(driverInformation.getDriverName()).ifPresent(builder::driverName);
        ofNullable(driverInformation.getDriverVersion()).ifPresent(builder::driverVersion);
        ofNullable(driverInformation.getDriverPlatform()).ifPresent(builder::driverPlatform);
        return builder.build();
    }

    private static AssertionError failOnEmptyMetadata() {
        return Assertions.fail("Client metadata was expected to be present after ping command");
    }
}
