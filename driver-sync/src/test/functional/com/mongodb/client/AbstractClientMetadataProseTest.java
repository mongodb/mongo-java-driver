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
import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.md#test-plan">spec</a>
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

    public static Stream<Arguments> provideDriverInformation() {
        return Stream.of(
                Arguments.of("1.0", "Framework", "Framework Platform"),
                Arguments.of("1.0", "Framework", null),
                Arguments.of(null, "Framework", "Framework Platform"),
                Arguments.of(null, "Framework", null)
        );
    }

    @ParameterizedTest
    @MethodSource("provideDriverInformation")
    void shouldAppendToPreviousMetadataWhenUpdatedAfterInitialization(@Nullable final String driverVersion,
                                                                      @Nullable final String driverName,
                                                                      @Nullable final String driverPlatform) {
        //given
        MongoDriverInformation initialWrappingLibraryDriverInformation = MongoDriverInformation.builder()
                .driverName("library")
                .driverVersion("1.2")
                .driverPlatform("Library Platform")
                .build();

        try (MongoClient mongoClient = createMongoClient(initialWrappingLibraryDriverInformation, getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder ->
                        builder.maxConnectionIdleTime(1, TimeUnit.MILLISECONDS))
                .build())) {

            //TODO change get() to orElseThrow
            BsonDocument initialClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument driverInformation = initialClientMetadata.getDocument("driver");
            String generatedDriverName = driverInformation.get("name").asString().getValue();
            String generatedVersionName = driverInformation.get("version").asString().getValue();
            String generatedPlatformName = initialClientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            updateClientMetadata(driverVersion, driverName, driverPlatform, mongoClient);

            //then
            //TODO change get() to orElseThrow
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument updatedDriverInformation = updatedClientMetadata.getDocument("driver");

            String expectedDriverName = driverName == null ? generatedDriverName : generatedDriverName + "|" + driverName;
            String expectedDriverVersion = driverVersion == null ? generatedVersionName : generatedVersionName + "|" + driverVersion;
            String expectedDriverPlatform = driverPlatform == null ? generatedPlatformName : generatedPlatformName + "|" + driverPlatform;

            assertEquals(updatedDriverInformation.getString("name").getValue(), expectedDriverName);
            assertTrue(updatedDriverInformation.getString("version").getValue().endsWith(expectedDriverVersion));
            assertTrue(updatedClientMetadata.getString("platform").getValue().endsWith(expectedDriverPlatform));

            assertEquals(
                    withRemovedKeys(updatedClientMetadata, "driver", "platform"),
                    withRemovedKeys(initialClientMetadata, "driver", "platform"));
        }
    }

    @ParameterizedTest
    @MethodSource("provideDriverInformation")
    void shouldAppendToDefaultClientMetadataWhenUpdatedAfterInitialization(@Nullable final String driverVersion,
                                                                           @Nullable final String driverName,
                                                                           @Nullable final String driverPlatform) {
        //given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder ->
                        builder.maxConnectionIdleTime(1, TimeUnit.MILLISECONDS))
                .build())) {

            //TODO change get() to orElseThrow
            BsonDocument initialClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();

            BsonDocument generatedDriverInformation = initialClientMetadata.getDocument("driver");
            String generatedDriverName = generatedDriverInformation.get("name").asString().getValue();
            String generatedVersionName = generatedDriverInformation.get("version").asString().getValue();
            String generatedPlatformName = initialClientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            updateClientMetadata(driverVersion, driverName, driverPlatform, mongoClient);

            //then
            //TODO change get() to orElseThrow
            BsonDocument updatedClientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument updatedDriverInformation = updatedClientMetadata.getDocument("driver");

            String expectedDriverName = driverName == null ? generatedDriverName : generatedDriverName + "|" + driverName;
            String expectedDriverVersion = driverVersion == null ? generatedVersionName : generatedVersionName + "|" + driverVersion;
            String expectedDriverPlatform = driverPlatform == null ? generatedPlatformName : generatedPlatformName + "|" + driverPlatform;

            assertEquals(updatedDriverInformation.getString("name").getValue(), expectedDriverName);
            assertTrue(updatedDriverInformation.getString("version").getValue().endsWith(expectedDriverVersion));
            assertTrue(updatedClientMetadata.getString("platform").getValue().endsWith(expectedDriverPlatform));

            assertEquals(
                    withRemovedKeys(updatedClientMetadata, "driver", "platform"),
                    withRemovedKeys(initialClientMetadata, "driver", "platform"));
        }
    }

    // Not a prose test. Additional test for better coverage.
    @Test
    void shouldAppendProvidedMetadatDuringInitialization() {
        //given
        MongoDriverInformation initialWrappingLibraryDriverInformation = MongoDriverInformation.builder()
                .driverName("library")
                .driverVersion("1.2")
                .driverPlatform("Library Platform")
                .build();

        try (MongoClient mongoClient = createMongoClient(initialWrappingLibraryDriverInformation, getMongoClientSettingsBuilder()
                .build())) {

            //when
            //TODO change get() to orElseThrow
            BsonDocument clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument driverInformation = clientMetadata.getDocument("driver");

            //then
            assertTrue(driverInformation.get("name").asString().getValue().endsWith("|library"));
            assertTrue(driverInformation.get("version").asString().getValue().endsWith("|1.2"));
            assertTrue(clientMetadata.get("platform").asString().getValue().endsWith("|Library Platform"));
        }
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

    private static void updateClientMetadata(@Nullable final String driverVersion,
                                             @Nullable final String driverName,
                                             @Nullable final String driverPlatform,
                                             final MongoClient mongoClient) {
        MongoDriverInformation.Builder builder;
        builder = MongoDriverInformation.builder();
        ofNullable(driverName).ifPresent(builder::driverName);
        ofNullable(driverVersion).ifPresent(builder::driverVersion);
        ofNullable(driverPlatform).ifPresent(builder::driverPlatform);
        mongoClient.appendMetadata(builder.build());
    }
}