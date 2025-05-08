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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.sleep;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assumeFalse(ClusterFixture.isLoadBalanced());

        commandListener = new TestCommandListener();
        connectionPoolListener = new TestConnectionPoolListener();
        InternalStreamConnection.setRecordEverything(true);
    }

    @Test
    void shouldAppendToPreviousMetadataWhenUpdatedAfterInitialization() {
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
            BsonDocument clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument driverInformation = clientMetadata.getDocument("driver");
            String generatedDriverName = driverInformation.get("name").asString().getValue();
            String generatedVersionName = driverInformation.get("version").asString().getValue();
            String generatedPlatformName = clientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.updateMetadata(MongoDriverInformation.builder()
                    .driverVersion("1.0")
                    .driverName("Framework")
                    .driverPlatform("Framework Platform")
                    .build());

            //then
            //TODO change get() to orElseThrow
            clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            driverInformation = clientMetadata.getDocument("driver");

            assertThat(driverInformation.getString("name").getValue()).isEqualTo(generatedDriverName + "|Framework");
            assertThat(driverInformation.getString("version").getValue()).isEqualTo(generatedVersionName + "|1.0");
            assertThat(clientMetadata.getString("platform").getValue()).isEqualTo(generatedPlatformName + "|Framework Platform");
        }
    }

    @Test
    void shouldAppendToDefaultClientMetadataWhenUpdatedAfterInitialization() {
        //given
        try (MongoClient mongoClient = createMongoClient(null, getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder ->
                        builder.maxConnectionIdleTime(1, TimeUnit.MILLISECONDS))
                .build())) {

            //TODO change get() to orElseThrow
            BsonDocument clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();

            BsonDocument generatedDriverInformation = clientMetadata.getDocument("driver");
            String generatedDriverName = generatedDriverInformation.get("name").asString().getValue();
            String generatedVersionName = generatedDriverInformation.get("version").asString().getValue();
            String generatedPlatformName = clientMetadata.get("platform").asString().getValue();

            //when
            sleep(5); // wait for connection to become idle
            mongoClient.updateMetadata(MongoDriverInformation.builder()
                    .driverVersion("1.0")
                    .driverName("Framework")
                    .driverPlatform("Framework Platform")
                    .build());

            //then
            //TODO change get() to orElseThrow
            clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            generatedDriverInformation = clientMetadata.getDocument("driver");

            assertThat(generatedDriverInformation.getString("name").getValue()).isEqualTo(generatedDriverName + "|Framework");
            assertThat(generatedDriverInformation.getString("version").getValue()).endsWith(generatedVersionName + "|1.0");
            assertThat(clientMetadata.getString("platform").getValue()).endsWith(generatedPlatformName + "|Framework Platform");
        }
    }

    @Test
    void shouldNotCloseExistingConnectionsToUpdateMetadata() {
        //given
        MongoDriverInformation initialWrappingLibraryDriverInformation = MongoDriverInformation.builder()
                .driverName("library")
                .driverVersion("1.2")
                .driverPlatform("Library Platform")
                .build();

        try (MongoClient mongoClient = createMongoClient(initialWrappingLibraryDriverInformation, getMongoClientSettingsBuilder()
                .build())) {

            //TODO change get() to orElseThrow
            BsonDocument clientMetadata = executePingAndCaptureMetadataHandshake(mongoClient).get();
            BsonDocument driverInformation = clientMetadata.getDocument("driver");
            String generatedDriverName = driverInformation.get("name").asString().getValue();
            String generatedVersionName = driverInformation.get("version").asString().getValue();
            String generatedPlatformName = clientMetadata.get("platform").asString().getValue();

            //when
            mongoClient.updateMetadata(initialWrappingLibraryDriverInformation);

            //then
            assertThat(executePingAndCaptureMetadataHandshake(mongoClient)).isEmpty();
            driverInformation = clientMetadata.getDocument("driver");

            assertThat(driverInformation.getString("name").getValue()).isEqualTo(generatedDriverName);
            assertThat(driverInformation.getString("version").getValue()).isEqualTo(generatedVersionName);
            assertThat(clientMetadata.getString("platform").getValue()).isEqualTo(generatedPlatformName);

            assertFalse(connectionPoolListener.getEvents().stream().anyMatch(ConnectionClosedEvent.class::isInstance),
                    "Expected no connection closed events");
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

    @AfterEach
    public void tearDown() {
        InternalStreamConnection.setRecordEverything(false);
    }
}

