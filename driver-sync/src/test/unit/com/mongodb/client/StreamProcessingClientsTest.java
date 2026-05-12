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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamProcessingClientsTest {

    private static final String WORKSPACE_URI =
            "mongodb://user:pass@atlas-stream-68f93575a1b17c4d20fb60cb-y7ufzk.virginia-usa.a.query.mongodb-qa.net/";

    @ParameterizedTest
    @ValueSource(strings = {
            "mongodb://localhost/",
            "mongodb://cluster0.example.mongodb.net/",
            "mongodb://user:pass@hostname.mongodb.net/"
    })
    @DisplayName("Non-workspace connection string throws IllegalArgumentException")
    void nonWorkspaceUriThrows(final String uri) {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> StreamProcessingClients.create(uri));
        assertTrue(ex.getMessage().contains("Atlas Stream Processing workspace"),
                "Exception message should mention Atlas Stream Processing workspace");
    }

    @Test
    @DisplayName("Non-workspace ConnectionString object throws IllegalArgumentException")
    void nonWorkspaceConnectionStringObjectThrows() {
        ConnectionString cs = new ConnectionString("mongodb://localhost/");
        assertThrows(IllegalArgumentException.class, () -> StreamProcessingClients.create(cs));
    }

    @Test
    @DisplayName("Workspace URI returns a StreamProcessingClient instance")
    void workspaceUriReturnsClient() {
        try (StreamProcessingClient client = StreamProcessingClients.create(WORKSPACE_URI)) {
            assertInstanceOf(StreamProcessingClient.class, client);
        }
    }

    @Test
    @DisplayName("Workspace ConnectionString returns a StreamProcessingClient instance")
    void workspaceConnectionStringReturnsClient() {
        ConnectionString cs = new ConnectionString(WORKSPACE_URI);
        assertTrue(cs.isAtlasStreamProcessingWorkspace());
        try (StreamProcessingClient client = StreamProcessingClients.create(cs)) {
            assertInstanceOf(StreamProcessingClient.class, client);
        }
    }

    @Test
    @DisplayName("create(MongoClientSettings) with LOAD_BALANCED settings succeeds")
    void settingsBasedCreateSucceeds() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(b -> b
                        .mode(ClusterConnectionMode.LOAD_BALANCED)
                        .applyConnectionString(new ConnectionString(WORKSPACE_URI)))
                .build();
        assertDoesNotThrow(() -> {
            try (StreamProcessingClient client = StreamProcessingClients.create(settings)) {
                assertInstanceOf(StreamProcessingClient.class, client);
            }
        });
    }

    @Test
    @DisplayName("close() is idempotent — double-close does not throw")
    void closeIsIdempotent() {
        StreamProcessingClient client = StreamProcessingClients.create(WORKSPACE_URI);
        assertDoesNotThrow(() -> {
            client.close();
            client.close();
        });
    }
}
