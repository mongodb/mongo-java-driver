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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class AsyncTransportSettingsTest {
    @Test
    public void shouldDefaultAllValuesToNull() {
        AsyncTransportSettings settings = TransportSettings.asyncBuilder().build();

        assertNull(settings.getExecutorService());
    }

    @Test
    public void shouldApplySettingsFromBuilder() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        AsyncTransportSettings settings = TransportSettings.asyncBuilder()
                .executorService(executorService)
                .build();

        assertEquals(executorService, settings.getExecutorService());
    }

    @Test
    void testAsyncTransportSettings() {
        ExecutorService executorService = spy(Executors.newFixedThreadPool(5));
        AsyncTransportSettings asyncTransportSettings = TransportSettings.asyncBuilder()
                .executorService(executorService)
                .build();
        MongoClientSettings mongoClientSettings = getMongoClientSettingsBuilder()
                .transportSettings(asyncTransportSettings)
                .build();

        try (MongoClient client = new SyncMongoClient(MongoClients.create(mongoClientSettings))) {
            client.listDatabases().first();
        }
        verify(executorService, atLeastOnce()).execute(any());
    }
}
