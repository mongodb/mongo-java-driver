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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AsyncTransportSettingsTest {

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("try")
    void testExternalExecutorWasShutDown(final boolean tlsEnabled) {
        ExecutorService executorService = spy(Executors.newFixedThreadPool(5));
        AsyncTransportSettings asyncTransportSettings = TransportSettings.asyncBuilder()
                .executorService(executorService)
                .build();
        MongoClientSettings mongoClientSettings = getMongoClientSettingsBuilder()
                .applyToSslSettings(builder -> builder.enabled(tlsEnabled))
                .transportSettings(asyncTransportSettings)
                .build();

        try (MongoClient ignored = new SyncMongoClient(MongoClients.create(mongoClientSettings))) {
            // ignored
        }
        ClusterFixture.sleep(100);
        verify(executorService, times(1)).shutdown();
    }
}
