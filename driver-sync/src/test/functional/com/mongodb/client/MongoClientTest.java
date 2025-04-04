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
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.ClusterId;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.mockito.MongoMockito;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;

class MongoClientTest {

    @SuppressWarnings("try")
    @Test
    void shouldIncludeApplicationNameInClusterId() throws InterruptedException,
            ExecutionException, TimeoutException {
        CompletableFuture<ClusterId> clusterIdFuture = new CompletableFuture<>();
        ClusterListener clusterListener = new ClusterListener() {
            @Override
            public void clusterOpening(final ClusterOpeningEvent event) {
                clusterIdFuture.complete(event.getClusterId());
            }
        };
        String applicationName = "test";
        try (MongoClient ignored = MongoClients.create(getMongoClientSettingsBuilder()
                .applicationName(applicationName)
                .applyToClusterSettings(builder -> builder.addClusterListener(clusterListener))
                .build())) {
            ClusterId clusterId = clusterIdFuture.get(ClusterFixture.TIMEOUT, TimeUnit.SECONDS);
            assertEquals(applicationName, clusterId.getDescription());
        }
    }

    @Test
    void shouldCloseExternalResources() throws Exception {

        //given
        Cluster cluster = MongoMockito.mock(
                Cluster.class,
                mockedCluster -> {
                    doNothing().when(mockedCluster).close();
                });
        AutoCloseable externalResource = MongoMockito.mock(
                AutoCloseable.class,
                mockedExternalResource -> {
                    try {
                        doNothing().when(mockedExternalResource).close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        MongoClientImpl mongoClient = new MongoClientImpl(
                cluster,
                MongoClientSettings.builder().build(),
                MongoDriverInformation.builder().build(),
                externalResource);

        //when
        mongoClient.close();

        //then
        Mockito.verify(externalResource).close();
        Mockito.verify(cluster).close();
    }
}
