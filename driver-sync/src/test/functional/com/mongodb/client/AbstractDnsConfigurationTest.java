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
import com.mongodb.MongoException;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.DnsException;
import com.mongodb.spi.dns.InetAddressResolver;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractDnsConfigurationTest {

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Test
    public void testInetAddressResolverConfiguration() throws InterruptedException, ExecutionException, TimeoutException {
        UnknownHostException exception = new UnknownHostException();
        InetAddressResolver resolver = host -> {
            throw exception;
        };

        CompletableFuture<UnknownHostException> exceptionReceived = new CompletableFuture<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                builder.addClusterListener(new ClusterListener() {
                    @Override
                    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                        ServerDescription serverDescription = event.getNewDescription().getServerDescriptions().get(0);
                        if (serverDescription.getException() != null) {
                            exceptionReceived.complete(exception);
                        }
                    }
                }))
                .inetAddressResolver(resolver)
                .build();

        try (MongoClient ignored = createMongoClient(settings)) {
            assertEquals(exception, exceptionReceived.get(1, SECONDS));
        }
    }

    @Test
    public void testDnsClientConfiguration() throws InterruptedException, ExecutionException, TimeoutException {
        DnsException exception = new DnsException("", new Exception());
        DnsClient dnsClient = (name, type) -> {
            throw exception;
        };

        CompletableFuture<Throwable> exceptionReceived = new CompletableFuture<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb+srv://free-java.mongodb-dev.net"))
                .applyToClusterSettings(builder ->
                        builder.addClusterListener(new ClusterListener() {
                            @Override
                            public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                                MongoException srvResolutionException = event.getNewDescription().getSrvResolutionException();
                                if (srvResolutionException != null) {
                                    exceptionReceived.complete(srvResolutionException.getCause());
                                }
                            }
                        }))
                .dnsClient(dnsClient)
                .build();

        try (MongoClient ignored = createMongoClient(settings)) {
            assertEquals(exception, exceptionReceived.get(1, SECONDS));
        }
    }
}
