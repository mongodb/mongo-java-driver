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
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.DnsException;
import com.mongodb.spi.dns.InetAddressResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.isStandalone;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("try")
public abstract class AbstractDnsConfigurationTest {

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Test
    public void testInetAddressResolverConfiguration() throws InterruptedException, ExecutionException, TimeoutException {
        UnknownHostException exception = new UnknownHostException();
        InetAddressResolver resolver = host -> {
            throw exception;
        };

        CompletableFuture<Throwable> exceptionReceivedFuture = new CompletableFuture<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress("some.host")))
                                .addClusterListener(new ClusterListener() {
                                    @Override
                                    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                                        ServerDescription serverDescription = event.getNewDescription().getServerDescriptions().get(0);
                                        if (serverDescription.getException() != null) {
                                            exceptionReceivedFuture.complete(serverDescription.getException());
                                        }
                                    }
                                }))
                .inetAddressResolver(resolver)
                .build();

        try (MongoClient ignored = createMongoClient(settings)) {
            Throwable exceptionReceived = exceptionReceivedFuture.get(1, SECONDS);
            assertEquals(MongoSocketException.class, exceptionReceived.getClass());
            assertEquals(exception, exceptionReceived.getCause());
        }
    }

    @ParameterizedTest(name = "InetAddressResolver should not be used to resolve IP literal {0}")
    @ValueSource(strings = {"127.0.0.1", "::1", "[0:0:0:0:0:0:0:1]"})
    public void testInetAddressResolverDoesNotResolveIpLiteral(final String ipLiteral) throws InterruptedException, ExecutionException,
            TimeoutException {
        assumeTrue(isStandalone());
        assumeTrue(getServerApi() == null);

        // should not be invoked for IP literals
        InetAddressResolver resolver = host -> {
            throw new UnknownHostException();
        };

        CompletableFuture<Boolean> serverConnectedFuture = new CompletableFuture<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(getConnectionString())
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(new ServerAddress(ipLiteral)))
                                .addClusterListener(new ClusterListener() {
                                    @Override
                                    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
                                        ServerDescription serverDescription = event.getNewDescription().getServerDescriptions().get(0);
                                        // If the resolver that throws an exception is invoked, the state will not be CONNECTED
                                        if (serverDescription.getState() == ServerConnectionState.CONNECTED) {
                                            serverConnectedFuture.complete(true);
                                        }
                                    }
                                }))
                .inetAddressResolver(resolver)
                .build();

        try (MongoClient ignored = createMongoClient(settings)) {
            assertTrue(serverConnectedFuture.get(1, SECONDS));
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
