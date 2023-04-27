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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.internal.NoCheckedAutoCloseable;
import com.mongodb.internal.capi.MongoCryptHelper;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.AbstractClientSideEncryptionTest.cryptSharedLibPathSysPropValue;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.unified.UnifiedClientEncryptionHelper.localKmsProviderKey;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#bypass-creating-mongocryptd-client-when-shared-library-is-loaded">
 * 20. Bypass creating mongocryptd client when shared library is loaded</a>.
 */
public abstract class AbstractClientSideEncryptionNotCreateMongocryptdClientTest {
    @Nullable
    private static final String CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE = cryptSharedLibPathSysPropValue().orElse(null);
    private static final int DEFAULT_MONGOCRYPTD_PORT = MongoCryptHelper.createMongocryptdClientSettings(null)
            .getClusterSettings().getHosts().get(0).getPort();
    private static final String LOCAL_KMS_PROVIDER_ID = "local";
    private static final Duration TIMEOUT = Duration.ofMillis(1_000);
    private static final MongoNamespace KEY_VAULT_NAMESPACE = new MongoNamespace("keyvault", "datakeys");

    private MongoClient client;
    private MongoCollection<Document> collection;
    private ConnectionTracker mongocryptdConnectionTracker;

    @BeforeEach
    public void setUp() throws Exception {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue(CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE != null);
        mongocryptdConnectionTracker = ConnectionTracker.start();
        client = createMongoClient(MongoClientSettings.builder(getMongoClientSettings())
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .kmsProviders(singletonMap(LOCAL_KMS_PROVIDER_ID, singletonMap("key", localKmsProviderKey())))
                        .keyVaultNamespace(KEY_VAULT_NAMESPACE.getFullName())
                        .extraOptions(Stream.of(
                                new SimpleImmutableEntry<>("cryptSharedLibPath", CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE),
                                new SimpleImmutableEntry<>("mongocryptdURI", format("mongodb://%s:%d/db?serverSelectionTimeoutMS=%d",
                                        mongocryptdConnectionTracker.serverSocket().getInetAddress().getHostAddress(),
                                        mongocryptdConnectionTracker.serverSocket().getLocalPort(),
                                        TIMEOUT.toMillis()))
                        ).collect(toMap(Entry::getKey, Entry::getValue)))
                        .build())
                .build());
        client.getDatabase(KEY_VAULT_NAMESPACE.getDatabaseName()).drop();
        MongoDatabase db = client.getDatabase("db");
        db.drop();
        collection = db.getCollection("coll");
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() throws Exception {
        //noinspection unused
        try (ConnectionTracker autoClosed = mongocryptdConnectionTracker;
             MongoClient autoClosed2 = client) {
            // empty
        }
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Test
    @SuppressWarnings("try")
    void whenCryptSharedLoaded() throws Exception {
        //noinspection unused
        try (AutoCloseable assertNoConnectionsOnAutoCloseToPreserveAssertionFailure = mongocryptdConnectionTracker) {
            collection.insertOne(Document.parse("{unencrypted: 'test'}"));
        }
    }

    static int findAvailableMongocryptdLoopbackPort() {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            int foundPort = serverSocket.getLocalPort();
            if (foundPort != DEFAULT_MONGOCRYPTD_PORT) {
                return foundPort;
            } else {
                return findAvailableMongocryptdLoopbackPort();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find an available port", e);
        }
    }

    private static final class ConnectionTracker implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final ExecutorService executor;
        private final Future<?> failOnConnect;
        private boolean active;

        private ConnectionTracker(
                final ServerSocket serverSocket, final ExecutorService executor, final Future<?> failOnConnect) {
            this.serverSocket = serverSocket;
            this.executor = executor;
            this.failOnConnect = failOnConnect;
            active = true;
        }

        ServerSocket serverSocket() {
            return serverSocket;
        }

        static ConnectionTracker start() throws Exception {
            ServerSocket serverSocket = new ServerSocket();
            try {
                serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), findAvailableMongocryptdLoopbackPort()));
                ExecutorService executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("NotConnectNotSpawnMongocryptd"));
                try {
                    return start(serverSocket, executor);
                } catch (Exception e) {
                    executor.shutdownNow();
                    throw e;
                }
            } catch (Exception e) {
                serverSocket.close();
                throw e;
            }
        }

        @SuppressWarnings("try")
        private static ConnectionTracker start(final ServerSocket serverSocket, final ExecutorService executor) throws Exception {
            CompletableFuture<Void> confirmListening = new CompletableFuture<>();
            Future<?> failOnConnect = executor.submit(() -> {
                try {
                    //noinspection unused
                    try (Socket expectedIgnoredSocket = serverSocket.accept()) {
                        confirmListening.complete(null);
                    }
                    try (Socket unexpectedSocket = serverSocket.accept()) {
                        fail(format("Fake mongocryptd bound to %s received a connection from %s",
                                unexpectedSocket.getLocalSocketAddress(), unexpectedSocket.getRemoteSocketAddress()));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            try (Socket socket = new Socket()) {
                socket.connect(serverSocket.getLocalSocketAddress(), toIntExact(TIMEOUT.toMillis()));
                confirmListening.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            return new ConnectionTracker(serverSocket, executor, failOnConnect);
        }

        private void assertNoConnections() {
            try {
                failOnConnect.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException expected) {
                // expected
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                fail(cause == null ? e : cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
            } catch (Exception e) {
                fail(e);
            }
        }

        @Override
        @SuppressWarnings("try")
        public void close() throws IOException {
            if (active) {
                active = false;
                //noinspection unused
                try (NoCheckedAutoCloseable autoClosed = executor::shutdownNow;
                     ServerSocket autoClosed1 = serverSocket) {
                    assertNoConnections();
                }
            }
        }
    }
}
