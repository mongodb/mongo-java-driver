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

package com.mongodb.internal.connection;

import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.RequestCallbackResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.TestListener;
import com.mongodb.event.CommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.OIDC_CALLBACK_KEY;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.ThreadTestHelpers.executeAll;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/auth/tests/mongodb-oidc.rst#mongodb-oidc">Prose Tests</a>.
 */
public class OidcAuthenticationProseTests {

    public static boolean oidcTestsEnabled() {
        return Boolean.parseBoolean(getenv().get("OIDC_TESTS_ENABLED"));
    }

    private static final String AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";

    private String appName;

    protected static String getOidcUri() {
        ConnectionString cs = ClusterFixture.getConnectionString();
        // remove username and password
        return "mongodb+srv://" + cs.getHosts().get(0) + "/?authMechanism=MONGODB-OIDC";
    }

    private static String getAwsOidcUri() {
        return getOidcUri() + "&authMechanismProperties=PROVIDER_NAME:aws";
    }

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    @BeforeEach
    public void beforeEach() {
        assumeTrue(oidcTestsEnabled());
        InternalStreamConnection.setRecordEverything(true);
        this.appName = this.getClass().getSimpleName() + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    @AfterEach
    public void afterEach() {
        InternalStreamConnection.setRecordEverything(false);
    }

    @Test
    public void test1p1CallbackIsCalledDuringAuth() {
        // #. Create a ``MongoClient`` configured with an OIDC callback...
        TestCallback onRequest = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
        assertEquals(1, onRequest.invocations.get());
    }

    @Test
    public void test1p2CallbackCalledOnceForMultipleConnections() {
        TestCallback onRequest = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Thread t = new Thread(() -> performFind(mongoClient));
                t.setDaemon(true);
                t.start();
                threads.add(t);
            }
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        assertEquals(1, onRequest.invocations.get());
    }

    @Test
    public void test2p1ValidCallbackInputs() {
        String connectionString = getOidcUri();
        Duration expectedSeconds = Duration.ofMinutes(5);

        TestCallback onRequest = createCallback();
        // #. Verify that the request callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        OidcRequestCallback onRequest2 = (context) -> {
            assertEquals(expectedSeconds, context.getTimeout());
            return onRequest.onRequest(context);
        };
        MongoClientSettings clientSettings = createSettings(connectionString, onRequest2);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            // callback was called
            assertEquals(1, onRequest.getInvocations());
        }
    }

    @Test
    public void test2p2RequestCallbackReturnsNull() {
        //noinspection ConstantConditions
        OidcRequestCallback onRequest = (context) -> null;
        MongoClientSettings settings = this.createSettings(getOidcUri(), onRequest, null);
        performFind(settings, MongoConfigurationException.class, "Result of callback must not be null");
    }

    @Test
    public void test2p3CallbackReturnsMissingData() {
        // #. Create a client with a request callback that returns data not
        //    conforming to the OIDCRequestTokenResult with missing field(s).
        OidcRequestCallback onRequest = (context) -> {
            //noinspection ConstantConditions
            return new RequestCallbackResult(null);
        };
        // we ensure that the error is propagated
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            try {
                performFind(mongoClient);
                fail();
            } catch (Exception e) {
                assertCause(IllegalArgumentException.class, "accessToken can not be null", e);
            }
        }
    }

    @Test
    public void test2p4InvalidClientConfigurationWithCallback() {
        String awsOidcUri = getAwsOidcUri();
        MongoClientSettings settings = createSettings(
                awsOidcUri, createCallback(), null);
        try {
            performFind(settings);
            fail();
        } catch (Exception e) {
            assertCause(IllegalArgumentException.class,
                    "OIDC_CALLBACK must not be specified when PROVIDER_NAME is specified", e);
        }
    }

    @Test
    public void test3p1AuthFailsWithCachedToken() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        TestCallback onRequestWrapped = createCallback();
        CompletableFuture<String> poisonToken = new CompletableFuture<>();
        OidcRequestCallback onRequest = (context) -> {
            RequestCallbackResult result = onRequestWrapped.onRequest(context);
            String accessToken = result.getAccessToken();
            if (!poisonToken.isDone()) {
                poisonToken.complete(accessToken);
            }
            return result;
        };

        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // populate cache
            performFind(mongoClient);
            assertEquals(1, onRequestWrapped.invocations.get());
            // Poison the *Client Cache* with an invalid access token.
            // uses reflection
            String poisonString = poisonToken.get();
            Field f = String.class.getDeclaredField("value");
            f.setAccessible(true);
            byte[] poisonChars = (byte[]) f.get(poisonString);
            poisonChars[0] = '~';
            poisonChars[1] = '~';

            assertEquals(1, onRequestWrapped.invocations.get());

            // cause another connection to be opened
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
        }
        assertEquals(2, onRequestWrapped.invocations.get());
    }

    @Test
    public void test3p2AuthFailsWithoutCachedToken() {
        MongoClientSettings clientSettings = createSettings(getOidcUri(),
                (x) -> new RequestCallbackResult("invalid_token"), null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            try {
                performFind(mongoClient);
                fail();
            } catch (Exception e) {
                assertCause(MongoCommandException.class,
                        "Command failed with error 18 (AuthenticationFailed):", e);
            }
        }
    }


    @Test
    public void test4p1Reauthentication() {
        TestCallback onRequest = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            failCommand(391, 1, "find");
            // #. Perform a find operation that succeeds.
            performFind(mongoClient);
        }
        assertEquals(2, onRequest.invocations.get());
    }

    public MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcRequestCallback onRequest) {
        return createSettings(connectionString, onRequest, null);
    }

    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcRequestCallback onRequest,
            @Nullable final CommandListener commandListener) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = cs.getCredential()
                .withMechanismProperty(OIDC_CALLBACK_KEY, onRequest);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applicationName(appName)
                .applyConnectionString(cs)
                .retryReads(false)
                .credential(credential);
        if (commandListener != null) {
            builder.addCommandListener(commandListener);
        }
        return builder.build();
    }

    private void performFind(final MongoClientSettings settings) {
        try (MongoClient mongoClient = createMongoClient(settings)) {
            performFind(mongoClient);
        }
    }

    private <T extends Throwable> void performFind(
            final MongoClientSettings settings,
            final Class<T> expectedExceptionOrCause,
            final String expectedMessage) {
        try (MongoClient mongoClient = createMongoClient(settings)) {
            assertCause(expectedExceptionOrCause, expectedMessage, () -> performFind(mongoClient));
        }
    }

    private void performFind(final MongoClient mongoClient) {
        mongoClient
                .getDatabase("test")
                .getCollection("test")
                .find()
                .first();
    }

    private static <T extends Throwable> void assertCause(
            final Class<T> expectedCause, final String expectedMessageFragment, final Executable e) {
        Throwable actualException = assertThrows(Throwable.class, e);
        assertCause(expectedCause, expectedMessageFragment, actualException);
    }

    private static <T extends Throwable> void assertCause(
            final Class<T> expectedCause, final String expectedMessageFragment, final Throwable actualException) {
        Throwable cause = actualException;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (!expectedCause.isInstance(cause)) {
            throw new AssertionFailedError("Unexpected cause", actualException);
        }
        if (!cause.getMessage().contains(expectedMessageFragment)) {
            throw new AssertionFailedError("Unexpected message", actualException);
        }
    }

    protected void delayNextFind() {
        try (MongoClient client = createMongoClient(createSettings(getAwsOidcUri(), null, null))) {
            BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                    .append("mode", new BsonDocument("times", new BsonInt32(1)))
                    .append("data", new BsonDocument()
                            .append("appName", new BsonString(appName))
                            .append("failCommands", new BsonArray(asList(new BsonString("find"))))
                            .append("blockConnection", new BsonBoolean(true))
                            .append("blockTimeMS", new BsonInt32(100)));
            client.getDatabase("admin").runCommand(failPointDocument);
        }
    }

    protected void failCommand(final int code, final int times, final String... commands) {
        try (MongoClient mongoClient = createMongoClient(createSettings(
                getAwsOidcUri(), null, null))) {
            List<BsonString> list = Arrays.stream(commands).map(c -> new BsonString(c)).collect(Collectors.toList());
            BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                    .append("mode", new BsonDocument("times", new BsonInt32(times)))
                    .append("data", new BsonDocument()
                            .append("appName", new BsonString(appName))
                            .append("failCommands", new BsonArray(list))
                            .append("errorCode", new BsonInt32(code)));
            mongoClient.getDatabase("admin").runCommand(failPointDocument);
        }
    }

    public static class TestCallback implements OidcRequestCallback {
        private final AtomicInteger invocations = new AtomicInteger();
        @Nullable
        private final Integer delayInMilliseconds;
        @Nullable
        private final AtomicInteger concurrentTracker;
        @Nullable
        private final TestListener testListener;
        @Nullable
        private final Supplier<String> pathSupplier;

        public TestCallback() {
            this(null, new AtomicInteger(), null, null);
        }

        public TestCallback(
                @Nullable final Integer delayInMilliseconds,
                @Nullable final AtomicInteger concurrentTracker,
                @Nullable final TestListener testListener,
                @Nullable final Supplier<String> pathSupplier) {
            this.delayInMilliseconds = delayInMilliseconds;
            this.concurrentTracker = concurrentTracker;
            this.testListener = testListener;
            this.pathSupplier = pathSupplier;
        }

        public int getInvocations() {
            return invocations.get();
        }

        @Override
        public RequestCallbackResult onRequest(final OidcRequestContext context) {
            if (testListener != null) {
                testListener.add("onRequest invoked");
            }
            return callback();
        }

        @NotNull
        private RequestCallbackResult callback() {
            if (concurrentTracker != null) {
                if (concurrentTracker.get() > 0) {
                    throw new RuntimeException("Callbacks should not be invoked by multiple threads.");
                }
                concurrentTracker.incrementAndGet();
            }
            try {
                invocations.incrementAndGet();
                Path path = Paths.get(pathSupplier == null
                        ? getenv(AWS_WEB_IDENTITY_TOKEN_FILE)
                        : pathSupplier.get());
                String accessToken;
                try {
                    simulateDelay();
                    accessToken = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (testListener != null) {
                    testListener.add("read access token: " + path.getFileName());
                }
                return new RequestCallbackResult(accessToken);
            } finally {
                if (concurrentTracker != null) {
                    concurrentTracker.decrementAndGet();
                }
            }
        }

        private void simulateDelay() throws InterruptedException {
            if (delayInMilliseconds != null) {
                Thread.sleep(delayInMilliseconds);
            }
        }

        public TestCallback setDelayMs(final int milliseconds) {
            return new TestCallback(
                    milliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setConcurrentTracker(final AtomicInteger c) {
            return new TestCallback(
                    this.delayInMilliseconds,
                    c,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setEventListener(final TestListener testListener) {
            return new TestCallback(
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    testListener,
                    this.pathSupplier);
        }

        public TestCallback setPathSupplier(final Supplier<String> pathSupplier) {
            return new TestCallback(
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    pathSupplier);
        }
    }

    public TestCallback createCallback() {
        return new TestCallback();
    }
}
