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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.RequestCallbackResult;
import com.mongodb.MongoSecurityException;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.PROVIDER_NAME_KEY;
import static com.mongodb.MongoCredential.REQUEST_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.createOidcCredential;
import static com.mongodb.client.TestHelper.setEnvironmentVariable;
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

    public static final String TOKEN_DIRECTORY = "/tmp/tokens/";

    private String appName;

    protected static String getOidcUri() {
        ConnectionString cs = new ConnectionString(getenv("MONGODB_URI"));
        // remove username and password
        return "mongodb+srv://" + cs.getHosts().get(0) + "/?authMechanism=MONGODB-OIDC";
    }

    private static String getAwsOidcUri() {
        return getOidcUri() + "&authMechanismProperties=PROVIDER_NAME:aws";
    }

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    protected void setOidcFile(final String file) {
        setEnvironmentVariable(AWS_WEB_IDENTITY_TOKEN_FILE, TOKEN_DIRECTORY + file);
    }

    @BeforeEach
    public void beforeEach() {
        assumeTrue(oidcTestsEnabled());
        // In each test, clearing the cache is not required, since there is no global cache
        setOidcFile("test_user1");
        InternalStreamConnection.setRecordEverything(true);
        this.appName = this.getClass().getSimpleName() + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }

    @AfterEach
    public void afterEach() {
        InternalStreamConnection.setRecordEverything(false);
    }

    @Test
    public void test1p1CallbackDrivenAuth() {
        // #. Create a request callback that returns a valid token.
        OidcRequestCallback onRequest = createCallback();
        // #. Create a client with a URL of the form ... and the OIDC request callback.
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void test1p7LockAvoidsExtraCallbackCalls() {
        proveThatConcurrentCallbacksThrow();
        // The test requires that two operations are attempted concurrently.
        // The delay on the next find should cause the initial request to delay
        // and the ensuing refresh to block, rather than entering onRefresh.
        // After blocking, this ensuing refresh thread will enter onRefresh.
        AtomicInteger concurrent = new AtomicInteger();
        TestCallback onRequest = createCallback().setConcurrentTracker(concurrent);
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            assertEquals(1, onRequest.getInvocations());
        }
    }

    public void proveThatConcurrentCallbacksThrow() {
        // ensure that, via delay, test callbacks throw when invoked concurrently
        AtomicInteger c = new AtomicInteger();
        TestCallback request = createCallback().setConcurrentTracker(c).setDelayMs(5);
        TestCallback refresh = createCallback().setConcurrentTracker(c);
        executeAll(() -> {
            sleep(2);
            assertThrows(RuntimeException.class, () -> {
                refresh.onRequest(new OidcAuthenticator.OidcRequestContextImpl(Duration.ofSeconds(1234)));
            });
        }, () -> {
            request.onRequest(new OidcAuthenticator.OidcRequestContextImpl(Duration.ofSeconds(1234)));
        });
    }

    private void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test2AwsAutomaticAuth() {
        String uri = getAwsOidcUri();

        // #. Create a client with a url of the form ...
        MongoCredential credential = createOidcCredential(null)
                .withMechanismProperty(PROVIDER_NAME_KEY, "aws");
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applicationName(appName)
                .credential(credential)
                .applyConnectionString(new ConnectionString(uri))
                .build();
        // #. Perform a find operation that succeeds.
        performFind(clientSettings);
    }

    @Test
    public void test2p4AllowedHostsIgnored() {
        MongoClientSettings settings = createSettings(
                getAwsOidcUri(), null, null);
        performFind(settings);
    }

    @Test
    public void test3p1ValidCallbacks() {
        String connectionString = getOidcUri();
        Duration expectedSeconds = Duration.ofMinutes(5);

        TestCallback onRequest = createCallback();
        // #. Verify that the request callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        // #. Verify that the refresh callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        OidcRequestCallback onRequest2 = (context) -> {
            assertEquals(expectedSeconds, context.getTimeout());
            return onRequest.onRequest(context);
        };
        MongoClientSettings clientSettings = createSettings(connectionString, onRequest2);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            // Ensure that both callbacks were called
            assertEquals(1, onRequest.getInvocations());
        }
    }

    @Test
    public void test3p2RequestCallbackReturnsNull() {
        //noinspection ConstantConditions
        OidcRequestCallback onRequest = (context) -> null;
        MongoClientSettings settings = this.createSettings(getOidcUri(), onRequest, null);
        performFind(settings, MongoConfigurationException.class, "Result of callback must not be null");
    }

    @Test
    public void test3p4RequestCallbackReturnsInvalidData() {
        // #. Create a client with a request callback that returns data not
        //    conforming to the OIDCRequestTokenResult with missing field(s).
        // #. ... with extra field(s). - not possible
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

    // 3.6   Refresh Callback Returns Extra Data - not possible due to use of class

    @Test
    public void test4p1CachedCredentialsCacheWithRefresh() {
        // #. Create a new client with a request callback that gives credentials that expire in one minute.
        TestCallback onRequest = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Create a new client with the same request callback and a refresh callback.
            // Instead:
            // 1. Delay the first find, causing the second find to authenticate a second connection
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            // #. Ensure that a find operation adds credentials to the cache.
            // #. Ensure that a find operation results in a call to the refresh callback.
            assertEquals(1, onRequest.getInvocations());
            // the refresh invocation will fail if the cached tokens are null
            // so a success implies that credentials were present in the cache
        }
    }

    @Test
    public void test4p4ErrorClearsCache() {
        // #. Create a new client with a valid request callback that
        //    gives credentials that expire within 5 minutes and
        //    a refresh callback that gives invalid credentials.

        TestListener listener = new TestListener();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_1");
        TestCallback onRequest = createCallback()
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);

        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Ensure that a find operation adds a new entry to the cache.
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "isMaster started",
                    "isMaster succeeded",
                    "onRequest invoked",
                    "read access token: test_user1",
                    "saslStart started",
                    "saslStart succeeded",
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
            listener.clear();

            // #. Ensure that a subsequent find operation results in a 391 error.
            failCommand(391, 1, "find");
            // ensure that the operation entirely fails, after attempting both potential fallback callbacks
            assertThrows(MongoSecurityException.class, () -> performFind(mongoClient));
            assertEquals(Arrays.asList(
                    "find started",
                    "find failed",
                    "onRequest invoked",
                    "read access token: test_user1_expires",
                    "saslStart started",
                    "saslStart failed"
            ), listener.getEventStrings());
            listener.clear();

            // #. Ensure that the cache value cleared.
            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "find started",
                    "find failed",
                    "onRequest invoked",
                    "read access token: test_user1_1",
                    "saslStart started",
                    "saslStart succeeded",
                    // auth has finished
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
            listener.clear();
        }
    }

    @Test
    public void test4p5AwsAutomaticWorkflowDoesNotUseCache() {
        // #. Create a new client that uses the AWS automatic workflow.
        // #. Ensure that a find operation does not add credentials to the cache.
        setOidcFile("test_user1");
        MongoCredential credential = createOidcCredential(null)
                .withMechanismProperty(PROVIDER_NAME_KEY, "aws");
        ConnectionString connectionString = new ConnectionString(getAwsOidcUri());
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applicationName(appName)
                .credential(credential)
                .applyConnectionString(connectionString)
                .build();
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            // This ensures that the next find failure results in a file (rather than cache) read
            failCommand(391, 1, "find");
            setOidcFile("invalid_file");
            assertCause(NoSuchFileException.class, "invalid_file", () -> performFind(mongoClient));
        }
    }

    // Not a prose test
    @Test
    public void testAutomaticAuthUsesSpeculative() {
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings settings = createSettings(
                getAwsOidcUri(), null, commandListener);
        try (MongoClient mongoClient = createMongoClient(settings)) {
            // we use a listener instead of a failpoint
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "isMaster started",
                    "isMaster succeeded",
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
        }
    }

    @Test
    public void test6p1ReauthenticationSucceeds() {
        // #. Create request callback that returns valid credentials that will not expire soon.
        TestListener listener = new TestListener();
        TestCallback onRequest = createCallback().setEventListener(listener);

        // #. Create a client with the callbacks and an event listener capable of listening for SASL commands.
        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {

            // #. Perform a find operation that succeeds.
            performFind(mongoClient);

            assertEquals(Arrays.asList(
                    // speculative:
                    "isMaster started",
                    "isMaster succeeded",
                    // onRequest:
                    "onRequest invoked",
                    "read access token: test_user1",
                    // jwt from onRequest:
                    "saslStart started",
                    "saslStart succeeded",
                    // ensuing find:
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());

            // #. Clear the listener state if possible.
            commandListener.reset();
            listener.clear();

            // #. Force a reauthenication using a failCommand
            failCommand(391, 1, "find");

            // #. Perform another find operation that succeeds.
            performFind(mongoClient);

            // #. Assert that the ordering of command started events is: find, find.
            // #. Assert that the ordering of command succeeded events is: find.
            // #. Assert that a find operation failed once during the command execution.
            assertEquals(Arrays.asList(
                    "find started",
                    "find failed",
                    // find has triggered 391, and cleared the access token; fall back to callback:
                    "onRequest invoked",
                    "read access token: test_user1",
                    "saslStart started",
                    "saslStart succeeded",
                    // find retry succeeds:
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
        }
    }

    @NotNull
    private ConcurrentLinkedQueue<String> tokenQueue(final String... queue) {
        return Stream
                .of(queue)
                .map(v -> TOKEN_DIRECTORY + v)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    }

    @Test
    public void test6p2ReauthenticationRetriesAndSucceedsWithCache() {
        // #. Create request and refresh callbacks that return valid credentials that will not expire soon.
        TestCallback onRequest = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Perform a find operation that succeeds.
            performFind(mongoClient);
            // #. Force a reauthenication using a failCommand
            failCommand(391, 1, "find");
            // #. Perform a find operation that succeeds.
            performFind(mongoClient);
        }
    }

    // 6.3   Retries and Fails with no Cache
    // Appears to be untestable, since it requires 391 failure on jwt (may be fixed in future spec)

    @Test
    public void test6p4SeparateConnectionsAvoidExtraCallbackCalls() {
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_1");
        TestCallback onRequest = createCallback().setPathSupplier(() -> tokens.remove());
        MongoClientSettings clientSettings = createSettings(getOidcUri(), onRequest);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Peform a find operation on each ... that succeeds.
            delayNextFind();
            executeAll(2, () -> performFind(mongoClient));
            // #. Ensure that the request callback has been called once and the refresh callback has not been called.
            assertEquals(1, onRequest.getInvocations());

            failCommand(391, 2, "find");
            executeAll(2, () -> performFind(mongoClient));

            // #. Ensure that the callback
            // has been called twice:
            assertEquals(2, onRequest.getInvocations());
        }
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
                .withMechanismProperty(REQUEST_TOKEN_CALLBACK_KEY, onRequest);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applicationName(appName)
                .applyConnectionString(cs)
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
