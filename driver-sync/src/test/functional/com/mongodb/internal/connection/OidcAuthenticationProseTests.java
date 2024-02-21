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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.OIDC_HUMAN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OidcCallbackResult;
import static com.mongodb.MongoCredential.OidcCallback;
import static com.mongodb.MongoCredential.OidcCallbackContext;
import static com.mongodb.MongoCredential.OIDC_CALLBACK_KEY;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private String appName;

    protected static String getOidcUri() {
        ConnectionString cs = new ConnectionString(getenv("OIDC_ATLAS_URI_SINGLE"));
        // remove any username and password
        return "mongodb+srv://" + cs.getHosts().get(0) + "/?authMechanism=MONGODB-OIDC";
    }

    protected static String getOidcUri(final String username) {
        ConnectionString cs = new ConnectionString(getenv("OIDC_ATLAS_URI_SINGLE"));
        // set username
        return "mongodb+srv://" + username + "@" + cs.getHosts().get(0) + "/?authMechanism=MONGODB-OIDC";
    }

    protected static String getOidcUriMulti(@Nullable final String username) {
        ConnectionString cs = new ConnectionString(getenv("OIDC_ATLAS_URI_MULTI"));
        // set username
        String userPart = username == null ? "" : username + "@";
        return "mongodb+srv://" + userPart + cs.getHosts().get(0) + "/?authMechanism=MONGODB-OIDC";
    }

    private static String getAwsOidcUri() {
        return getOidcUri() + "&authMechanismProperties=PROVIDER_NAME:aws";
    }

    @NotNull
    private static String oidcTokenDirectory() {
        return getenv("OIDC_TOKEN_DIR");
    }

    private static String getAwsTokenFilePath() {
        return getenv(OidcAuthenticator.AWS_WEB_IDENTITY_TOKEN_FILE);
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
        OidcCallback onRequest2 = (context) -> {
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
        OidcCallback onRequest = (context) -> null;
        MongoClientSettings settings = this.createSettings(getOidcUri(), onRequest, null);
        performFind(settings, MongoConfigurationException.class, "Result of callback must not be null");
    }

    @Test
    public void test2p3CallbackReturnsMissingData() {
        // #. Create a client with a request callback that returns data not
        //    conforming to the OIDCRequestTokenResult with missing field(s).
        OidcCallback onRequest = (context) -> {
            //noinspection ConstantConditions
            return new OidcCallbackResult(null);
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
        OidcCallback onRequest = (context) -> {
            OidcCallbackResult result = onRequestWrapped.onRequest(context);
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
                (x) -> new OidcCallbackResult("invalid_token"), null);
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

    // Tests for human authentication ("testh", to preserve ordering)

    @Test
    public void testh1p1SinglePrincipalImplicitUsername() {
        // #. Create default OIDC client with authMechanism=MONGODB-OIDC.
        String oidcUri = getOidcUri();
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(oidcUri, callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
        assertEquals(1, callback.invocations.get());
    }

    @Test
    public void testh1p2SinglePrincipalExplicitUsername() {
        // #. Create a client with MONGODB_URI_SINGLE, a username of test_user1,
        //    authMechanism=MONGODB-OIDC, and the OIDC human callback.
        String oidcUri = getOidcUri("test_user1");
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(oidcUri, callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void testh1p3MultiplePrincipalUser1() {
        // #. Create a client with MONGODB_URI_MULTI, a username of test_user1,
        //    authMechanism=MONGODB-OIDC, and the OIDC human callback.
        String oidcUri = getOidcUriMulti("test_user1");
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(oidcUri, callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void testh1p4MultiplePrincipalUser2() {
        //- Create a human callback that reads in the generated ``test_user2`` token file.
        //- Create a client with ``MONGODB_URI_MULTI``, a username of ``test_user2``,
        //  ``authMechanism=MONGODB-OIDC``, and the OIDC human callback.
        String oidcUri = getOidcUriMulti("test_user2");
        TestCallback callback = createHumanCallback()
                .setPathSupplier(() -> tokenQueue("test_user2").remove());
        MongoClientSettings clientSettings = createHumanSettings(oidcUri, callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void testh1p5MultiplePrincipalNoUser() {
        //- Create a client with ``MONGODB_URI_MULTI``, no username,
        //  ``authMechanism=MONGODB-OIDC``, and the OIDC human callback.
        String oidcUri = getOidcUriMulti(null);
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(oidcUri, callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings, MongoCommandException.class, "Authentication failed");
    }

    @Test
    public void testh1p6AllowedHostsBlocked() {
        //- Create a default OIDC client, with an ``ALLOWED_HOSTS`` that is an empty list.
        //- Assert that a ``find`` operation fails with a client-side error.
        MongoClientSettings settings1 = createSettings(
                getOidcUri(),
                createHumanCallback(), null, OIDC_HUMAN_CALLBACK_KEY, Collections.emptyList());
        performFind(settings1, MongoSecurityException.class, "Host not permitted by ALLOWED_HOSTS");

        //- Create a client that uses the URL
        //  ``mongodb://localhost/?authMechanism=MONGODB-OIDC&ignored=example.com``, a
        //  human callback, and an ``ALLOWED_HOSTS`` that contains ``["example.com"]``.
        //- Assert that a ``find`` operation fails with a client-side error.
        MongoClientSettings settings2 = createSettings(
                getOidcUri() + "&ignored=example.com",
                createHumanCallback(), null, OIDC_HUMAN_CALLBACK_KEY, Arrays.asList("example.com"));
        performFind(settings2, MongoSecurityException.class, "Host not permitted by ALLOWED_HOSTS");
    }

    @Test
    public void testh2p1ValidCallbackInputs() {
        TestCallback onRequest = createHumanCallback();
        OidcCallback onRequest2 = (context) -> {
            assertTrue(context.getIdpInfo().getClientId().startsWith("0oad"));
            assertTrue(context.getIdpInfo().getIssuer().endsWith("mock-identity-config-oidc"));
            assertEquals(Arrays.asList("fizz", "buzz"), context.getIdpInfo().getRequestScopes());
            assertEquals(Duration.ofMinutes(5), context.getTimeout());
            return onRequest.onRequest(context);
        };
        MongoClientSettings clientSettings = createHumanSettings(getOidcUri(), onRequest2, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            // Ensure that callback was called
            assertEquals(1, onRequest.getInvocations());
        }
    }

    @Test
    public void testh2p2HumanCallbackReturnsMissingData() {
        //noinspection ConstantConditions
        OidcCallback onRequestNull = (context) -> null;
        performFind(createHumanSettings(getOidcUri(), onRequestNull, null),
                MongoConfigurationException.class,
                "Result of callback must not be null");

        //noinspection ConstantConditions
        OidcCallback onRequest = (context) -> new OidcCallbackResult(null);
        performFind(createHumanSettings(getOidcUri(), onRequest, null),
                IllegalArgumentException.class,
                "accessToken can not be null");

        // additionally, check validation for refresh in machine workflow:
        OidcCallback onRequestMachineRefresh = (context) -> new OidcCallbackResult("access", "exists");
        performFind(createSettings(getOidcUri(), onRequestMachineRefresh, null),
                MongoConfigurationException.class,
                "Refresh token must only be provided in human workflow");
    }

    @Test
    public void testh3p1UsesSpecAuthIfCachedToken() {
        failCommandAndCloseConnection("find", 1);
        MongoClientSettings settings = createHumanSettings(getOidcUri(), createHumanCallback(), null);

        try (MongoClient mongoClient = createMongoClient(settings)) {
            assertCause(MongoSocketException.class,
                    "Prematurely reached end of stream",
                    () -> performFind(mongoClient));
            failCommand(20, 99, "saslStart");

            performFind(mongoClient);
        }
    }

    @Test
    public void testh3p2NoSpecAuthIfNoCachedToken() {
        failCommand(20, 99, "saslStart");
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);
        performFind(createHumanSettings(getOidcUri(), createHumanCallback(), commandListener),
                MongoCommandException.class,
                "Command failed with error 20");
        assertEquals(Arrays.asList(
                "isMaster started",
                "isMaster succeeded",
                "saslStart started",
                "saslStart failed"
        ), listener.getEventStrings());
        listener.clear();
    }

    @Test
    public void testh4p1Succeeds() {
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);
        TestCallback callback = createHumanCallback()
                .setEventListener(listener);
        MongoClientSettings settings = createHumanSettings(getOidcUri(), callback, commandListener);
        try (MongoClient mongoClient = createMongoClient(settings)) {
            performFind(mongoClient);
            listener.clear();
            assertEquals(1, callback.getInvocations());

            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    // first find fails:
                    "find started",
                    "find failed",
                    "onRequest invoked (Refresh Token: present - IdpInfo: present)",
                    "read access token: test_user1",
                    "saslStart started",
                    "saslStart succeeded",
                    // second find succeeds:
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
            assertEquals(2, callback.getInvocations());
        }
    }

    @Test
    public void testh4p2SucceedsNoRefresh() {
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);
        TestCallback callback = createHumanCallback().setEventListener(listener);
        MongoClientSettings settings = createHumanSettings(getOidcUri(), callback, commandListener);
        try (MongoClient mongoClient = createMongoClient(settings)) {

            performFind(mongoClient);
            listener.clear();
            assertEquals(1, callback.getInvocations());

            failCommand(391, 1, "find");
            performFind(mongoClient);
        }
    }


    // TODO-OIDC awaiting spec updates, add 4.3 and 4.4

    // Not a prose test
    @Test
    public void testErrorClearsCache() {
        // #. Create a new client with a valid request callback that
        //    gives credentials that expire within 5 minutes and
        //    a refresh callback that gives invalid credentials.
        TestListener listener = new TestListener();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_expires",
                "test_user1_1");
        TestCallback onRequest = createHumanCallback()
                .setRefreshToken("refresh")
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);

        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createHumanSettings(getOidcUri(), onRequest, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Ensure that a find operation adds a new entry to the cache.
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "isMaster started",
                    "isMaster succeeded",
                    // no speculative auth. Send principal request:
                    "saslStart started",
                    "saslStart succeeded",
                    "onRequest invoked (Refresh Token: none - IdpInfo: present)",
                    "read access token: test_user1",
                    // the refresh token from the callback is cached here
                    // send jwt:
                    "saslContinue started",
                    "saslContinue succeeded",
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
                    "find failed", // reauth 391; current access token is invalid
                    // fall back to refresh token, from prior find
                    "onRequest invoked (Refresh Token: present - IdpInfo: present)",
                    "read access token: test_user1_expires",
                    "saslStart started",
                    "saslStart failed", // it is expired, fails immediately
                    // fall back to principal request, and non-refresh callback:
                    "saslStart started",
                    "saslStart succeeded",
                    "onRequest invoked (Refresh Token: none - IdpInfo: present)",
                    "read access token: test_user1_expires",
                    "saslContinue started",
                    "saslContinue failed" // also fails due to 391
            ), listener.getEventStrings());
            listener.clear();

            // #. Ensure that the cache value cleared.
            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "find started",
                    "find failed",
                    // falling back to principal request, onRequest callback.
                    // this implies that the cache has been cleared during the
                    // preceding find operation.
                    "saslStart started",
                    "saslStart succeeded",
                    "onRequest invoked (Refresh Token: none - IdpInfo: present)",
                    "read access token: test_user1_1",
                    "saslContinue started",
                    "saslContinue succeeded",
                    // auth has finished
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
            listener.clear();
        }
    }

    public MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcCallback onRequest) {
        return createSettings(connectionString, onRequest, null);
    }

    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcCallback callback,
            @Nullable final CommandListener commandListener) {
        return createSettings(connectionString, callback, commandListener, OIDC_CALLBACK_KEY);
    }

    private MongoClientSettings createHumanSettings(
            final String connectionString,
            @Nullable final OidcCallback callback,
            @Nullable final CommandListener commandListener) {
        return createSettings(connectionString, callback, commandListener, OIDC_HUMAN_CALLBACK_KEY);
    }

    @NotNull
    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcCallback onRequest,
            @Nullable final CommandListener commandListener,
            final String oidcCallbackKey) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = cs.getCredential()
                .withMechanismProperty(oidcCallbackKey, onRequest);
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

    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcCallback onRequest,
            @Nullable final CommandListener commandListener,
            final String oidcCallbackKey,
            @Nullable final List<String> allowedHosts) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = cs.getCredential()
                .withMechanismProperty(oidcCallbackKey, onRequest)
                .withMechanismProperty(ALLOWED_HOSTS_KEY, allowedHosts);
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
        try (MongoClient client = createMongoClient(createSettings(
                getAwsOidcUri(), null, null))) {
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

    private void failCommandAndCloseConnection(final String command, final int times) {
        try (MongoClient mongoClient = createMongoClient(createSettings(
                getAwsOidcUri(), null, null))) {
            BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                    .append("mode", new BsonDocument("times", new BsonInt32(times)))
                    .append("data", new BsonDocument()
                            .append("appName", new BsonString(appName))
                            .append("closeConnection", new BsonBoolean(true))
                            .append("failCommands", new BsonArray(Arrays.asList(new BsonString(command))))
                    );
            mongoClient.getDatabase("admin").runCommand(failPointDocument);
        }
    }

    public static class TestCallback implements OidcCallback {
        private final AtomicInteger invocations = new AtomicInteger();
        @Nullable
        private final Integer delayInMilliseconds;
        @Nullable
        private final String refreshToken;
        @Nullable
        private final AtomicInteger concurrentTracker;
        @Nullable
        private final TestListener testListener;
        @Nullable
        private final Supplier<String> pathSupplier;

        public TestCallback() {
            this(null, null, new AtomicInteger(), null, null);
        }

        public TestCallback(
                @Nullable final String refreshToken,
                @Nullable final Integer delayInMilliseconds,
                @Nullable final AtomicInteger concurrentTracker,
                @Nullable final TestListener testListener,
                @Nullable final Supplier<String> pathSupplier) {
            this.refreshToken = refreshToken;
            this.delayInMilliseconds = delayInMilliseconds;
            this.concurrentTracker = concurrentTracker;
            this.testListener = testListener;
            this.pathSupplier = pathSupplier;
        }

        public int getInvocations() {
            return invocations.get();
        }

        @Override
        public OidcCallbackResult onRequest(final OidcCallbackContext context) {
            if (testListener != null) {
                testListener.add("onRequest invoked ("
                        + "Refresh Token: " + (context.getRefreshToken() == null ? "none" : "present")
                        + " - IdpInfo: " + (context.getIdpInfo() == null ? "none" : "present")
                        + ")");
            }
            return callback();
        }

        @NotNull
        private OidcCallbackResult callback() {
            if (concurrentTracker != null) {
                if (concurrentTracker.get() > 0) {
                    throw new RuntimeException("Callbacks should not be invoked by multiple threads.");
                }
                concurrentTracker.incrementAndGet();
            }
            try {
                invocations.incrementAndGet();
                Path path = Paths.get(pathSupplier == null
                        ? getAwsTokenFilePath()
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
                return new OidcCallbackResult(accessToken, refreshToken);
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
                    this.refreshToken,
                    milliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setConcurrentTracker(final AtomicInteger c) {
            return new TestCallback(
                    this.refreshToken,
                    this.delayInMilliseconds,
                    c,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setEventListener(final TestListener testListener) {
            return new TestCallback(
                    this.refreshToken,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    testListener,
                    this.pathSupplier);
        }

        public TestCallback setPathSupplier(final Supplier<String> pathSupplier) {
            return new TestCallback(
                    this.refreshToken,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    pathSupplier);
        }
        public TestCallback setRefreshToken(final String token) {
            return new TestCallback(
                    token,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    this.pathSupplier);
        }
    }

    @NotNull
    private ConcurrentLinkedQueue<String> tokenQueue(final String... queue) {
        String tokenPath = oidcTokenDirectory();
        return java.util.stream.Stream
                .of(queue)
                .map(v -> tokenPath + v)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    }

    public TestCallback createCallback() {
        return new TestCallback();
    }

    public TestCallback createHumanCallback() {
        return new TestCallback()
                .setPathSupplier(() -> oidcTokenDirectory() + "test_user1")
                .setRefreshToken("refreshToken");
    }
}
