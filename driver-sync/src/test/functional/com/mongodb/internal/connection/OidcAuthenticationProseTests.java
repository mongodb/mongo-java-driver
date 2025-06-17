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
import com.mongodb.assertions.Assertions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TestListener;
import com.mongodb.event.CommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.ENVIRONMENT_KEY;
import static com.mongodb.MongoCredential.OIDC_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OIDC_HUMAN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OidcCallback;
import static com.mongodb.MongoCredential.OidcCallbackContext;
import static com.mongodb.MongoCredential.OidcCallbackResult;
import static com.mongodb.MongoCredential.TOKEN_RESOURCE_KEY;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.testing.MongoAssertions.assertCause;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.ThreadTestHelpers.executeAll;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/auth/tests/mongodb-oidc.md#mongodb-oidc">Prose Tests</a>.
 */
public class OidcAuthenticationProseTests {

    private String appName;

    public static boolean oidcTestsEnabled() {
        return Boolean.parseBoolean(getenv().get("OIDC_TESTS_ENABLED"));
    }

    private void assumeTestEnvironment() {
        assumeTrue(getenv("OIDC_TOKEN_DIR") != null);
    }

    protected static String getOidcUri() {
        return assertNotNull(getenv("MONGODB_URI_SINGLE"));
    }

    private static String getOidcUriMulti() {
        return assertNotNull(getenv("MONGODB_URI_MULTI"));
    }

    private static String getOidcEnv() {
        return assertNotNull(getenv("OIDC_ENV"));
    }

    private static void assumeAzure() {
        assumeTrue(getOidcEnv().equals("azure"));
    }

    @Nullable
    private static String getUserWithDomain(@Nullable final String user) {
        return user == null ? null : user + "@" + getenv("OIDC_DOMAIN");
    }

    private static String oidcTokenDirectory() {
        String dir = getenv("OIDC_TOKEN_DIR");
        if (!dir.endsWith("/")) {
            dir = dir + "/";
        }
        return dir;
    }

    private static String getTestTokenFilePath() {
        return getenv(OidcAuthenticator.OIDC_TOKEN_FILE);
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
        TestCallback callback = createCallback();
        MongoClientSettings clientSettings = createSettings(callback);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
        assertEquals(1, callback.invocations.get());
    }

    @Test
    public void test1p2CallbackCalledOnceForMultipleConnections() {
        TestCallback callback = createCallback();
        MongoClientSettings clientSettings = createSettings(callback);
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
        assertEquals(1, callback.invocations.get());
    }

    @Test
    public void test2p1ValidCallbackInputs() {
        Duration expectedTimeoutDuration = Duration.ofMinutes(1);

        TestCallback callback1 = createCallback();
        // #. Verify that the request callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        OidcCallback callback2 = (context) -> {
            assertEquals(expectedTimeoutDuration, context.getTimeout());
            return callback1.onRequest(context);
        };
        MongoClientSettings clientSettings = createSettings(callback2);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            // callback was called
            assertEquals(1, callback1.getInvocations());
        }
    }

    // Not a prose test
    @ParameterizedTest
    @MethodSource
    @DisplayName("{testName}")
    void testValidCallbackInputsTimeout(final String testName,
                                        final int timeoutMs,
                                        final int serverSelectionTimeoutMS,
                                        final int expectedTimeoutThreshold) {
        TestCallback callback1 = createCallback();

        OidcCallback callback2 = (context) -> {
            assertTrue(context.getTimeout().toMillis() < expectedTimeoutThreshold,
                    format("Expected timeout to be less than %d, but was %d",
                            expectedTimeoutThreshold,
                            context.getTimeout().toMillis()));
            return callback1.onRequest(context);
        };

        MongoClientSettings clientSettings = MongoClientSettings.builder(createSettings(callback2))
                .applyToClusterSettings(builder ->
                        builder.serverSelectionTimeout(
                                serverSelectionTimeoutMS,
                                TimeUnit.MILLISECONDS))
                .timeout(timeoutMs, TimeUnit.MILLISECONDS)
                .build();

        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            long start = System.nanoTime();
            performFind(mongoClient);
            assertEquals(1, callback1.getInvocations());
            long elapsed = msElapsedSince(start);

            assertFalse(elapsed > (timeoutMs == 0 ? serverSelectionTimeoutMS : min(serverSelectionTimeoutMS, timeoutMs)),
                    format("Elapsed time %d is greater then minimum of serverSelectionTimeoutMS and timeoutMs, which is %d. "
                                    + "This indicates that the callback was not called with the expected timeout.",
                            min(serverSelectionTimeoutMS, timeoutMs),
                            elapsed));
        }
    }

    private static Stream<Arguments> testValidCallbackInputsTimeout() {
        return Stream.of(
                Arguments.of("serverSelectionTimeoutMS honored for oidc callback if it's lower than timeoutMS",
                        1000, // timeoutMS
                        500,  // serverSelectionTimeoutMS
                        499), // expectedTimeoutThreshold
                Arguments.of("timeoutMS honored for oidc callback if it's lower than serverSelectionTimeoutMS",
                        500,  // timeoutMS
                        1000, // serverSelectionTimeoutMS
                        499), // expectedTimeoutThreshold
                Arguments.of("serverSelectionTimeoutMS honored for oidc callback if timeoutMS=0",
                        0,   // infinite timeoutMS
                        500, // serverSelectionTimeoutMS
                        499) // expectedTimeoutThreshold
        );
    }

    @Test
    public void test2p2RequestCallbackReturnsNull() {
        //noinspection ConstantConditions
        OidcCallback callback = (context) -> null;
        MongoClientSettings clientSettings = this.createSettings(callback);
        assertFindFails(clientSettings, MongoConfigurationException.class,
                "Result of callback must not be null");
    }

    @Test
    public void test2p3CallbackReturnsMissingData() {
        // #. Create a client with a request callback that returns data not
        //    conforming to the OIDCRequestTokenResult with missing field(s).
        OidcCallback callback = (context) -> {
            //noinspection ConstantConditions
            return new OidcCallbackResult(null);
        };
        // we ensure that the error is propagated
        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            assertCause(IllegalArgumentException.class,
                    "accessToken can not be null",
                    () -> performFind(mongoClient));
        }
    }

    @Test
    public void test2p4InvalidClientConfigurationWithCallback() {
        String uri = getOidcUri() + "&authMechanismProperties=ENVIRONMENT:" + getOidcEnv();
        MongoClientSettings settings = createSettings(
                uri, createCallback(), null, OIDC_CALLBACK_KEY);
        assertCause(IllegalArgumentException.class,
                "OIDC_CALLBACK must not be specified when ENVIRONMENT is specified",
                () -> performFind(settings));
    }

    @Test
    public void test2p5InvalidAllowedHosts() {
        assumeTestEnvironment();

        String uri = "mongodb://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:azure,TOKEN_RESOURCE:123";
        ConnectionString cs = new ConnectionString(uri);
        MongoCredential credential = assertNotNull(cs.getCredential())
                .withMechanismProperty("ALLOWED_HOSTS", Collections.emptyList());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applicationName(appName)
                .applyConnectionString(cs)
                .retryReads(false)
                .credential(credential)
                .build();
        assertCause(IllegalArgumentException.class,
                "ALLOWED_HOSTS must be specified only when OIDC_HUMAN_CALLBACK is specified",
                () -> {
                    try (MongoClient mongoClient = createMongoClient(settings)) {
                        performFind(mongoClient);
                    }
                });
    }

    @Test
    public void test3p1AuthFailsWithCachedToken() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        TestCallback callbackWrapped = createCallback();
        // reference to the token to poison
        CompletableFuture<String> poisonToken = new CompletableFuture<>();
        OidcCallback callback = (context) -> {
            OidcCallbackResult result = callbackWrapped.onRequest(context);
            String accessToken = result.getAccessToken();
            if (!poisonToken.isDone()) {
                poisonToken.complete(accessToken);
            }
            return result;
        };

        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // populate cache
            performFind(mongoClient);
            assertEquals(1, callbackWrapped.invocations.get());
            // Poison the *Client Cache* with an invalid access token.
            // uses reflection
            String poisonString = poisonToken.get();
            Field f = String.class.getDeclaredField("value");
            f.setAccessible(true);
            byte[] poisonChars = (byte[]) f.get(poisonString);
            poisonChars[0] = '~';
            poisonChars[1] = '~';

            assertEquals(1, callbackWrapped.invocations.get());

            // cause another connection to be opened
            delayNextFind();
            executeAll(2, () -> performFind(mongoClient));
        }
        assertEquals(2, callbackWrapped.invocations.get());
    }

    @Test
    public void test3p2AuthFailsWithoutCachedToken() {
        OidcCallback callback =
                (x) -> new OidcCallbackResult("invalid_token");
        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            assertCause(MongoCommandException.class,
                    "Command failed with error 18 (AuthenticationFailed):",
                    () -> performFind(mongoClient));
        }
    }

    @Test
    public void test3p3UnexpectedErrorDoesNotClearCache() {
        assumeTestEnvironment();

        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);

        TestCallback callback = createCallback();
        MongoClientSettings clientSettings = createSettings(getOidcUri(), callback, commandListener);

        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            failCommand(20, 1, "saslStart");
            assertCause(MongoCommandException.class,
                    "Command failed with error 20",
                    () -> performFind(mongoClient));

            assertEquals(Arrays.asList(
                    "isMaster started",
                    "isMaster succeeded",
                    "saslStart started",
                    "saslStart failed"
            ), listener.getEventStrings());

            assertEquals(1, callback.getInvocations());
            performFind(mongoClient);
            assertEquals(1, callback.getInvocations());
        }
    }

    @Test
    public void test4p1Reauthentication() {
        testReauthentication(false);
    }

    private void testReauthentication(final boolean inSession) {
        TestCallback callback = createCallback();
        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings);
             ClientSession session = inSession ? mongoClient.startSession() : null) {
            failCommand(391, 1, "find");
            // #. Perform a find operation that succeeds.
            performFind(mongoClient, session);
        }
        assertEquals(2, callback.invocations.get());
    }

    @Test
    public void test4p2ReadCommandsFailIfReauthenticationFails() {
        // Create a `MongoClient` whose OIDC callback returns one good token
        // and then bad tokens after the first call.
        TestCallback wrappedCallback = createCallback();
        OidcCallback callback = (context) -> {
            OidcCallbackResult result1 = wrappedCallback.callback(context);
            return new OidcCallbackResult(wrappedCallback.getInvocations() > 1 ? "bad" : result1.getAccessToken());
        };
        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            failCommand(391, 1, "find");
            assertCause(MongoCommandException.class,
                    "Command failed with error 18",
                    () -> performFind(mongoClient));
        }
        assertEquals(2, wrappedCallback.invocations.get());
    }

    @Test
    public void test4p3WriteCommandsFailIfReauthenticationFails() {
        // Create a `MongoClient` whose OIDC callback returns one good token
        // and then bad tokens after the first call.
        TestCallback wrappedCallback = createCallback();
        OidcCallback callback = (context) -> {
            OidcCallbackResult result1 = wrappedCallback.callback(context);
            return new OidcCallbackResult(
                    wrappedCallback.getInvocations() > 1 ? "bad" : result1.getAccessToken());
        };
        MongoClientSettings clientSettings = createSettings(callback);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performInsert(mongoClient);
            failCommand(391, 1, "insert");
            assertCause(MongoCommandException.class,
                    "Command failed with error 18",
                    () -> performInsert(mongoClient));
        }
        assertEquals(2, wrappedCallback.invocations.get());
    }

    private static void performInsert(final MongoClient mongoClient) {
        mongoClient
                .getDatabase("test")
                .getCollection("test")
                .insertOne(Document.parse("{ x: 1 }"));
    }

    @Test
    public void test4p5ReauthenticationInSession() {
        testReauthentication(true);
    }

    @Test
    public void test5p1AzureSucceedsWithNoUsername() {
        assumeAzure();
        String oidcUri = getOidcUri();
        MongoClientSettings clientSettings = createSettings(oidcUri, createCallback(), null);
        // Create an OIDC configured client with `ENVIRONMENT:azure` and a valid
        // `TOKEN_RESOURCE` and no username.
        MongoCredential credential = Assertions.assertNotNull(clientSettings.getCredential());
        assertNotNull(credential.getMechanismProperty(TOKEN_RESOURCE_KEY, null));
        assertNull(credential.getUserName());
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // Perform a `find` operation that succeeds.
            performFind(mongoClient);
        }
    }

    @Test
    public void test5p2AzureFailsWithBadUsername() {
        assumeAzure();
        String oidcUri = getOidcUri();
        ConnectionString cs = new ConnectionString(oidcUri);
        MongoCredential oldCredential = Assertions.assertNotNull(cs.getCredential());
        String tokenResource = oldCredential.getMechanismProperty(TOKEN_RESOURCE_KEY, null);
        assertNotNull(tokenResource);
        MongoCredential cred = MongoCredential.createOidcCredential("bad")
                .withMechanismProperty(ENVIRONMENT_KEY, "azure")
                .withMechanismProperty(TOKEN_RESOURCE_KEY, tokenResource);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applicationName(appName)
                .retryReads(false)
                .applyConnectionString(cs)
                .credential(cred);
        MongoClientSettings clientSettings = builder.build();
        // the failure is external to the driver
        assertFindFails(clientSettings, IOException.class, "400 Bad Request");
    }

    // Tests for human authentication ("testh", to preserve ordering)

    @Test
    public void testh1p1SinglePrincipalImplicitUsername() {
        assumeTestEnvironment();
        // #. Create default OIDC client with authMechanism=MONGODB-OIDC.
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(callback, null);
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
        assertEquals(1, callback.invocations.get());
    }

    @Test
    public void testh1p2SinglePrincipalExplicitUsername() {
        assumeTestEnvironment();
        // #. Create a client with MONGODB_URI_SINGLE, a username of test_user1,
        //    authMechanism=MONGODB-OIDC, and the OIDC human callback.
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createSettingsHuman(getUserWithDomain("test_user1"), callback, getOidcUri());
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void testh1p3MultiplePrincipalUser1() {
        assumeTestEnvironment();
        // #. Create a client with MONGODB_URI_MULTI, a username of test_user1,
        //    authMechanism=MONGODB-OIDC, and the OIDC human callback.
        MongoClientSettings clientSettings = createSettingsMulti(getUserWithDomain("test_user1"), createHumanCallback());
        // #. Perform a find operation that succeeds
        performFind(clientSettings);
    }

    @Test
    public void testh1p4MultiplePrincipalUser2() {
        assumeTestEnvironment();
        //- Create a human callback that reads in the generated ``test_user2`` token file.
        //- Create a client with ``MONGODB_URI_MULTI``, a username of ``test_user2``,
        //  ``authMechanism=MONGODB-OIDC``, and the OIDC human callback.
        MongoClientSettings clientSettings = createSettingsMulti(getUserWithDomain("test_user2"), createHumanCallback()
                .setPathSupplier(() -> tokenQueue("test_user2").remove()));
        performFind(clientSettings);
    }

    @Test
    public void testh1p5MultiplePrincipalNoUser() {
        assumeTestEnvironment();
        // Create an OIDC configured client with `MONGODB_URI_MULTI` and no username.
        MongoClientSettings clientSettings = createSettingsMulti(null, createHumanCallback());
        // Assert that a `find` operation fails.
        assertFindFails(clientSettings, MongoCommandException.class, "Authentication failed");
    }

    @Test
    public void testh1p6AllowedHostsBlocked() {
        assumeTestEnvironment();
        //- Create a default OIDC client, with an ``ALLOWED_HOSTS`` that is an empty list.
        //- Assert that a ``find`` operation fails with a client-side error.
        MongoClientSettings clientSettings1 = createSettings(getOidcUri(),
                createHumanCallback(), null, OIDC_HUMAN_CALLBACK_KEY, Collections.emptyList());
        assertFindFails(clientSettings1, MongoSecurityException.class, "not permitted by ALLOWED_HOSTS");

        //- Create a client that uses the URL
        //  ``mongodb://localhost/?authMechanism=MONGODB-OIDC&ignored=example.com``, a
        //  human callback, and an ``ALLOWED_HOSTS`` that contains ``["example.com"]``.
        //- Assert that a ``find`` operation fails with a client-side error.
        MongoClientSettings clientSettings2 = createSettings(getOidcUri() + "&ignored=example.com",
                createHumanCallback(), null, OIDC_HUMAN_CALLBACK_KEY, Arrays.asList("example.com"));
        assertFindFails(clientSettings2, MongoSecurityException.class, "not permitted by ALLOWED_HOSTS");
    }

    // Not a prose test
    @Test
    public void testAllowedHostsDisallowedInConnectionString() {
        String string = "mongodb://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties=ALLOWED_HOSTS:localhost";
        assertCause(IllegalArgumentException.class,
                "connection string contains disallowed mechanism properties",
                () ->  new ConnectionString(string));
    }

    @Test
    public void testh1p7AllowedHostsInConnectionStringIgnored() {
        // example.com changed to localhost, because resolveAdditionalQueryParametersFromTxtRecords
        // fails with "Failed looking up TXT record for host example.com"
        String string = "mongodb+srv://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties=ALLOWED_HOSTS:%5B%22localhost%22%5D";
        assertCause(IllegalArgumentException.class,
                "connection string contains disallowed mechanism properties",
                () ->  new ConnectionString(string));
    }

    @Test
    public void testh1p8MachineIdpWithHumanCallback() {
        assumeTrue(getenv("OIDC_IS_LOCAL") != null);

        TestCallback callback = createHumanCallback()
                .setPathSupplier(() -> oidcTokenDirectory() + "test_machine");
        MongoClientSettings clientSettings = createSettingsHuman(
                "test_machine", callback, getOidcUri());
        performFind(clientSettings);
    }

    @Test
    public void testh2p1ValidCallbackInputs() {
        assumeTestEnvironment();
        TestCallback callback1 = createHumanCallback();
        OidcCallback callback2 = (context) -> {
            MongoCredential.IdpInfo idpInfo = assertNotNull(context.getIdpInfo());
            assertTrue(assertNotNull(idpInfo.getClientId()).startsWith("0oad"));
            assertTrue(idpInfo.getIssuer().endsWith("mock-identity-config-oidc"));
            assertEquals(Arrays.asList("fizz", "buzz"), idpInfo.getRequestScopes());
            assertEquals(Duration.ofMinutes(5), context.getTimeout());
            return callback1.onRequest(context);
        };
        MongoClientSettings clientSettings = createHumanSettings(callback2, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            // Ensure that callback was called
            assertEquals(1, callback1.getInvocations());
        }
    }

    @Test
    public void testh2p2HumanCallbackReturnsMissingData() {
        assumeTestEnvironment();
        //noinspection ConstantConditions
        OidcCallback callbackNull = (context) -> null;
        assertFindFails(createHumanSettings(callbackNull, null),
                MongoConfigurationException.class,
                "Result of callback must not be null");

        //noinspection ConstantConditions
        OidcCallback callback =
                (context) -> new OidcCallbackResult(null);
        assertFindFails(createHumanSettings(callback, null),
                IllegalArgumentException.class,
                "accessToken can not be null");
    }

    // not a prose test
    @Test
    public void testRefreshTokenAbsent() {
        // additionally, check validation for refresh in machine workflow:
        OidcCallback callbackMachineRefresh =
                (context) -> new OidcCallbackResult("access", Duration.ZERO, "exists");
        assertFindFails(createSettings(callbackMachineRefresh),
                MongoConfigurationException.class,
                "Refresh token must only be provided in human workflow");
    }

    @Test
    public void testh2p3RefreshTokenPassed() {
        assumeTestEnvironment();
        AtomicInteger refreshTokensProvided = new AtomicInteger();
        TestCallback callback1 = createHumanCallback();
        OidcCallback callback2 = (context) -> {
            if (context.getRefreshToken() != null) {
                refreshTokensProvided.incrementAndGet();
            }
            return callback1.onRequest(context);
        };
        MongoClientSettings clientSettings = createHumanSettings(callback2, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(2, callback1.getInvocations());
            assertEquals(1, refreshTokensProvided.get());
        }
    }

    @Test
    public void testh3p1UsesSpecAuthIfCachedToken() {
        assumeTestEnvironment();
        MongoClientSettings clientSettings = createHumanSettings(createHumanCallback(), null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            failCommandAndCloseConnection("find", 1);
            assertCause(MongoSocketException.class,
                    "Prematurely reached end of stream",
                    () -> performFind(mongoClient));
            failCommand(18, 1, "saslStart");
            performFind(mongoClient);
        }
    }

    @Test
    public void testh3p2NoSpecAuthIfNoCachedToken() {
        assumeTestEnvironment();
        failCommand(18, 1, "saslStart");
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);
        assertFindFails(createHumanSettings(createHumanCallback(), commandListener),
                MongoCommandException.class,
                "Command failed with error 18");
        assertEquals(Arrays.asList(
                "isMaster started",
                "isMaster succeeded",
                "saslStart started",
                "saslStart failed"
        ), listener.getEventStrings());
        listener.clear();
    }

    @Test
    public void testh4p1ReauthenticationSucceeds() {
        assumeTestEnvironment();
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);
        TestCallback callback = createHumanCallback()
                .setEventListener(listener);
        MongoClientSettings clientSettings = createHumanSettings(callback, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            listener.clear();
            assertEquals(1, callback.getInvocations());
            failCommand(391, 1, "find");
            // Perform another find operation that succeeds.
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
        assumeTestEnvironment();
        TestCallback callback = createHumanCallback();
        MongoClientSettings clientSettings = createHumanSettings(callback, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            assertEquals(1, callback.getInvocations());

            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(2, callback.getInvocations());
        }
    }


    @Test
    public void testh4p3SucceedsAfterRefreshFails() {
        assumeTestEnvironment();
        TestCallback callback1 = createHumanCallback();
        OidcCallback callback2 = (context) -> {
            OidcCallbackResult oidcCallbackResult = callback1.onRequest(context);
            return new OidcCallbackResult(oidcCallbackResult.getAccessToken(), Duration.ofMinutes(5), "BAD_REFRESH");
        };
        MongoClientSettings clientSettings = createHumanSettings(callback2, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            failCommand(391, 1, "find");
            performFind(mongoClient);
            assertEquals(2, callback1.getInvocations());
        }
    }

    @Test
    public void testh4p4Fails() {
        assumeTestEnvironment();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_expires");
        TestCallback callback1 = createHumanCallback()
                .setPathSupplier(() -> tokens.remove());
        OidcCallback callback2 = (context) -> {
            OidcCallbackResult oidcCallbackResult = callback1.onRequest(context);
            return new OidcCallbackResult(oidcCallbackResult.getAccessToken(), Duration.ofMinutes(5), "BAD_REFRESH");
        };
        MongoClientSettings clientSettings = createHumanSettings(callback2, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            assertEquals(1, callback1.getInvocations());
            failCommand(391, 1, "find");
            assertCause(MongoCommandException.class,
                    "Command failed with error 18",
                    () -> performFind(mongoClient));
            assertEquals(3, callback1.getInvocations());
        }
    }

    // Not a prose test
    @Test
    public void testErrorClearsCache() {
        assumeTestEnvironment();
        // #. Create a new client with a valid request callback that
        //    gives credentials that expire within 5 minutes and
        //    a refresh callback that gives invalid credentials.
        TestListener listener = new TestListener();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_expires",
                "test_user1_1");
        TestCallback callback = createHumanCallback()
                .setRefreshToken("refresh")
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);

        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createHumanSettings(callback, commandListener);
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


    private MongoClientSettings createSettings(final OidcCallback callback) {
        return createSettings(getOidcUri(), callback, null);
    }

    public MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final TestCallback callback) {
        return createSettings(connectionString, callback, null);
    }

    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcCallback callback,
            @Nullable final CommandListener commandListener) {
        String cleanedConnectionString = callback == null ? connectionString : connectionString
                .replace("ENVIRONMENT:azure,", "")
                .replace("ENVIRONMENT:gcp,", "")
                .replace("&authMechanismProperties=ENVIRONMENT:k8s", "")
                .replace("ENVIRONMENT:test,", "");
        return createSettings(cleanedConnectionString, callback, commandListener, OIDC_CALLBACK_KEY);
    }

    private MongoClientSettings createHumanSettings(
            final OidcCallback callback, @Nullable final TestCommandListener commandListener) {
        return createHumanSettings(getOidcUri(), callback, commandListener);
    }

    private MongoClientSettings createHumanSettings(
            final String connectionString,
            @Nullable final OidcCallback callback,
            @Nullable final CommandListener commandListener) {
        return createSettings(connectionString, callback, commandListener, OIDC_HUMAN_CALLBACK_KEY);
    }

    private MongoClientSettings createSettings(
            final String connectionString,
            final @Nullable OidcCallback callback,
            @Nullable final CommandListener commandListener,
            final String oidcCallbackKey) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = assertNotNull(cs.getCredential());
        if (callback != null) {
            credential = credential.withMechanismProperty(oidcCallbackKey, callback);
        }
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
            @Nullable final OidcCallback callback,
            @Nullable final CommandListener commandListener,
            final String oidcCallbackKey,
            @Nullable final List<String> allowedHosts) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = cs.getCredential()
                .withMechanismProperty(oidcCallbackKey, callback)
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

    private MongoClientSettings createSettingsMulti(@Nullable final String user, final OidcCallback callback) {
        return createSettingsHuman(user, callback, getOidcUriMulti());
    }

    private MongoClientSettings createSettingsHuman(@Nullable final String user, final OidcCallback callback, final String oidcUri) {
        ConnectionString cs = new ConnectionString(oidcUri);
        MongoCredential credential = MongoCredential.createOidcCredential(user)
                .withMechanismProperty(OIDC_HUMAN_CALLBACK_KEY, callback);
        return MongoClientSettings.builder()
                .applicationName(appName)
                .applyConnectionString(cs)
                .retryReads(false)
                .credential(credential)
                .build();
    }

    private void performFind(final MongoClientSettings settings) {
        try (MongoClient mongoClient = createMongoClient(settings)) {
            performFind(mongoClient);
        }
    }

    private <T extends Throwable> void assertFindFails(
            final MongoClientSettings settings,
            final Class<T> expectedExceptionOrCause,
            final String expectedMessage) {
        try (MongoClient mongoClient = createMongoClient(settings)) {
            assertCause(expectedExceptionOrCause, expectedMessage, () -> performFind(mongoClient));
        }
    }

    private static void performFind(final MongoClient mongoClient) {
        performFind(mongoClient, null);
    }

    private static void performFind(final MongoClient mongoClient, @Nullable final ClientSession session) {
        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("test");
        FindIterable<Document> findIterable = session == null ? collection.find() : collection.find(session);
        findIterable.first();
    }

    protected void delayNextFind() {

        try (MongoClient client = createMongoClient(Fixture.getMongoClientSettings())) {
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
        try (MongoClient mongoClient = createMongoClient(Fixture.getMongoClientSettings())) {
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
        try (MongoClient mongoClient = createMongoClient(Fixture.getMongoClientSettings())) {
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
            return callback(context);
        }

        private OidcCallbackResult callback(final OidcCallbackContext context) {
            if (concurrentTracker != null) {
                if (concurrentTracker.get() > 0) {
                    throw new RuntimeException("Callbacks should not be invoked by multiple threads.");
                }
                concurrentTracker.incrementAndGet();
            }
            try {
                invocations.incrementAndGet();
                try {
                    simulateDelay();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                MongoCredential credential = assertNotNull(new ConnectionString(getOidcUri()).getCredential());
                String oidcEnv = getOidcEnv();
                OidcCallback c;
                if (oidcEnv.contains("azure")) {
                    c = OidcAuthenticator.getAzureCallback(credential);
                } else if (oidcEnv.contains("gcp")) {
                    c = OidcAuthenticator.getGcpCallback(credential);
                } else if (oidcEnv.contains("k8s")) {
                    c = OidcAuthenticator.getK8sCallback();
                } else {
                    c = getProseTestCallback();
                }
                return c.onRequest(context);

            } finally {
                if (concurrentTracker != null) {
                    concurrentTracker.decrementAndGet();
                }
            }
        }

        private OidcCallback getProseTestCallback() {
            return (x) -> {
                try {
                    Path path = Paths.get(pathSupplier == null
                            ? getTestTokenFilePath()
                            : pathSupplier.get());
                    String accessToken = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                    if (testListener != null) {
                        testListener.add("read access token: " + path.getFileName());
                    }
                    return new OidcCallbackResult(accessToken, Duration.ZERO, refreshToken);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
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

    private long msElapsedSince(final long timeOfStart) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - timeOfStart);
    }
}
