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
import com.mongodb.MongoCredential.IdpResponse;
import com.mongodb.MongoCredential.OidcRefreshCallback;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opentest4j.AssertionFailedError;
import org.opentest4j.MultipleFailuresError;

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

import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.IdpInfo;
import static com.mongodb.MongoCredential.OidcRefreshContext;
import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.PROVIDER_NAME_KEY;
import static com.mongodb.MongoCredential.REFRESH_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.REQUEST_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.createOidcCredential;
import static com.mongodb.client.TestHelper.setEnvironmentVariable;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    private static final String AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";

    public static final String TOKEN_DIRECTORY = "/tmp/tokens/"; // TODO-OIDC

    protected static final String OIDC_URL = "mongodb://localhost/?authMechanism=MONGODB-OIDC";
    private static final String AWS_OIDC_URL =
            "mongodb://localhost/?authMechanism=MONGODB-OIDC&authMechanismProperties=PROVIDER_NAME:aws";
    private String appName;

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

    @ParameterizedTest
    @CsvSource(delimiter = '#', value = {
            // 1.1 to 1.5:
            "test1p1 # test_user1 # " + OIDC_URL,
            "test1p2 # test_user1 # mongodb://test_user1@localhost/?authMechanism=MONGODB-OIDC",
            "test1p3 # test_user1 # mongodb://test_user1@localhost:27018/?authMechanism=MONGODB-OIDC&directConnection=true&readPreference=secondaryPreferred",
            "test1p4 # test_user2 # mongodb://test_user2@localhost:27018/?authMechanism=MONGODB-OIDC&directConnection=true&readPreference=secondaryPreferred",
            "test1p5 # invalid # mongodb://localhost:27018/?authMechanism=MONGODB-OIDC&directConnection=true&readPreference=secondaryPreferred",
    })
    public void test1CallbackDrivenAuth(final String name, final String file, final String url) {
        boolean shouldPass = !file.equals("invalid");
        setOidcFile(file);
        // #. Create a request callback that returns a valid token.
        OidcRequestCallback onRequest = createCallback();
        // #. Create a client with a URL of the form ... and the OIDC request callback.
        MongoClientSettings clientSettings = createSettings(url, onRequest, null);
        // #. Perform a find operation that succeeds / fails
        if (shouldPass) {
            performFind(clientSettings);
        } else {
            performFind(
                    clientSettings,
                    MongoCommandException.class,
                    "Command failed with error 18 (AuthenticationFailed)");
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '#', value = {
            // 1.6, both variants:
            "'' # " + OIDC_URL,
            "example.com # mongodb://localhost/?authMechanism=MONGODB-OIDC&ignored=example.com",
    })
    public void test1p6CallbackDrivenAuthAllowedHostsBlocked(final String allowedHosts, final String url) {
        // Create a client that uses the OIDC url and a request callback, and an ALLOWED_HOSTS that contains...
        List<String> allowedHostsList = asList(allowedHosts.split(","));
        MongoClientSettings settings = createSettings(url, createCallback(), null, allowedHostsList, null);
        // #. Assert that a find operation fails with a client-side error.
        performFind(settings, MongoSecurityException.class, "");
    }

    @Test
    public void test1p7LockAvoidsExtraCallbackCalls() {
        proveThatConcurrentCallbacksThrow();
        // The test requires that two operations are attempted concurrently.
        // The delay on the next find should cause the initial request to delay
        // and the ensuing refresh to block, rather than entering onRefresh.
        // After blocking, this ensuing refresh thread will enter onRefresh.
        AtomicInteger concurrent = new AtomicInteger();
        TestCallback onRequest = createCallback().setExpired().setConcurrentTracker(concurrent);
        TestCallback onRefresh = createCallback().setConcurrentTracker(concurrent);
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            assertEquals(1, onRequest.getInvocations());
            assertEquals(1, onRefresh.getInvocations());
        }
    }

    public void proveThatConcurrentCallbacksThrow() {
        // ensure that, via delay, test callbacks throw when invoked concurrently
        AtomicInteger c = new AtomicInteger();
        TestCallback request = createCallback().setConcurrentTracker(c).setDelayMs(5);
        TestCallback refresh = createCallback().setConcurrentTracker(c);
        IdpInfo serverInfo = new OidcAuthenticator.IdpInfoImpl("issuer", "clientId", asList());
        executeAll(() -> {
            sleep(2);
            assertThrows(RuntimeException.class, () -> {
                refresh.onRefresh(new OidcAuthenticator.OidcRefreshContextImpl(serverInfo, "refToken", Duration.ofSeconds(1234)));
            });
        }, () -> {
            request.onRequest(new OidcAuthenticator.OidcRequestContextImpl(serverInfo, Duration.ofSeconds(1234)));
        });
    }

    private void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @CsvSource(delimiter = '#', value = {
            // 2.1 to 2.3:
            "test2p1 # test_user1 # " + AWS_OIDC_URL,
            "test2p2 # test_user1 # mongodb://localhost:27018/?authMechanism=MONGODB-OIDC&authMechanismProperties=PROVIDER_NAME:aws&directConnection=true&readPreference=secondaryPreferred",
            "test2p3 # test_user2 # mongodb://localhost:27018/?authMechanism=MONGODB-OIDC&authMechanismProperties=PROVIDER_NAME:aws&directConnection=true&readPreference=secondaryPreferred",
    })
    public void test2AwsAutomaticAuth(final String name, final String file, final String url) {
        setOidcFile(file);
        // #. Create a client with a url of the form ...
        MongoCredential credential = createOidcCredential(null)
                .withMechanismProperty(PROVIDER_NAME_KEY, "aws");
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applicationName(appName)
                .credential(credential)
                .applyConnectionString(new ConnectionString(url))
                .build();
        // #. Perform a find operation that succeeds.
        performFind(clientSettings);
    }

    @Test
    public void test2p4AllowedHostsIgnored() {
        MongoClientSettings settings = createSettings(
                AWS_OIDC_URL, null, null, Arrays.asList(), null);
        performFind(settings);
    }

    @Test
    public void test3p1ValidCallbacks() {
        String connectionString = "mongodb://test_user1@localhost/?authMechanism=MONGODB-OIDC";
        String expectedClientId = "0oadp0hpl7q3UIehP297";
        String expectedIssuer = "https://ebgxby0dw8.execute-api.us-west-1.amazonaws.com/default/mock-identity-config-oidc";
        Duration expectedSeconds = Duration.ofMinutes(5);

        TestCallback onRequest = createCallback().setExpired();
        TestCallback onRefresh = createCallback();
        // #. Verify that the request callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        // #. Verify that the refresh callback was called with the appropriate
        //    inputs, including the timeout parameter if possible.
        OidcRequestCallback onRequest2 = (context) -> {
            assertEquals(expectedClientId, context.getIdpInfo().getClientId());
            assertEquals(expectedIssuer, context.getIdpInfo().getIssuer());
            assertEquals(Arrays.asList(), context.getIdpInfo().getRequestScopes());
            assertEquals(expectedSeconds, context.getTimeout());
            return onRequest.onRequest(context);
        };
        OidcRefreshCallback onRefresh2 = (context) -> {
            assertEquals(expectedClientId, context.getIdpInfo().getClientId());
            assertEquals(expectedIssuer, context.getIdpInfo().getIssuer());
            assertEquals(Arrays.asList(), context.getIdpInfo().getRequestScopes());
            assertEquals(expectedSeconds, context.getTimeout());
            assertEquals("refreshToken", context.getRefreshToken());
            return onRefresh.onRefresh(context);
        };
        MongoClientSettings clientSettings = createSettings(connectionString, onRequest2, onRefresh2);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            // Ensure that both callbacks were called
            assertEquals(1, onRequest.getInvocations());
            assertEquals(1, onRefresh.getInvocations());
        }
    }

    @Test
    public void test3p2RequestCallbackReturnsNull() {
        //noinspection ConstantConditions
        OidcRequestCallback onRequest = (context) -> null;
        MongoClientSettings settings = this.createSettings(OIDC_URL, onRequest, null);
        performFind(settings, MongoConfigurationException.class, "Result of callback must not be null");
    }

    @Test
    public void test3p3RefreshCallbackReturnsNull() {
        TestCallback onRequest = createCallback().setExpired().setDelayMs(100);
        //noinspection ConstantConditions
        OidcRefreshCallback onRefresh = (context) -> null;
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            try {
                executeAll(2, () -> performFind(mongoClient));
            } catch (MultipleFailuresError actual) {
                assertEquals(1, actual.getFailures().size());
                assertCause(
                        MongoConfigurationException.class,
                        "Result of callback must not be null",
                        actual.getFailures().get(0));
            }
            assertEquals(1, onRequest.getInvocations());
        }
    }

    @Test
    public void test3p4RequestCallbackReturnsInvalidData() {
        // #. Create a client with a request callback that returns data not
        //    conforming to the OIDCRequestTokenResult with missing field(s).
        // #. ... with extra field(s). - not possible
        OidcRequestCallback onRequest = (context) -> {
            //noinspection ConstantConditions
            return new IdpResponse(null, null, null);
        };
        // we ensure that the error is propagated
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, null);
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
    public void test3p5RefreshCallbackReturnsInvalidData() {
        TestCallback onRequest = createCallback().setExpired();
        OidcRefreshCallback onRefresh = (context) -> {
            //noinspection ConstantConditions
            return new IdpResponse(null, null, null);
        };
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            try {
                executeAll(2, () -> performFind(mongoClient));
            } catch (MultipleFailuresError actual) {
                assertEquals(1, actual.getFailures().size());
                assertCause(
                        IllegalArgumentException.class,
                        "accessToken can not be null",
                        actual.getFailures().get(0));
            }
            assertEquals(1, onRequest.getInvocations());
        }
    }

    // 3.6   Refresh Callback Returns Extra Data - not possible due to use of class

    @Test
    public void test4p1CachedCredentialsCacheWithRefresh() {
        // #. Create a new client with a request callback that gives credentials that expire in one minute.
        TestCallback onRequest = createCallback().setExpired();
        TestCallback onRefresh = createCallback();
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Create a new client with the same request callback and a refresh callback.
            // Instead:
            // 1. Delay the first find, causing the second find to authenticate a second connection
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            // #. Ensure that a find operation adds credentials to the cache.
            // #. Ensure that a find operation results in a call to the refresh callback.
            assertEquals(1, onRequest.getInvocations());
            assertEquals(1, onRefresh.getInvocations());
            // the refresh invocation will fail if the cached tokens are null
            // so a success implies that credentials were present in the cache
        }
    }

    @Test
    public void test4p2CachedCredentialsCacheWithNoRefresh() {
        // #. Create a new client with a request callback that gives credentials that expire in one minute.
        // #. Ensure that a find operation adds credentials to the cache.
        // #. Create a new client with a request callback but no refresh callback.
        // #. Ensure that a find operation results in a call to the request callback.
        TestCallback onRequest = createCallback().setExpired();
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, null);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            delayNextFind(); // cause both callbacks to be called
            executeAll(2, () -> performFind(mongoClient));
            // test is the same as 4.1, but no onRefresh, and assert that the onRequest is called twice
            assertEquals(2, onRequest.getInvocations());
        }
    }

    // 4.3   Cache key includes callback - skipped:
    // If the driver does not support using callback references or hashes as part of the cache key, skip this test.

    @Test
    public void test4p4ErrorClearsCache() {
        // #. Create a new client with a valid request callback that
        //    gives credentials that expire within 5 minutes and
        //    a refresh callback that gives invalid credentials.

        TestListener listener = new TestListener();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_expires",
                "test_user1_1");
        TestCallback onRequest = createCallback()
                .setExpired()
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);
        TestCallback onRefresh = createCallback()
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);

        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh, null, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Ensure that a find operation adds a new entry to the cache.
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "isMaster started",
                    "isMaster succeeded",
                    "onRequest invoked",
                    "read access token: test_user1",
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
                    "find failed",
                    "onRefresh invoked",
                    "read access token: test_user1_expires",
                    "saslStart started",
                    "saslStart failed",
                    // falling back to principal request, onRequest callback.
                    "saslStart started",
                    "saslStart succeeded",
                    "onRequest invoked",
                    "read access token: test_user1_expires",
                    "saslContinue started",
                    "saslContinue failed"
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
                    "onRequest invoked",
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

    // not a prose test.
    @Test
    public void testEventListenerMustNotLogReauthentication() {
        InternalStreamConnection.setRecordEverything(false);

        TestListener listener = new TestListener();
        ConcurrentLinkedQueue<String> tokens = tokenQueue(
                "test_user1",
                "test_user1_expires",
                "test_user1_expires",
                "test_user1_1");
        TestCallback onRequest = createCallback()
                .setExpired()
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);
        TestCallback onRefresh = createCallback()
                .setPathSupplier(() -> tokens.remove())
                .setEventListener(listener);

        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh, null, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            performFind(mongoClient);
            assertEquals(Arrays.asList(
                    "onRequest invoked",
                    "read access token: test_user1",
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());
            listener.clear();

            failCommand(391, 1, "find");
            assertThrows(MongoSecurityException.class, () -> performFind(mongoClient));
            assertEquals(Arrays.asList(
                    "find started",
                    "find failed",
                    "onRefresh invoked",
                    "read access token: test_user1_expires",
                    // falling back to principal request, onRequest callback
                    "onRequest invoked",
                    "read access token: test_user1_expires"
            ), listener.getEventStrings());
        }
    }

    @Test
    public void test4p5AwsAutomaticWorkflowDoesNotUseCache() {
        // #. Create a new client that uses the AWS automatic workflow.
        // #. Ensure that a find operation does not add credentials to the cache.
        setOidcFile("test_user1");
        MongoCredential credential = createOidcCredential(null)
                .withMechanismProperty(PROVIDER_NAME_KEY, "aws");
        ConnectionString connectionString = new ConnectionString(AWS_OIDC_URL);
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

    @Test
    public void test5SpeculativeAuthentication() {
        // #. We can only test the successful case, by verifying that saslStart is not called.
        // #. Create a client with a request callback that returns a valid token that will not expire soon.
        TestListener listener = new TestListener();
        TestCallback onRequest = createCallback().setEventListener(listener);
        TestCommandListener commandListener = new TestCommandListener(listener);
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, null, null, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // instead of setting failpoints for saslStart, we inspect events
            delayNextFind();
            executeAll(2, () -> performFind(mongoClient));

            List<String> events = listener.getEventStrings();
            assertFalse(events.stream().anyMatch(e -> e.contains("saslStart")));
            // onRequest is 2-step, so we expect 2 continues
            assertEquals(2, events.stream().filter(e -> e.contains("saslContinue started")).count());
            // confirm all commands are enabled
            assertTrue(events.stream().anyMatch(e -> e.contains("isMaster started")));
        }
    }

    // Not a prose test
    @Test
    public void testAutomaticAuthUsesSpeculative() {
        TestListener listener = new TestListener();
        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings settings = createSettings(
                AWS_OIDC_URL, null, null, Arrays.asList(), commandListener);
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
        // #. Create request and refresh callbacks that return valid credentials that will not expire soon.
        TestListener listener = new TestListener();
        TestCallback onRequest = createCallback().setEventListener(listener);
        TestCallback onRefresh = createCallback().setEventListener(listener);

        // #. Create a client with the callbacks and an event listener capable of listening for SASL commands.
        TestCommandListener commandListener = new TestCommandListener(listener);

        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh, null, commandListener);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {

            // #. Perform a find operation that succeeds.
            performFind(mongoClient);

            // #. Assert that the refresh callback has not been called.
            assertEquals(0, onRefresh.getInvocations());

            assertEquals(Arrays.asList(
                    // speculative:
                    "isMaster started",
                    "isMaster succeeded",
                    // onRequest:
                    "onRequest invoked",
                    "read access token: test_user1",
                    // jwt from onRequest:
                    "saslContinue started",
                    "saslContinue succeeded",
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
                    // find has triggered 391, and cleared the access token; fall back to refresh:
                    "onRefresh invoked",
                    "read access token: test_user1",
                    "saslStart started",
                    "saslStart succeeded",
                    // find retry succeeds:
                    "find started",
                    "find succeeded"
            ), listener.getEventStrings());

            // #. Assert that the refresh callback has been called once, if possible.
            assertEquals(1, onRefresh.getInvocations());
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
        TestCallback onRefresh = createCallback();
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
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
        TestCallback onRefresh = createCallback().setPathSupplier(() -> tokens.remove());
        MongoClientSettings clientSettings = createSettings(OIDC_URL, onRequest, onRefresh);
        try (MongoClient mongoClient = createMongoClient(clientSettings)) {
            // #. Peform a find operation on each ... that succeeds.
            delayNextFind();
            executeAll(2, () -> performFind(mongoClient));
            // #. Ensure that the request callback has been called once and the refresh callback has not been called.
            assertEquals(1, onRequest.getInvocations());
            assertEquals(0, onRefresh.getInvocations());

            failCommand(391, 2, "find");
            executeAll(2, () -> performFind(mongoClient));

            // #. Ensure that the request callback has been called once and the refresh callback has been called once.
            assertEquals(1, onRequest.getInvocations());
            assertEquals(1, onRefresh.getInvocations());
        }
    }

    public MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcRequestCallback onRequest,
            @Nullable final OidcRefreshCallback onRefresh) {
        return createSettings(connectionString, onRequest, onRefresh, null, null);
    }

    private MongoClientSettings createSettings(
            final String connectionString,
            @Nullable final OidcRequestCallback onRequest,
            @Nullable final OidcRefreshCallback onRefresh,
            @Nullable final List<String> allowedHosts,
            @Nullable final CommandListener commandListener) {
        ConnectionString cs = new ConnectionString(connectionString);
        MongoCredential credential = cs.getCredential()
                .withMechanismProperty(REQUEST_TOKEN_CALLBACK_KEY, onRequest)
                .withMechanismProperty(REFRESH_TOKEN_CALLBACK_KEY, onRefresh)
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
        try (MongoClient client = createMongoClient(createSettings(AWS_OIDC_URL, null, null))) {
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
                AWS_OIDC_URL, null, null))) {
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

    public static class TestCallback implements OidcRequestCallback, OidcRefreshCallback {
        private final AtomicInteger invocations = new AtomicInteger();
        @Nullable
        private final Integer expiresInSeconds;
        @Nullable
        private final Integer delayInMilliseconds;
        @Nullable
        private final AtomicInteger concurrentTracker;
        @Nullable
        private final TestListener testListener;
        @Nullable
        private final Supplier<String> pathSupplier;

        public TestCallback() {
            this(60 * 60, null, new AtomicInteger(), null, null);
        }

        public TestCallback(
                @Nullable final Integer expiresInSeconds,
                @Nullable final Integer delayInMilliseconds,
                @Nullable final AtomicInteger concurrentTracker,
                @Nullable final TestListener testListener,
                @Nullable final Supplier<String> pathSupplier) {
            this.expiresInSeconds = expiresInSeconds;
            this.delayInMilliseconds = delayInMilliseconds;
            this.concurrentTracker = concurrentTracker;
            this.testListener = testListener;
            this.pathSupplier = pathSupplier;
        }

        public int getInvocations() {
            return invocations.get();
        }

        @Override
        public IdpResponse onRequest(final OidcRequestContext context) {
            if (testListener != null) {
                testListener.add("onRequest invoked");
            }
            return callback();
        }

        @Override
        public IdpResponse onRefresh(final OidcRefreshContext context) {
            if (context.getRefreshToken() == null) {
                throw new IllegalArgumentException("refreshToken was null");
            }
            if (testListener != null) {
                testListener.add("onRefresh invoked");
            }
            return callback();
        }

        @NotNull
        private IdpResponse callback() {
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
                String refreshToken = "refreshToken";
                if (testListener != null) {
                    testListener.add("read access token: " + path.getFileName());
                }
                return new IdpResponse(
                        accessToken,
                        expiresInSeconds,
                        refreshToken);
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

        public TestCallback setExpiresInSeconds(final Integer expiresInSeconds) {
            return new TestCallback(
                    expiresInSeconds,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setDelayMs(final int milliseconds) {
            return new TestCallback(
                    this.expiresInSeconds,
                    milliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setConcurrentTracker(final AtomicInteger c) {
            return new TestCallback(
                    this.expiresInSeconds,
                    this.delayInMilliseconds,
                    c,
                    this.testListener,
                    this.pathSupplier);
        }

        public TestCallback setEventListener(final TestListener testListener) {
            return new TestCallback(
                    this.expiresInSeconds,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    testListener,
                    this.pathSupplier);
        }

        public TestCallback setPathSupplier(final Supplier<String> pathSupplier) {
            return new TestCallback(
                    this.expiresInSeconds,
                    this.delayInMilliseconds,
                    this.concurrentTracker,
                    this.testListener,
                    pathSupplier);
        }

        public TestCallback setExpired() {
            return this.setExpiresInSeconds(60);
        }
    }

    public TestCallback createCallback() {
        return new TestCallback();
    }
}
