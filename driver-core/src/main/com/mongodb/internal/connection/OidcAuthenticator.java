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

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.RequestCallbackResult;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.Locks;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;

import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.mongodb.AuthenticationMechanism.MONGODB_OIDC;
import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.PROVIDER_NAME_KEY;
import static com.mongodb.MongoCredential.OIDC_CALLBACK_KEY;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.connection.OidcAuthenticator.OidcValidator.validateBeforeUse;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class OidcAuthenticator extends SaslAuthenticator {

    private static final List<String> SUPPORTED_PROVIDERS = Arrays.asList("aws");

    private static final Duration CALLBACK_TIMEOUT = Duration.ofMinutes(5);

    public static final String AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";
    private static final int CALLBACK_API_VERSION_NUMBER = 1;

    @Nullable
    private String connectionLastAccessToken;

    private FallbackState fallbackState = FallbackState.INITIAL;

    @Nullable
    private BsonDocument speculativeAuthenticateResponse;

    public OidcAuthenticator(final MongoCredentialWithCache credential,
            final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
        validateBeforeUse(credential.getCredential());

        if (getMongoCredential().getAuthenticationMechanism() != MONGODB_OIDC) {
            throw new MongoException("Incorrect mechanism: " + getMongoCredential().getMechanism());
        }
    }

    @Override
    public String getMechanismName() {
        return MONGODB_OIDC.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        return new OidcSaslClient(mongoCredentialWithCache);
    }

    @Override
    @Nullable
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        try {
            if (isAutomaticAuthentication()) {
                return wrapInSpeculative(prepareAwsTokenFromFileAsJwt());
            }
            String cachedAccessToken = getCachedAccessToken();
            if (cachedAccessToken != null) {
                return wrapInSpeculative(prepareTokenAsJwt(cachedAccessToken));
            } else {
                // otherwise, skip speculative auth
                return null;
            }
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    private BsonDocument wrapInSpeculative(final byte[] outToken) {
        BsonDocument startDocument = createSaslStartCommandDocument(outToken)
                .append("db", new BsonString(getMongoCredential().getSource()));
        appendSaslStartOptions(startDocument);
        return startDocument;
    }

    @Override
    @Nullable
    public BsonDocument getSpeculativeAuthenticateResponse() {
        BsonDocument response = speculativeAuthenticateResponse;
        // response should only be read once
        this.speculativeAuthenticateResponse = null;
        if (response == null) {
            this.connectionLastAccessToken = null;
        }
        return response;
    }

    @Override
    public void setSpeculativeAuthenticateResponse(@Nullable final BsonDocument response) {
        speculativeAuthenticateResponse = response;
    }

    @Nullable
    private OidcRequestCallback getRequestCallback() {
        return getMongoCredentialWithCache()
                .getCredential()
                .getMechanismProperty(OIDC_CALLBACK_KEY, null);
    }

    @Override
    public void reauthenticate(final InternalConnection connection) {
        assertTrue(connection.opened());
        authenticationLoop(connection, connection.getDescription());
    }

    @Override
    public void reauthenticateAsync(final InternalConnection connection, final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            assertTrue(connection.opened());
            authenticationLoopAsync(connection, connection.getDescription(), c);
        }).finish(callback);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        assertFalse(connection.opened());
        authenticationLoop(connection, connectionDescription);
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            assertFalse(connection.opened());
            authenticationLoopAsync(connection, connectionDescription, c);
        }).finish(callback);
    }

    private static boolean triggersRetry(@Nullable final Throwable t) {
        if (t instanceof MongoSecurityException) {
            MongoSecurityException e = (MongoSecurityException) t;
            Throwable cause = e.getCause();
            if (cause instanceof MongoCommandException) {
                MongoCommandException commandCause = (MongoCommandException) cause;
                return commandCause.getErrorCode() == 18;
            }
        }
        return false;
    }

    private void authenticationLoop(final InternalConnection connection, final ConnectionDescription description) {
        fallbackState = FallbackState.INITIAL;
        while (true) {
            try {
                super.authenticate(connection, description);
                break;
            } catch (MongoSecurityException e) {
                if (triggersRetry(e) && shouldRetryHandler()) {
                    continue;
                }
                throw e;
            }
        }
    }

    private void authenticationLoopAsync(final InternalConnection connection, final ConnectionDescription description,
            final SingleResultCallback<Void> callback) {
        fallbackState = FallbackState.INITIAL;
        beginAsync().thenRunRetryingWhile(
                c -> super.authenticateAsync(connection, description, c),
                e -> triggersRetry(e) && shouldRetryHandler()
        ).finish(callback);
    }

    private byte[] evaluate(final byte[] challenge) {
        if (isAutomaticAuthentication()) {
            return prepareAwsTokenFromFileAsJwt();
        }
        byte[][] jwt = new byte[1][];
        Locks.withLock(getMongoCredentialWithCache().getOidcLock(), () -> {
            String cachedAccessToken = validatedCachedAccessToken();

            if (cachedAccessToken != null) {
                jwt[0] = prepareTokenAsJwt(cachedAccessToken);
                fallbackState = FallbackState.PHASE_1_CACHED_TOKEN;
            } else {
                // cache is empty
                OidcRequestCallback requestCallback = assertNotNull(getRequestCallback());
                RequestCallbackResult result = requestCallback.onRequest(new OidcRequestContextImpl(CALLBACK_TIMEOUT));
                jwt[0] = populateCacheWithCallbackResultAndPrepareJwt(result);
                fallbackState = FallbackState.PHASE_2_CALLBACK_TOKEN;
            }
        });
        return jwt[0];
    }

    /**
     * Must be guarded by {@link MongoCredentialWithCache#getOidcLock()}.
     */
    @Nullable
    private String validatedCachedAccessToken() {
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
        String cachedAccessToken = getCachedAccessToken();
        String invalidConnectionAccessToken = connectionLastAccessToken;

        if (cachedAccessToken != null) {
            boolean cachedTokenIsInvalid = cachedAccessToken.equals(invalidConnectionAccessToken);
            if (cachedTokenIsInvalid) {
                mongoCredentialWithCache.setOidcCacheEntry(cacheEntry.clearAccessToken());
                cachedAccessToken = null;
            }
        }
        return cachedAccessToken;
    }

    private boolean isAutomaticAuthentication() {
        return getRequestCallback() == null;
    }

    private boolean clientIsComplete() {
        return true; // all possibilities are 1-step
    }

    private boolean shouldRetryHandler() {
        Locks.withLock(getMongoCredentialWithCache().getOidcLock(), () -> {
            validatedCachedAccessToken();
        });
        return fallbackState == FallbackState.PHASE_1_CACHED_TOKEN;
    }

    @Nullable
    private String getCachedAccessToken() {
        return getMongoCredentialWithCache()
                .getOidcCacheEntry()
                .getCachedAccessToken();
    }

    static final class OidcCacheEntry {
        @Nullable
        private final String accessToken;

        @Override
        public String toString() {
            return "OidcCacheEntry{"
                    + "\n accessToken=[omitted]"
                    + '}';
        }

        OidcCacheEntry(final RequestCallbackResult requestCallbackResult) {
            this.accessToken = requestCallbackResult.getAccessToken();
        }

        OidcCacheEntry() {
            this((String) null);
        }

        private OidcCacheEntry(@Nullable final String accessToken) {
            this.accessToken = accessToken;
        }

        @Nullable
        String getCachedAccessToken() {
            return accessToken;
        }

        OidcCacheEntry clearAccessToken() {
            return new OidcCacheEntry((String) null);
        }
    }

    private final class OidcSaslClient extends SaslClientImpl {

        private OidcSaslClient(final MongoCredentialWithCache mongoCredentialWithCache) {
            super(mongoCredentialWithCache.getCredential());
        }

        @Override
        public byte[] evaluateChallenge(final byte[] challenge) {
            return evaluate(challenge);
        }

        @Override
        public boolean isComplete() {
            return clientIsComplete();
        }

    }

    private static String readAwsTokenFromFile() {
        String path = System.getenv(AWS_WEB_IDENTITY_TOKEN_FILE);
        if (path == null) {
            throw new MongoClientException(
                    format("Environment variable must be specified: %s", AWS_WEB_IDENTITY_TOKEN_FILE));
        }
        try {
            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new MongoClientException(format(
                    "Could not read file specified by environment variable: %s at path: %s",
                    AWS_WEB_IDENTITY_TOKEN_FILE, path), e);
        }
    }

    private byte[] populateCacheWithCallbackResultAndPrepareJwt(@Nullable final RequestCallbackResult requestCallbackResult) {
        if (requestCallbackResult == null) {
            throw new MongoConfigurationException("Result of callback must not be null");
        }
        OidcCacheEntry newEntry = new OidcCacheEntry(requestCallbackResult);
        getMongoCredentialWithCache().setOidcCacheEntry(newEntry);
        return prepareTokenAsJwt(requestCallbackResult.getAccessToken());
    }

    private byte[] prepareTokenAsJwt(final String accessToken) {
        connectionLastAccessToken = accessToken;
        return toJwtDocument(accessToken);
    }

    private static byte[] prepareAwsTokenFromFileAsJwt() {
        String accessToken = readAwsTokenFromFile();
        return toJwtDocument(accessToken);
    }

    private static byte[] toJwtDocument(final String accessToken) {
        return toBson(new BsonDocument().append("jwt", new BsonString(accessToken)));
    }

    /**
     * Contains all validation logic for OIDC in one location
     */
    public static final class OidcValidator {
        private OidcValidator() {
        }

        public static void validateOidcCredentialConstruction(
                final String source,
                final Map<String, Object> mechanismProperties) {

            if (!"$external".equals(source)) {
                throw new IllegalArgumentException("source must be '$external'");
            }

            Object providerName = mechanismProperties.get(PROVIDER_NAME_KEY.toLowerCase());
            if (providerName != null) {
                if (!(providerName instanceof String) || !SUPPORTED_PROVIDERS.contains(providerName)) {
                    throw new IllegalArgumentException(PROVIDER_NAME_KEY + " must be one of: " + SUPPORTED_PROVIDERS);
                }
            }
        }

        public static void validateCreateOidcCredential(@Nullable final char[] password) {
            if (password != null) {
                throw new IllegalArgumentException("password must not be specified for "
                        + AuthenticationMechanism.MONGODB_OIDC);
            }
        }

        @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
        public static void validateBeforeUse(final MongoCredential credential) {
            String userName = credential.getUserName();
            Object providerName = credential.getMechanismProperty(PROVIDER_NAME_KEY, null);
            Object requestCallback = credential.getMechanismProperty(OIDC_CALLBACK_KEY, null);
            if (providerName == null) {
                // callback
                if (requestCallback == null) {
                    throw new IllegalArgumentException("Either " + PROVIDER_NAME_KEY + " or "
                            + OIDC_CALLBACK_KEY + " must be specified");
                }
            } else {
                // automatic
                if (userName != null) {
                    throw new IllegalArgumentException("user name must not be specified when " + PROVIDER_NAME_KEY + " is specified");
                }
                if (requestCallback != null) {
                    throw new IllegalArgumentException(OIDC_CALLBACK_KEY + " must not be specified when " + PROVIDER_NAME_KEY + " is specified");
                }
            }
        }
    }


    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static class OidcRequestContextImpl implements OidcRequestContext {
        private final Duration timeout;

        OidcRequestContextImpl(final Duration timeout) {
            this.timeout = assertNotNull(timeout);
        }

        @Override
        public Duration getTimeout() {
            return timeout;
        }

        @Override
        public int getVersion() {
            return CALLBACK_API_VERSION_NUMBER;
        }
    }

    /**
     * What was sent in the last request by this connection to the server.
     */
    private enum FallbackState {
        INITIAL,
        PHASE_1_CACHED_TOKEN,
        PHASE_2_CALLBACK_TOKEN
    }
}
