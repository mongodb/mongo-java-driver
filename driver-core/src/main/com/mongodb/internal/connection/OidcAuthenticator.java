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
import com.mongodb.MongoCredential.IdpInfo;
import com.mongodb.MongoCredential.IdpResponse;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.Locks;
import com.mongodb.internal.Timeout;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.jetbrains.annotations.NotNull;

import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.AuthenticationMechanism.MONGODB_OIDC;
import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.DEFAULT_ALLOWED_HOSTS;
import static com.mongodb.MongoCredential.OidcRefreshCallback;
import static com.mongodb.MongoCredential.OidcRefreshContext;
import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.PROVIDER_NAME_KEY;
import static com.mongodb.MongoCredential.REFRESH_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.REQUEST_TOKEN_CALLBACK_KEY;
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

    private static final String AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";

    @Nullable
    private ServerAddress serverAddress;

    @Nullable
    private String connectionLastAccessToken;

    private FallbackState fallbackState = FallbackState.INITIAL;

    @Nullable
    private BsonDocument speculativeAuthenticateResponse;

    @Nullable
    private Function<byte[], byte[]> evaluateChallengeFunction;

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
        this.serverAddress = serverAddress;
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
            String cachedAccessToken = getValidCachedAccessToken();
            MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
            if (cachedAccessToken != null) {
                return wrapInSpeculative(prepareTokenAsJwt(cachedAccessToken));
            } else if (mongoCredentialWithCache.getOidcCacheEntry().getIdpInfo() == null) {
                String userName = mongoCredentialWithCache.getCredential().getUserName();
                return wrapInSpeculative(prepareUsername(userName));
            } else {
                // otherwise, skip speculative auth
                return null;
            }
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @NotNull
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
    private OidcRefreshCallback getRefreshCallback() {
        return getMongoCredentialWithCache()
                .getCredential()
                .getMechanismProperty(REFRESH_TOKEN_CALLBACK_KEY, null);
    }

    @Nullable
    private OidcRequestCallback getRequestCallback() {
        return getMongoCredentialWithCache()
                .getCredential()
                .getMechanismProperty(REQUEST_TOKEN_CALLBACK_KEY, null);
    }

    @Override
    public void reauthenticate(final InternalConnection connection) {
        assertTrue(connection.opened());
        authLock(connection, connection.getDescription());
    }

    @Override
    public void reauthenticateAsync(final InternalConnection connection, final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            assertTrue(connection.opened());
            authLockAsync(connection, connection.getDescription(), c);
        }).finish(callback);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        assertFalse(connection.opened());
        String accessToken = getValidCachedAccessToken();
        if (accessToken != null) {
            authenticateOptimistically(connection, connectionDescription, accessToken);
        } else {
            authLock(connection, connectionDescription);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            assertFalse(connection.opened());
            String accessToken = getValidCachedAccessToken();
            if (accessToken != null) {
                authenticateOptimisticallyAsync(connection, connectionDescription, accessToken, c);
            } else {
                authLockAsync(connection, connectionDescription, c);
            }
        }).finish(callback);
    }

    private void authenticateOptimistically(final InternalConnection connection,
            final ConnectionDescription connectionDescription, final String accessToken) {
        try {
            authenticateUsingFunction(connection, connectionDescription, (challenge) -> prepareTokenAsJwt(accessToken));
        } catch (MongoSecurityException e) {
            if (triggersRetry(e)) {
                authLock(connection, connectionDescription);
            } else {
                throw e;
            }
        }
    }

    private void authenticateOptimisticallyAsync(final InternalConnection connection,
            final ConnectionDescription connectionDescription, final String accessToken,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            authenticateUsingFunctionAsync(connection, connectionDescription, (challenge) -> prepareTokenAsJwt(accessToken), c);
        }).onErrorIf(e -> triggersRetry(e), c -> {
            authLockAsync(connection, connectionDescription, c);
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

    private void authenticateUsingFunctionAsync(final InternalConnection connection,
            final ConnectionDescription connectionDescription, final Function<byte[], byte[]> evaluateChallengeFunction,
            final SingleResultCallback<Void> callback) {
        this.evaluateChallengeFunction = evaluateChallengeFunction;
        super.authenticateAsync(connection, connectionDescription, callback);
    }

    private void authenticateUsingFunction(
            final InternalConnection connection,
            final ConnectionDescription connectionDescription,
            final Function<byte[], byte[]> evaluateChallengeFunction) {
        this.evaluateChallengeFunction = evaluateChallengeFunction;
        super.authenticate(connection, connectionDescription);
    }

    private void authLock(final InternalConnection connection, final ConnectionDescription description) {
        fallbackState = FallbackState.INITIAL;
        Locks.withLock(getMongoCredentialWithCache().getOidcLock(), () -> {
            while (true) {
                try {
                    authenticateUsingFunction(connection, description, (challenge) -> evaluate(challenge));
                    break;
                } catch (MongoSecurityException e) {
                    if (triggersRetry(e) && shouldRetryHandler()) {
                        continue;
                    }
                    throw e;
                }
            }
        });
    }

    private void authLockAsync(final InternalConnection connection, final ConnectionDescription description,
            final SingleResultCallback<Void> callback) {
        fallbackState = FallbackState.INITIAL;
        Locks.withLockAsync(getMongoCredentialWithCache().getOidcLock(),
                beginAsync().thenRunRetryingWhile(
                        c -> authenticateUsingFunctionAsync(connection, description, (challenge) -> evaluate(challenge), c),
                        e -> triggersRetry(e) && shouldRetryHandler()
                ), callback);
    }

    private byte[] evaluate(final byte[] challenge) {
        if (isAutomaticAuthentication()) {
            return prepareAwsTokenFromFileAsJwt();
        }

        OidcRequestCallback requestCallback = assertNotNull(getRequestCallback());
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
        String cachedAccessToken = getValidCachedAccessToken();
        String invalidConnectionAccessToken = connectionLastAccessToken;
        String cachedRefreshToken = cacheEntry.getRefreshToken();
        IdpInfo cachedIdpInfo = cacheEntry.getIdpInfo();

        if (cachedAccessToken != null) {
            boolean cachedTokenIsInvalid = cachedAccessToken.equals(invalidConnectionAccessToken);
            if (cachedTokenIsInvalid) {
                mongoCredentialWithCache.setOidcCacheEntry(cacheEntry.clearAccessToken());
                cachedAccessToken = null;
            }
        }
        OidcRefreshCallback refreshCallback = getRefreshCallback();
        if (cachedAccessToken != null) {
            fallbackState = FallbackState.PHASE_1_CACHED_TOKEN;
            return prepareTokenAsJwt(cachedAccessToken);
        } else if (refreshCallback != null && cachedRefreshToken != null) {
            assertNotNull(cachedIdpInfo);
            // Invoke Refresh Callback using cached Refresh Token
            validateAllowedHosts(getMongoCredential());
            fallbackState = FallbackState.PHASE_2_REFRESH_CALLBACK_TOKEN;
            IdpResponse result = refreshCallback.onRefresh(new OidcRefreshContextImpl(
                    cachedIdpInfo, cachedRefreshToken, CALLBACK_TIMEOUT));
            return populateCacheWithCallbackResultAndPrepareJwt(cachedIdpInfo, result);
        } else {
            // cache is empty

            /*
            A check for present idp info short-circuits phase-3a.

            If a challenge is present, it can only be a response to a
            "principal-request", so the challenge must be the resulting
            idp info. Such a request is made during speculative auth,
            though the source is unimportant, as long as we detect and
            use it here.

            Checking that the fallback state is not phase-3a ensures that
            this does not loop infinitely in the case of a bug.
             */
            boolean idpInfoNotPresent = challenge.length == 0;
            if (fallbackState != FallbackState.PHASE_3A_PRINCIPAL && idpInfoNotPresent) {
                fallbackState = FallbackState.PHASE_3A_PRINCIPAL;
                return prepareUsername(mongoCredentialWithCache.getCredential().getUserName());
            } else {
                IdpInfo idpInfo = toIdpInfo(challenge);
                validateAllowedHosts(getMongoCredential());
                IdpResponse result = requestCallback.onRequest(new OidcRequestContextImpl(idpInfo, CALLBACK_TIMEOUT));
                fallbackState = FallbackState.PHASE_3B_REQUEST_CALLBACK_TOKEN;
                return populateCacheWithCallbackResultAndPrepareJwt(idpInfo, result);
            }
        }
    }

    private boolean isAutomaticAuthentication() {
        return getRequestCallback() == null;
    }

    private boolean clientIsComplete() {
        return fallbackState != FallbackState.PHASE_3A_PRINCIPAL;
    }

    private boolean shouldRetryHandler() {
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
        if (fallbackState == FallbackState.PHASE_1_CACHED_TOKEN) {
            // a cached access token failed
            mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                    .clearAccessToken());
        } else if (fallbackState == FallbackState.PHASE_2_REFRESH_CALLBACK_TOKEN) {
            // a refresh token failed
            mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                    .clearAccessToken()
                    .clearRefreshToken());
        } else {
            // a clean-restart failed
            mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                    .clearAccessToken()
                    .clearRefreshToken());
            return false;
        }
        return true;
    }

    @Nullable
    private String getValidCachedAccessToken() {
        return getMongoCredentialWithCache()
                .getOidcCacheEntry()
                .getValidCachedAccessToken();
    }

    static final class OidcCacheEntry {
        @Nullable
        private final String accessToken;
        @Nullable
        private final Timeout accessTokenExpiry;
        @Nullable
        private final String refreshToken;
        @Nullable
        private final IdpInfo idpInfo;

        @Override
        public String toString() {
            return "OidcCacheEntry{"
                    + "\n accessToken#hashCode='" + Objects.hashCode(accessToken) + '\''
                    + ",\n accessTokenExpiry=" + accessTokenExpiry
                    + ",\n refreshToken='" + refreshToken + '\''
                    + ",\n idpInfo=" + idpInfo
                    + '}';
        }

        OidcCacheEntry(final IdpInfo idpInfo, final IdpResponse idpResponse) {
            Integer accessTokenExpiresInSeconds = idpResponse.getAccessTokenExpiresInSeconds();
            if (accessTokenExpiresInSeconds != null) {
                this.accessToken = idpResponse.getAccessToken();
                long accessTokenExpiryReservedSeconds = TimeUnit.MINUTES.toSeconds(5);
                this.accessTokenExpiry = Timeout.startNow(
                        Math.max(0, accessTokenExpiresInSeconds - accessTokenExpiryReservedSeconds),
                        TimeUnit.SECONDS);
            } else {
                this.accessToken = null;
                this.accessTokenExpiry = null;
            }
            String refreshToken = idpResponse.getRefreshToken();
            if (refreshToken != null) {
                this.refreshToken = refreshToken;
                this.idpInfo = idpInfo;
            } else {
                this.refreshToken = null;
                this.idpInfo = null;
            }
        }

        OidcCacheEntry() {
            this(null, null, null, null);
        }

        private OidcCacheEntry(@Nullable final String accessToken, @Nullable final Timeout accessTokenExpiry,
                @Nullable final String refreshToken, @Nullable final IdpInfo idpInfo) {
            this.accessToken = accessToken;
            this.accessTokenExpiry = accessTokenExpiry;
            this.refreshToken = refreshToken;
            this.idpInfo = idpInfo;
        }

        @Nullable
        String getValidCachedAccessToken() {
            if (accessToken == null || accessTokenExpiry == null || accessTokenExpiry.expired()) {
                return null;
            }
            return accessToken;
        }

        @Nullable
        String getRefreshToken() {
            return refreshToken;
        }

        @Nullable
        IdpInfo getIdpInfo() {
            return idpInfo;
        }

        OidcCacheEntry clearAccessToken() {
            return new OidcCacheEntry(
                    null,
                    null,
                    this.refreshToken,
                    this.idpInfo);
        }

        OidcCacheEntry clearRefreshToken() {
            return new OidcCacheEntry(
                    this.accessToken,
                    this.accessTokenExpiry,
                    null,
                    null);
        }
    }

    private final class OidcSaslClient extends SaslClientImpl {

        private OidcSaslClient(final MongoCredentialWithCache mongoCredentialWithCache) {
            super(mongoCredentialWithCache.getCredential());
        }

        @Override
        public byte[] evaluateChallenge(final byte[] challenge) {
            return assertNotNull(evaluateChallengeFunction).apply(challenge);
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

    private static byte[] prepareUsername(@Nullable final String username) {
        BsonDocument document = new BsonDocument();
        if (username != null) {
            document = document.append("n", new BsonString(username));
        }
        return toBson(document);
    }

    private byte[] populateCacheWithCallbackResultAndPrepareJwt(
            final IdpInfo serverInfo,
            @Nullable final IdpResponse idpResponse) {
        if (idpResponse == null) {
            throw new MongoConfigurationException("Result of callback must not be null");
        }
        OidcCacheEntry newEntry = new OidcCacheEntry(serverInfo, idpResponse);
        getMongoCredentialWithCache().setOidcCacheEntry(newEntry);
        return prepareTokenAsJwt(idpResponse.getAccessToken());
    }

    private static IdpInfo toIdpInfo(final byte[] challenge) {
        BsonDocument c = new RawBsonDocument(challenge);
        String issuer = c.getString("issuer").getValue();
        String clientId = c.getString("clientId").getValue();
        return new IdpInfoImpl(
                issuer,
                clientId,
                getStringArray(c, "requestScopes"));
    }

    private void validateAllowedHosts(final MongoCredential credential) {
        List<String> allowedHosts = assertNotNull(credential.getMechanismProperty(ALLOWED_HOSTS_KEY, DEFAULT_ALLOWED_HOSTS));
        String host = assertNotNull(serverAddress).getHost();
        boolean permitted = allowedHosts.stream().anyMatch(allowedHost -> {
            if (allowedHost.startsWith("*.")) {
                String ending = allowedHost.substring(1);
                return host.endsWith(ending);
            } else if (allowedHost.contains("*")) {
                throw new IllegalArgumentException(
                        "Allowed host " + allowedHost + " contains invalid wildcard");
            } else {
                return host.equals(allowedHost);
            }
        });
        if (!permitted) {
            throw new MongoSecurityException(
                    credential, "Host not permitted by " + ALLOWED_HOSTS_KEY + ": " + host);
        }
    }

    @Nullable
    private static List<String> getStringArray(final BsonDocument document, final String key) {
        if (!document.isArray(key)) {
            return null;
        }
        return document.getArray(key).stream()
                // ignore non-string values from server, rather than error
                .filter(v -> v.isString())
                .map(v -> v.asString().getValue())
                .collect(Collectors.toList());
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
            Object requestCallback = credential.getMechanismProperty(REQUEST_TOKEN_CALLBACK_KEY, null);
            Object refreshCallback = credential.getMechanismProperty(REFRESH_TOKEN_CALLBACK_KEY, null);
            if (providerName == null) {
                // callback
                if (requestCallback == null) {
                    throw new IllegalArgumentException("Either " + PROVIDER_NAME_KEY + " or "
                            + REQUEST_TOKEN_CALLBACK_KEY + " must be specified");
                }
            } else {
                // automatic
                if (userName != null) {
                    throw new IllegalArgumentException("user name must not be specified when " + PROVIDER_NAME_KEY + " is specified");
                }
                if (requestCallback != null) {
                    throw new IllegalArgumentException(REQUEST_TOKEN_CALLBACK_KEY + " must not be specified when " + PROVIDER_NAME_KEY + " is specified");
                }
                if (refreshCallback != null) {
                    throw new IllegalArgumentException(REFRESH_TOKEN_CALLBACK_KEY + " must not be specified when " + PROVIDER_NAME_KEY + " is specified");
                }
            }
        }
    }


    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static class OidcRequestContextImpl implements OidcRequestContext {
        private final IdpInfo idpInfo;
        private final Duration timeout;

        OidcRequestContextImpl(final IdpInfo idpInfo, final Duration timeout) {
            this.idpInfo = assertNotNull(idpInfo);
            this.timeout = assertNotNull(timeout);
        }

        @Override
        public IdpInfo getIdpInfo() {
            return idpInfo;
        }

        @Override
        public Duration getTimeout() {
            return timeout;
        }
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static final class OidcRefreshContextImpl extends OidcRequestContextImpl
            implements OidcRefreshContext {
        private final String refreshToken;

        OidcRefreshContextImpl(final IdpInfo idpInfo, final String refreshToken,
                final Duration timeout) {
            super(idpInfo, timeout);
            this.refreshToken = assertNotNull(refreshToken);
        }

        @Override
        public String getRefreshToken() {
            return refreshToken;
        }
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static final class IdpInfoImpl implements IdpInfo {
        private final String issuer;
        private final String clientId;

        private final List<String> requestScopes;

        IdpInfoImpl(final String issuer, final String clientId, @Nullable final List<String> requestScopes) {
            this.issuer = assertNotNull(issuer);
            this.clientId = assertNotNull(clientId);
            this.requestScopes = requestScopes == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(requestScopes);
        }

        @Override
        public String getIssuer() {
            return issuer;
        }

        @Override
        public String getClientId() {
            return clientId;
        }

        @Override
        public List<String> getRequestScopes() {
            return requestScopes;
        }
    }

    /**
     * Represents what was sent in the last request to the MongoDB server.
     */
    private enum FallbackState {
        INITIAL,
        PHASE_1_CACHED_TOKEN,
        PHASE_2_REFRESH_CALLBACK_TOKEN,
        PHASE_3A_PRINCIPAL,
        PHASE_3B_REQUEST_CALLBACK_TOKEN
    }
}
