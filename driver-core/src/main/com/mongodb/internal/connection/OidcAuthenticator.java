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
import com.mongodb.MongoCredential.OidcCallbackResult;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.Locks;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.authentication.AzureCredentialHelper;
import com.mongodb.internal.authentication.CredentialInfo;
import com.mongodb.internal.authentication.GcpCredentialHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

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
import java.util.stream.Collectors;

import static com.mongodb.AuthenticationMechanism.MONGODB_OIDC;
import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.DEFAULT_ALLOWED_HOSTS;
import static com.mongodb.MongoCredential.ENVIRONMENT_KEY;
import static com.mongodb.MongoCredential.IdpInfo;
import static com.mongodb.MongoCredential.OIDC_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OIDC_HUMAN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OidcCallback;
import static com.mongodb.MongoCredential.OidcCallbackContext;
import static com.mongodb.MongoCredential.TOKEN_RESOURCE_KEY;
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

    private static final String TEST_ENVIRONMENT = "test";
    private static final String AZURE_ENVIRONMENT = "azure";
    private static final String GCP_ENVIRONMENT = "gcp";
    private static final List<String> IMPLEMENTED_ENVIRONMENTS = Arrays.asList(
            AZURE_ENVIRONMENT, GCP_ENVIRONMENT, TEST_ENVIRONMENT);
    private static final List<String> USER_SUPPORTED_ENVIRONMENTS = Arrays.asList(
            AZURE_ENVIRONMENT, GCP_ENVIRONMENT);
    private static final List<String> REQUIRES_TOKEN_RESOURCE = Arrays.asList(
            AZURE_ENVIRONMENT, GCP_ENVIRONMENT);
    private static final List<String> ALLOWS_USERNAME = Arrays.asList(
            AZURE_ENVIRONMENT);

    private static final Duration CALLBACK_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration HUMAN_CALLBACK_TIMEOUT = Duration.ofMinutes(5);

    public static final String OIDC_TOKEN_FILE = "OIDC_TOKEN_FILE";

    private static final int CALLBACK_API_VERSION_NUMBER = 1;

    @Nullable
    private ServerAddress serverAddress;

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

    private Duration getCallbackTimeout() {
        return isHumanCallback() ? HUMAN_CALLBACK_TIMEOUT : CALLBACK_TIMEOUT;
    }

    @Override
    public String getMechanismName() {
        return MONGODB_OIDC.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        this.serverAddress = assertNotNull(serverAddress);
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        return new OidcSaslClient(mongoCredentialWithCache);
    }

    @Override
    @Nullable
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        try {
            String cachedAccessToken = getMongoCredentialWithCache()
                    .getOidcCacheEntry()
                    .getCachedAccessToken();
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

    private boolean isHumanCallback() {
        // built-in providers (aws, azure...) are considered machine callbacks
        return getOidcCallbackMechanismProperty(OIDC_HUMAN_CALLBACK_KEY) != null;
    }

    @Nullable
    private OidcCallback getOidcCallbackMechanismProperty(final String key) {
        return getMongoCredentialWithCache()
                .getCredential()
                .getMechanismProperty(key, null);
    }

    private OidcCallback getRequestCallback() {
        String environment = getMongoCredential().getMechanismProperty(ENVIRONMENT_KEY, null);
        OidcCallback machine;
        if (TEST_ENVIRONMENT.equals(environment)) {
            machine = getTestCallback();
        } else if (AZURE_ENVIRONMENT.equals(environment)) {
            machine = getAzureCallback(getMongoCredential());
        } else if (GCP_ENVIRONMENT.equals(environment)) {
            machine = getGcpCallback(getMongoCredential());
        } else {
            machine = getOidcCallbackMechanismProperty(OIDC_CALLBACK_KEY);
        }
        OidcCallback human = getOidcCallbackMechanismProperty(OIDC_HUMAN_CALLBACK_KEY);
        return machine != null ? machine : assertNotNull(human);
    }

    private static OidcCallback getTestCallback() {
        return (context) -> {
            String accessToken = readTokenFromFile();
            return new OidcCallbackResult(accessToken);
        };
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static OidcCallback getAzureCallback(final MongoCredential credential) {
        return (context) -> {
            String resource = assertNotNull(credential.getMechanismProperty(TOKEN_RESOURCE_KEY, null));
            String clientId = credential.getUserName();
            CredentialInfo response = AzureCredentialHelper.fetchAzureCredentialInfo(resource, clientId);
            return new OidcCallbackResult(response.getAccessToken(), response.getExpiresIn());
        };
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static OidcCallback getGcpCallback(final MongoCredential credential) {
        return (context) -> {
            String resource = assertNotNull(credential.getMechanismProperty(TOKEN_RESOURCE_KEY, null));
            CredentialInfo response = GcpCredentialHelper.fetchGcpCredentialInfo(resource);
            return new OidcCallbackResult(response.getAccessToken(), response.getExpiresIn());
        };
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
                } catch (Exception e) {
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
        byte[][] jwt = new byte[1][];
        Locks.withInterruptibleLock(getMongoCredentialWithCache().getOidcLock(), () -> {
            OidcCacheEntry oidcCacheEntry = getMongoCredentialWithCache().getOidcCacheEntry();
            String cachedRefreshToken = oidcCacheEntry.getRefreshToken();
            IdpInfo cachedIdpInfo = oidcCacheEntry.getIdpInfo();
            String cachedAccessToken = validatedCachedAccessToken();
            OidcCallback requestCallback = getRequestCallback();
            boolean isHuman = isHumanCallback();
            String userName = getMongoCredentialWithCache().getCredential().getUserName();

            if (cachedAccessToken != null) {
                fallbackState = FallbackState.PHASE_1_CACHED_TOKEN;
                jwt[0] = prepareTokenAsJwt(cachedAccessToken);
            } else if (cachedRefreshToken != null) {
                // cached refresh token is only set when isHuman
                // original IDP info will be present, if refresh token present
                assertNotNull(cachedIdpInfo);
                // Invoke Callback using cached Refresh Token
                fallbackState = FallbackState.PHASE_2_REFRESH_CALLBACK_TOKEN;
                OidcCallbackResult result = requestCallback.onRequest(new OidcCallbackContextImpl(
                        getCallbackTimeout(), cachedIdpInfo, cachedRefreshToken, userName));
                jwt[0] = populateCacheWithCallbackResultAndPrepareJwt(cachedIdpInfo, result);
            } else {
                // cache is empty

                if (!isHuman) {
                    // no principal request
                    fallbackState = FallbackState.PHASE_3B_CALLBACK_TOKEN;
                    OidcCallbackResult result = requestCallback.onRequest(new OidcCallbackContextImpl(
                            getCallbackTimeout(), userName));
                    jwt[0] = populateCacheWithCallbackResultAndPrepareJwt(null, result);
                    if (result.getRefreshToken() != null) {
                        throw new MongoConfigurationException(
                                "Refresh token must only be provided in human workflow");
                    }
                } else {
                    /*
                    A check for present idp info short-circuits phase-3a.
                    If a challenge is present, it can only be a response to a
                    "principal-request", so the challenge must be the resulting
                    idp info. Such a request is made during speculative auth,
                    though the source is unimportant, as long as we detect and
                    use it here.
                     */
                    boolean idpInfoNotPresent = challenge.length == 0;
                    /*
                    Checking that the fallback state is not phase-3a ensures that
                    this does not loop infinitely in the case of a bug.
                     */
                    boolean alreadyTriedPrincipal = fallbackState == FallbackState.PHASE_3A_PRINCIPAL;
                    if (!alreadyTriedPrincipal && idpInfoNotPresent) {
                        // request for idp info, only in the human workflow
                        fallbackState = FallbackState.PHASE_3A_PRINCIPAL;
                        jwt[0] = prepareUsername(userName);
                    } else {
                        IdpInfo idpInfo = toIdpInfo(challenge);
                        // there is no cached refresh token
                        fallbackState = FallbackState.PHASE_3B_CALLBACK_TOKEN;
                        OidcCallbackResult result = requestCallback.onRequest(new OidcCallbackContextImpl(
                                getCallbackTimeout(), idpInfo, null, userName));
                        jwt[0] = populateCacheWithCallbackResultAndPrepareJwt(idpInfo, result);
                    }
                }
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
        String cachedAccessToken = cacheEntry.getCachedAccessToken();
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

    private boolean clientIsComplete() {
        return fallbackState != FallbackState.PHASE_3A_PRINCIPAL;
    }

    private boolean shouldRetryHandler() {
        boolean[] result = new boolean[1];
        Locks.withInterruptibleLock(getMongoCredentialWithCache().getOidcLock(), () -> {
            MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
            OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
            if (fallbackState == FallbackState.PHASE_1_CACHED_TOKEN) {
                // a cached access token failed
                mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                        .clearAccessToken());
                result[0] = true;
            } else if (fallbackState == FallbackState.PHASE_2_REFRESH_CALLBACK_TOKEN) {
                // a refresh token failed
                mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                        .clearAccessToken()
                        .clearRefreshToken());
                result[0] = true;
            } else {
                // a clean-restart failed
                mongoCredentialWithCache.setOidcCacheEntry(cacheEntry
                        .clearAccessToken()
                        .clearRefreshToken());
                result[0] = false;
            }
        });
        return result[0];
    }

    static final class OidcCacheEntry {
        @Nullable
        private final String accessToken;
        @Nullable
        private final String refreshToken;
        @Nullable
        private final IdpInfo idpInfo;

        @Override
        public String toString() {
            return "OidcCacheEntry{"
                    + "\n accessToken=[omitted]"
                    + ",\n refreshToken=[omitted]"
                    + ",\n idpInfo=" + idpInfo
                    + '}';
        }

        OidcCacheEntry() {
            this(null, null, null);
        }

        private OidcCacheEntry(@Nullable final String accessToken,
                @Nullable final String refreshToken, @Nullable final IdpInfo idpInfo) {
            this.accessToken = accessToken;
            this.refreshToken = refreshToken;
            this.idpInfo = idpInfo;
        }

        @Nullable
        String getCachedAccessToken() {
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
                    this.refreshToken,
                    this.idpInfo);
        }

        OidcCacheEntry clearRefreshToken() {
            return new OidcCacheEntry(
                    this.accessToken,
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
            return evaluate(challenge);
        }

        @Override
        public boolean isComplete() {
            return clientIsComplete();
        }

    }

    private static String readTokenFromFile() {
        String path = System.getenv(OIDC_TOKEN_FILE);
        if (path == null) {
            throw new MongoClientException(
                    format("Environment variable must be specified: %s", OIDC_TOKEN_FILE));
        }
        try {
            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new MongoClientException(format(
                    "Could not read file specified by environment variable: %s at path: %s",
                    OIDC_TOKEN_FILE, path), e);
        }
    }

    private byte[] populateCacheWithCallbackResultAndPrepareJwt(
            @Nullable final IdpInfo serverInfo,
            @Nullable final OidcCallbackResult oidcCallbackResult) {
        if (oidcCallbackResult == null) {
            throw new MongoConfigurationException("Result of callback must not be null");
        }
        OidcCacheEntry newEntry = new OidcCacheEntry(oidcCallbackResult.getAccessToken(),
                oidcCallbackResult.getRefreshToken(), serverInfo);
        getMongoCredentialWithCache().setOidcCacheEntry(newEntry);
        return prepareTokenAsJwt(oidcCallbackResult.getAccessToken());
    }

    private static byte[] prepareUsername(@Nullable final String username) {
        BsonDocument document = new BsonDocument();
        if (username != null) {
            document = document.append("n", new BsonString(username));
        }
        return toBson(document);
    }

    private IdpInfo toIdpInfo(final byte[] challenge) {
        // validate here to prevent creating IdpInfo for unauthorized hosts
        validateAllowedHosts(getMongoCredential());
        BsonDocument c = new RawBsonDocument(challenge);
        String issuer = c.getString("issuer").getValue();
        String clientId = !c.containsKey("clientId") ? null : c.getString("clientId").getValue();
        return new IdpInfoImpl(
                issuer,
                clientId,
                getStringArray(c, "requestScopes"));
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
                    credential, "Host " + host + " not permitted by " + ALLOWED_HOSTS_KEY
                    + ", values:  " + allowedHosts);
        }
    }

    private byte[] prepareTokenAsJwt(final String accessToken) {
        connectionLastAccessToken = accessToken;
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

            Object environmentName = mechanismProperties.get(ENVIRONMENT_KEY.toLowerCase());
            if (environmentName != null) {
                if (!(environmentName instanceof String) || !IMPLEMENTED_ENVIRONMENTS.contains(environmentName)) {
                    throw new IllegalArgumentException(ENVIRONMENT_KEY + " must be one of: " + USER_SUPPORTED_ENVIRONMENTS);
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
            Object environmentName = credential.getMechanismProperty(ENVIRONMENT_KEY, null);
            Object machineCallback = credential.getMechanismProperty(OIDC_CALLBACK_KEY, null);
            Object humanCallback = credential.getMechanismProperty(OIDC_HUMAN_CALLBACK_KEY, null);
            boolean allowedHostsIsSet = credential.getMechanismProperty(ALLOWED_HOSTS_KEY, null) != null;
            if (humanCallback == null && allowedHostsIsSet) {
                throw new IllegalArgumentException(ALLOWED_HOSTS_KEY + " must be specified only when "
                        + OIDC_HUMAN_CALLBACK_KEY + " is specified");
            }
            if (environmentName == null) {
                // callback
                if (machineCallback == null && humanCallback == null) {
                    throw new IllegalArgumentException("Either " + ENVIRONMENT_KEY
                            + " or " + OIDC_CALLBACK_KEY
                            + " or " + OIDC_HUMAN_CALLBACK_KEY
                            + " must be specified");
                }
                if (machineCallback != null && humanCallback != null) {
                    throw new IllegalArgumentException("Both " + OIDC_CALLBACK_KEY
                            + " and " + OIDC_HUMAN_CALLBACK_KEY
                            + " must not be specified");
                }
            } else {
                if (!(environmentName instanceof String)) {
                    throw new IllegalArgumentException(ENVIRONMENT_KEY + " must be a String");
                }
                if (userName != null && !ALLOWS_USERNAME.contains(environmentName)) {
                    throw new IllegalArgumentException("user name must not be specified when " + ENVIRONMENT_KEY + " is specified");
                }
                if (machineCallback != null) {
                    throw new IllegalArgumentException(OIDC_CALLBACK_KEY + " must not be specified when " + ENVIRONMENT_KEY + " is specified");
                }
                if (humanCallback != null) {
                    throw new IllegalArgumentException(OIDC_HUMAN_CALLBACK_KEY + " must not be specified when " + ENVIRONMENT_KEY + " is specified");
                }
                String tokenResource = credential.getMechanismProperty(TOKEN_RESOURCE_KEY, null);
                boolean hasTokenResourceProperty = tokenResource != null;
                boolean tokenResourceSupported = REQUIRES_TOKEN_RESOURCE.contains(environmentName);
                if (hasTokenResourceProperty != tokenResourceSupported) {
                    throw new IllegalArgumentException(TOKEN_RESOURCE_KEY
                            + " must be provided if and only if " + ENVIRONMENT_KEY
                            + " " + environmentName  + " "
                            + " is one of: " + REQUIRES_TOKEN_RESOURCE
                            + ". " + TOKEN_RESOURCE_KEY + ": " + tokenResource);
                }
            }
        }
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static class OidcCallbackContextImpl implements OidcCallbackContext {
        private final Duration timeout;
        @Nullable
        private final IdpInfo idpInfo;
        @Nullable
        private final String refreshToken;
        @Nullable
        private final String userName;

        OidcCallbackContextImpl(final Duration timeout, @Nullable final String userName) {
            this.timeout = assertNotNull(timeout);
            this.idpInfo = null;
            this.refreshToken = null;
            this.userName = userName;
        }

        OidcCallbackContextImpl(final Duration timeout, final IdpInfo idpInfo,
                @Nullable final String refreshToken, @Nullable final String userName) {
            this.timeout = assertNotNull(timeout);
            this.idpInfo = assertNotNull(idpInfo);
            this.refreshToken = refreshToken;
            this.userName = userName;
        }

        @Override
        @Nullable
        public IdpInfo getIdpInfo() {
            return idpInfo;
        }

        @Override
        public Duration getTimeout() {
            return timeout;
        }

        @Override
        public int getVersion() {
            return CALLBACK_API_VERSION_NUMBER;
        }

        @Override
        @Nullable
        public String getRefreshToken() {
            return refreshToken;
        }

        @Override
        @Nullable
        public String getUserName() {
            return userName;
        }
    }

    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    static final class IdpInfoImpl implements IdpInfo {
        private final String issuer;
        @Nullable
        private final String clientId;
        private final List<String> requestScopes;

        IdpInfoImpl(final String issuer, @Nullable final String clientId, @Nullable final List<String> requestScopes) {
            this.issuer = assertNotNull(issuer);
            this.clientId = clientId;
            this.requestScopes = requestScopes == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(requestScopes);
        }

        @Override
        public String getIssuer() {
            return issuer;
        }

        @Override
        @Nullable
        public String getClientId() {
            return clientId;
        }

        @Override
        public List<String> getRequestScopes() {
            return requestScopes;
        }
    }

    /**
     * What was sent in the last request by this connection to the server.
     */
    private enum FallbackState {
        INITIAL,
        PHASE_1_CACHED_TOKEN,
        PHASE_2_REFRESH_CALLBACK_TOKEN,
        PHASE_3A_PRINCIPAL,
        PHASE_3B_CALLBACK_TOKEN
    }
}
