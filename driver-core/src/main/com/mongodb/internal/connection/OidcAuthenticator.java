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
import com.mongodb.internal.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.AuthenticationMechanism.MONGODB_OIDC;
import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.MongoCredential.DEFAULT_ALLOWED_HOSTS;
import static com.mongodb.MongoCredential.OidcRefreshContext;
import static com.mongodb.MongoCredential.OidcRequestContext;
import static com.mongodb.MongoCredential.PROVIDER_NAME_KEY;
import static com.mongodb.MongoCredential.REFRESH_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.REQUEST_TOKEN_CALLBACK_KEY;
import static com.mongodb.MongoCredential.OidcRefreshCallback;
import static com.mongodb.MongoCredential.OidcRequestCallback;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class OidcAuthenticator extends SaslAuthenticator {

    private static final List<String> SUPPORTED_PROVIDERS = Arrays.asList("aws");

    private static final Duration CALLBACK_TIMEOUT = Duration.ofMinutes(5);

    private static final String AWS_WEB_IDENTITY_TOKEN_FILE = "AWS_WEB_IDENTITY_TOKEN_FILE";

    private ServerAddress serverAddress;

    private String connectionLastAccessToken;

    private FallbackState fallbackState = FallbackState.INITIAL;

    private BsonDocument speculativeAuthenticateResponse;

    private Function<byte[], byte[]> evaluateChallengeFunction;

    public OidcAuthenticator(final MongoCredentialWithCache credential,
            final ClusterConnectionMode clusterConnectionMode, @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);

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
            OidcRequestCallback requestCallback = getRequestCallback();
            if (requestCallback == null) {
                return wrapInSpeculative(prepareAwsTokenFromFile());
            }
            String cachedAccessToken = getValidCachedAccessToken();
            MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
            if (cachedAccessToken != null) {
                connectionLastAccessToken = cachedAccessToken;
                fallbackState = FallbackState.PHASE_1_CACHED_TOKEN;
                return wrapInSpeculative(prepareTokenAsJwt(cachedAccessToken));
            } else if (mongoCredentialWithCache.getOidcCacheEntry().idpInfo == null) {
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
            this.fallbackState = FallbackState.INITIAL;
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
        fallbackState = FallbackState.INITIAL;
        authLock(connection, connection.getDescription());
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        // method must only be called during original handshake; fail otherwise
        assertFalse(connection.opened());
        // this method "wraps" the default authentication method in custom OIDC retry logic
        String accessToken = getValidCachedAccessToken();
        if (accessToken != null) {
            try {
                authenticateUsing(connection, connectionDescription, (bytes) -> prepareTokenAsJwt(accessToken));
            } catch (MongoSecurityException e) {
                if (triggersRetry(e)) { // TODO-OIDC-x unclear how to provide test coverage for this
                    authLock(connection, connectionDescription);
                } else {
                    throw e;
                }
            }
        } else {
            authLock(connection, connectionDescription);
        }
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


    private void authenticateUsing(
            final InternalConnection connection,
            final ConnectionDescription connectionDescription,
            final Function<byte[], byte[]> evaluateChallengeFunction) {
        this.evaluateChallengeFunction = evaluateChallengeFunction;
        super.authenticate(connection, connectionDescription);
    }

    private void authLock(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        mongoCredentialWithCache.withOidcLock(() -> {
            while (true) {
                try {
                    authenticateUsing(connection, connectionDescription, (challenge) -> evaluate(challenge));
                    break;
                } catch (MongoSecurityException e) {
                    OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
                    if (triggersRetry(e)) {
                        prepareRetry(e, cacheEntry);
                    } else {
                        throw e;
                    }
                }
            }
            return null;
        });
    }

    private byte[] evaluate(final byte[] challenge) {
        OidcRequestCallback requestCallback = getRequestCallback();
        if (requestCallback == null) {
            return prepareAwsTokenFromFile();
        }

        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
        OidcCacheEntry cacheEntry = mongoCredentialWithCache.getOidcCacheEntry();
        String cachedAccessToken = getValidCachedAccessToken();
        String invalidConnectionAccessToken = connectionLastAccessToken;
        String cachedRefreshToken = cacheEntry.refreshToken;
        IdpInfo cachedIdpInfo = cacheEntry.idpInfo;

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
        } else if (refreshCallback != null && cachedRefreshToken != null && cachedIdpInfo != null) {
            fallbackState = FallbackState.PHASE_2_REFRESH_CALLBACK_TOKEN;
            // Invoke Refresh Callback using cached Refresh Token
            validateAllowedHosts(getMongoCredential());
            IdpResponse result = refreshCallback.onRefresh(new OidcRefreshContextImpl(
                    cachedIdpInfo, cachedRefreshToken, CALLBACK_TIMEOUT));
            return handleCallbackResult(cachedIdpInfo, result);
        } else {
            // cache is empty
            if (fallbackState != FallbackState.PHASE_3A_PRINCIPAL && challenge.length == 0) {
                fallbackState = FallbackState.PHASE_3A_PRINCIPAL;
                return prepareUsername(mongoCredentialWithCache.getCredential().getUserName());
            } else {
                fallbackState = FallbackState.PHASE_3B_REQUEST_CALLBACK_TOKEN;
                IdpInfo idpInfo = toIdpInfo(challenge);
                IdpResponse result = invokeRequestCallback(requestCallback, idpInfo);
                return handleCallbackResult(idpInfo, result);
            }
        }
    }

    private boolean clientIsComplete() {
        return fallbackState != FallbackState.PHASE_3A_PRINCIPAL;
    }

    private void prepareRetry(final MongoException e, final OidcCacheEntry cacheEntry) {
        MongoCredentialWithCache mongoCredentialWithCache = getMongoCredentialWithCache();
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
            throw e;
        }
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
                    + "\n accessToken#hashCode='" + (accessToken == null ? null : accessToken.hashCode()) + '\''
                    + ",\n expiry=" + accessTokenExpiry
                    + ",\n refreshToken='" + refreshToken + '\''
                    + ",\n idpInfo=" + idpInfo
                    + '}';
        }

        OidcCacheEntry(@Nullable final IdpInfo idpInfo, final IdpResponse idpResponse) {
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
        public String getValidCachedAccessToken() {
            if (accessToken == null || accessTokenExpiry == null || accessTokenExpiry.expired()) {
                return null;
            }
            return accessToken;
        }

        public OidcCacheEntry clearAccessToken() {
            return new OidcCacheEntry(
                    null,
                    null,
                    this.refreshToken,
                    this.idpInfo);
        }

        public OidcCacheEntry clearRefreshToken() {
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
            return evaluateChallengeInternal(challenge);
        }

        @Override
        public boolean isComplete() {
            return clientIsComplete();
        }

        public byte[] evaluateChallengeInternal(final byte[] challenge) {
            return evaluateChallengeFunction.apply(challenge);
        }
    }

    private static byte[] prepareAwsTokenFromFile() {
        return toBson(new BsonDocument()
                .append("jwt", new BsonString(readAwsTokenFromFile())));
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

    private byte[] prepareUsername(@Nullable final String username) {
        BsonDocument document = new BsonDocument();
        if (username != null) {
            document = document.append("n", new BsonString(username));
        }
        return toBson(document);
    }

    private byte[] handleCallbackResult(
            final IdpInfo serverInfo,
            @Nullable final IdpResponse tokens) {
        if (tokens == null) {
            throw new MongoConfigurationException("Result of callback must not be null");
        }
        OidcCacheEntry newEntry = new OidcCacheEntry(serverInfo, tokens);
        getMongoCredentialWithCache().setOidcCacheEntry(newEntry);
        return prepareTokenAsJwt(tokens.getAccessToken());
    }

    private IdpInfo toIdpInfo(final byte[] challenge) {
        BsonDocument c = new RawBsonDocument(challenge);
        String issuer = c.getString("issuer").getValue();
        String clientId = c.getString("clientId").getValue();
        return new IdpInfoImpl(
                issuer,
                clientId,
                getStringArray(c, "requestScopes"));
    }

    private IdpResponse invokeRequestCallback(final OidcRequestCallback requestCallback,
            final IdpInfo serverInfo) {
        validateAllowedHosts(getMongoCredential());
        return requestCallback.onRequest(new OidcRequestContextImpl(serverInfo, CALLBACK_TIMEOUT));
    }

    private void validateAllowedHosts(final MongoCredential credential) {
        List<String> allowedHosts = assertNotNull(credential.getMechanismProperty(ALLOWED_HOSTS_KEY, DEFAULT_ALLOWED_HOSTS));
        String host = serverAddress.getHost();
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
    private List<String> getStringArray(final BsonDocument document, final String key) {
        if (!document.containsKey(key) || document.isArray(key)) {
            return null;
        }
        List<String> result = document.getArray(key).getValues().stream()
                // ignore non-string values from server, rather than error
                .filter(v -> v.isString())
                .map(v -> v.asString().getValue())
                .collect(Collectors.toList());
        return Collections.unmodifiableList(result);
    }

    private byte[] prepareTokenAsJwt(final String accessToken) {
        connectionLastAccessToken = accessToken;
        return toBson(new BsonDocument().append("jwt", new BsonString(accessToken)));
    }

    private static byte[] toBson(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        byte[] bytes = new byte[buffer.size()];
        System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, buffer.getSize());
        return bytes;
    }

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

        public static void validateBeforeUse(final MongoCredential credential) {
            AuthenticationMechanism mechanism = credential.getAuthenticationMechanism();
            String userName = credential.getUserName();

            if (mechanism == AuthenticationMechanism.MONGODB_OIDC) {
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
    }


    public static class OidcRequestContextImpl implements OidcRequestContext {
        private final IdpInfo idpInfo;
        private final Duration timeout;

        public OidcRequestContextImpl(final IdpInfo idpInfo, final Duration timeout) {
            notNull("idpInfo", idpInfo);
            notNull("timeout", timeout);
            this.idpInfo = idpInfo;
            this.timeout = timeout;
        }

        public IdpInfo getIdpInfo() {
            return idpInfo;
        }

        public Duration getTimeout() {
            return timeout;
        }
    }

    public static final class OidcRefreshContextImpl extends OidcRequestContextImpl
            implements OidcRefreshContext {
        private final String refreshToken;

        public OidcRefreshContextImpl(final IdpInfo idpInfo, final String refreshToken,
                final Duration timeout) {
            super(idpInfo, timeout);
            notNull("refreshToken", refreshToken);
            this.refreshToken = refreshToken;
        }

        public String getRefreshToken() {
            return refreshToken;
        }
    }

    public static final class IdpInfoImpl implements IdpInfo {
        private final String issuer;
        private final String clientId;

        private final List<String> requestScopes;

        public IdpInfoImpl(final String issuer, final String clientId, @Nullable final List<String> requestScopes) {
            this.issuer = issuer;
            this.clientId = clientId;
            this.requestScopes = requestScopes == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(requestScopes);
        }

        public String getIssuer() {
            return issuer;
        }

        public String getClientId() {
            return clientId;
        }

        public List<String> getRequestScopes() {
            return requestScopes;
        }
    }

    /**
     * Represents what was sent in the last request to the MongoDB server.
     */
    enum FallbackState {
        INITIAL,
        PHASE_1_CACHED_TOKEN,
        PHASE_2_REFRESH_CALLBACK_TOKEN,
        PHASE_3A_PRINCIPAL,
        PHASE_3B_REQUEST_CALLBACK_TOKEN
    }
}
