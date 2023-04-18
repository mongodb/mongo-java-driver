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

import com.mongodb.LoggerSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;

class InternalStreamConnectionFactory implements InternalConnectionFactory {
    private final ClusterConnectionMode clusterConnectionMode;
    private final boolean isMonitoringConnection;
    private final StreamFactory streamFactory;
    private final BsonDocument clientMetadataDocument;
    private final List<MongoCompressor> compressorList;
    private final LoggerSettings loggerSettings;
    private final CommandListener commandListener;
    @Nullable
    private final ServerApi serverApi;
    private final InetAddressResolver inetAddressResolver;
    private final MongoCredentialWithCache credential;

    InternalStreamConnectionFactory(final ClusterConnectionMode clusterConnectionMode,
            final StreamFactory streamFactory,
            @Nullable final MongoCredentialWithCache credential,
            @Nullable final String applicationName, @Nullable final MongoDriverInformation mongoDriverInformation,
            final List<MongoCompressor> compressorList,
            final LoggerSettings loggerSettings, @Nullable final CommandListener commandListener, @Nullable final ServerApi serverApi,
            @Nullable final InetAddressResolver inetAddressResolver) {
        this(clusterConnectionMode, false, streamFactory, credential, applicationName, mongoDriverInformation, compressorList,
                loggerSettings, commandListener, serverApi, inetAddressResolver);
    }

    InternalStreamConnectionFactory(final ClusterConnectionMode clusterConnectionMode, final boolean isMonitoringConnection,
            final StreamFactory streamFactory,
            @Nullable final MongoCredentialWithCache credential,
            @Nullable final String applicationName, @Nullable final MongoDriverInformation mongoDriverInformation,
            final List<MongoCompressor> compressorList,
            final LoggerSettings loggerSettings, @Nullable final CommandListener commandListener, @Nullable final ServerApi serverApi,
            @Nullable final InetAddressResolver inetAddressResolver) {
        this.clusterConnectionMode = clusterConnectionMode;
        this.isMonitoringConnection = isMonitoringConnection;
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.compressorList = notNull("compressorList", compressorList);
        this.loggerSettings = loggerSettings;
        this.commandListener = commandListener;
        this.serverApi = serverApi;
        this.inetAddressResolver = inetAddressResolver;
        this.clientMetadataDocument = createClientMetadataDocument(applicationName, mongoDriverInformation);
        this.credential = credential;
    }

    @Override
    public InternalConnection create(final ServerId serverId, final ConnectionGenerationSupplier connectionGenerationSupplier) {
        Authenticator authenticator = credential == null ? null : createAuthenticator(credential);
        return new InternalStreamConnection(clusterConnectionMode, isMonitoringConnection, serverId, connectionGenerationSupplier,
                streamFactory, compressorList, loggerSettings, commandListener,
                new InternalStreamConnectionInitializer(clusterConnectionMode, authenticator, clientMetadataDocument, compressorList,
                        serverApi), inetAddressResolver);
    }

    private Authenticator createAuthenticator(final MongoCredentialWithCache credential) {
        if (credential.getAuthenticationMechanism() == null) {
            return new DefaultAuthenticator(credential, clusterConnectionMode, serverApi);
        }

        switch (assertNotNull(credential.getAuthenticationMechanism())) {
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, clusterConnectionMode, serverApi);
            case PLAIN:
                return new PlainAuthenticator(credential, clusterConnectionMode, serverApi);
            case MONGODB_X509:
                return new X509Authenticator(credential, clusterConnectionMode, serverApi);
            case SCRAM_SHA_1:
            case SCRAM_SHA_256:
                return new ScramShaAuthenticator(credential, clusterConnectionMode, serverApi);
            case MONGODB_AWS:
                return new AwsAuthenticator(credential, clusterConnectionMode, serverApi);
            default:
                throw new IllegalArgumentException("Unsupported authentication mechanism " + credential.getAuthenticationMechanism());
        }
    }
}
