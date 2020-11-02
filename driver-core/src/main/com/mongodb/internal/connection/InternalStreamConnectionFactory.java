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

import com.mongodb.MongoCompressor;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ServerApi;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;

class InternalStreamConnectionFactory implements InternalConnectionFactory {
    private final StreamFactory streamFactory;
    private final BsonDocument clientMetadataDocument;
    private final List<MongoCompressor> compressorList;
    private final CommandListener commandListener;
    @Nullable
    private ServerApi serverApi;
    private final MongoCredentialWithCache credential;

    InternalStreamConnectionFactory(final StreamFactory streamFactory, final MongoCredentialWithCache credential,
                                    final String applicationName, final MongoDriverInformation mongoDriverInformation,
                                    final List<MongoCompressor> compressorList,
                                    final CommandListener commandListener, @Nullable final ServerApi serverApi) {
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.compressorList = notNull("compressorList", compressorList);
        this.commandListener = commandListener;
        this.serverApi = serverApi;
        this.clientMetadataDocument = createClientMetadataDocument(applicationName, mongoDriverInformation);
        this.credential = credential;
    }

    @Override
    public InternalConnection create(final ServerId serverId) {
        Authenticator authenticator = credential == null ? null : createAuthenticator(credential);
        return new InternalStreamConnection(serverId, streamFactory, compressorList, commandListener,
                                            new InternalStreamConnectionInitializer(authenticator, clientMetadataDocument,
                                                                                           compressorList, serverApi));
    }

    private Authenticator createAuthenticator(final MongoCredentialWithCache credential) {
        if (credential.getAuthenticationMechanism() == null) {
            return new DefaultAuthenticator(credential, serverApi);
        }

        switch (credential.getAuthenticationMechanism()) {
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, serverApi);
            case PLAIN:
                return new PlainAuthenticator(credential, serverApi);
            case MONGODB_X509:
                return new X509Authenticator(credential, serverApi);
            case SCRAM_SHA_1:
            case SCRAM_SHA_256:
                return new ScramShaAuthenticator(credential, serverApi);
            case MONGODB_AWS:
                return new AwsAuthenticator(credential, serverApi);
            default:
                throw new IllegalArgumentException("Unsupported authentication mechanism " + credential.getAuthenticationMechanism());
        }
    }
}
