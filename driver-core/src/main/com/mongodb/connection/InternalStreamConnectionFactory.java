/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDriverInformation;
import com.mongodb.event.ConnectionListener;
import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClientMetadataHelper.createClientMetadataDocument;

class InternalStreamConnectionFactory implements InternalConnectionFactory {
    private final StreamFactory streamFactory;
    private final ConnectionListener connectionListener;
    private final BsonDocument clientMetadataDocument;
    private final List<Authenticator> authenticators;

    public InternalStreamConnectionFactory(final StreamFactory streamFactory, final List<MongoCredential> credentialList,
                                           final ConnectionListener connectionListener, final String applicationName,
                                           final MongoDriverInformation mongoDriverInformation) {
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.clientMetadataDocument = createClientMetadataDocument(applicationName, mongoDriverInformation);
        notNull("credentialList", credentialList);
        this.authenticators = new ArrayList<Authenticator>(credentialList.size());
        for (MongoCredential credential : credentialList) {
            authenticators.add(createAuthenticator(credential));
        }
    }

    @Override
    public InternalConnection create(final ServerId serverId) {
        return new InternalStreamConnection(serverId, streamFactory,
                                            new InternalStreamConnectionInitializer(authenticators, clientMetadataDocument),
                                                   connectionListener);
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        if (credential.getAuthenticationMechanism() == null) {
            return new DefaultAuthenticator(credential);
        }

        switch (credential.getAuthenticationMechanism()) {
            case GSSAPI:
                return new GSSAPIAuthenticator(credential);
            case PLAIN:
                return new PlainAuthenticator(credential);
            case MONGODB_X509:
                return new X509Authenticator(credential);
            case SCRAM_SHA_1:
                return new ScramSha1Authenticator(credential);
            case MONGODB_CR:
                return new NativeAuthenticator(credential);
            default:
                throw new IllegalArgumentException("Unsupported authentication mechanism " + credential.getAuthenticationMechanism());
        }
    }
}
