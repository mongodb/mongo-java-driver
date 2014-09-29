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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.DescriptionHelper.createConnectionDescription;

class PipelinedConnectionInitializer implements ConnectionInitializer, InternalConnection {
    private static final AtomicInteger INCREMENTING_ID = new AtomicInteger();
    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final List<MongoCredential> credentialList;
    private final StreamPipeline streamPipeline;
    private volatile boolean isClosed;

    static final Logger LOGGER = Loggers.getLogger("ConnectionInitializer");

    private String id;
    private ConnectionDescription connectionDescription;

    PipelinedConnectionInitializer(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                                   final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        this.streamPipeline = new StreamPipeline(clusterId, stream, connectionListener, this, true);
    }

    @Override
    public ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ConnectionDescription getDescription() {
        return connectionDescription;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return stream.getBuffer(size);
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        streamPipeline.sendMessage(byteBuffers, lastRequestId);
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        return streamPipeline.receiveMessage(responseTo);
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        streamPipeline.sendMessageAsync(byteBuffers, lastRequestId, callback);
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        streamPipeline.receiveMessageAsync(responseTo, callback);
    }

    @Override
    public void close() {
        isClosed = true;
        stream.close();
        connectionListener.connectionClosed(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void initialize() {
        try {
            initializeConnectionId();
            initializeServerDescription();
            authenticateAll();

            // try again if there was an exception calling getlasterror before authenticating
            if (id.contains("*")) {
                initializeConnectionId();
            }
        } catch (Throwable t) {
            LOGGER.warn("Exception initializing the connection", t);
            close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public void initialize(final SingleResultCallback<Void> initializationFuture) {
        try {
            initialize();
        } catch (MongoException e) {
            LOGGER.warn("Exception initializing the connection", e);
            initializationFuture.onResult(null, e);
            return;
        }
        initializationFuture.onResult(null, null);
    }

    private void initializeConnectionId() {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      this);
        id = "conn" + (response.containsKey("connectionId")
                       ? response.getNumber("connectionId").intValue()
                       : "*" + INCREMENTING_ID.incrementAndGet() + "*");
    }

    private void initializeServerDescription() {
        BsonDocument isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), this);
        BsonDocument buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), this);
        connectionDescription = createConnectionDescription(stream.getAddress(), isMasterResult, buildInfoResult);
    }


    private void authenticateAll() {
        if (connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER) {
            for (final MongoCredential cur : credentialList) {
                createAuthenticator(cur).authenticate();
            }
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        switch (credential.getAuthenticationMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(credential, this);
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, this);
            case PLAIN:
                return new PlainAuthenticator(credential, this);
            case MONGODB_X509:
                return new X509Authenticator(credential, this);
            case SCRAM_SHA_1:
                return new ScramSha1Authenticator(credential, this);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
    }

}
