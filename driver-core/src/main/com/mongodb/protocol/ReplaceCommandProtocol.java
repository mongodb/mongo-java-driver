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

package com.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.connection.Connection;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.operation.ReplaceRequest;
import com.mongodb.operation.WriteRequest;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.ReplaceCommandMessage;
import org.mongodb.BulkWriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * An implementation of the update command that handles full document replacements
 *
 * @mongodb.driver.manual manual/reference/command/insert/#dbcmd.update Update Command
 * @since 3.0
 */
public class ReplaceCommandProtocol extends WriteCommandProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.replace");

    private final List<ReplaceRequest> replaceRequests;

    /**
     * Construct an instance.
     *
     * @param namespace       the namespace
     * @param ordered         whether the inserts are ordered
     * @param writeConcern    the write concern
     * @param replaceRequests the list of inserts
     */
    public ReplaceCommandProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                  final List<ReplaceRequest> replaceRequests) {
        super(namespace, ordered, writeConcern);
        this.replaceRequests = notNull("replaces", replaceRequests);
    }

    @Override
    public BulkWriteResult execute(final Connection connection) {
        LOGGER.debug(format("Replacing document in namespace %s on connection [%s] to server %s", getNamespace(), connection.getId(),
                            connection.getServerAddress()));
        BulkWriteResult writeResult = super.execute(connection);
        LOGGER.debug("Replace completed");
        return writeResult;
    }

    @Override
    public MongoFuture<BulkWriteResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously replacing document in namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getId(), connection.getServerAddress()));
        final SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<BulkWriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final MongoException e) {
                if (e != null) {
                    LOGGER.debug("Asynchronous replace completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.REPLACE;
    }

    @Override
    protected ReplaceCommandMessage createRequestMessage(final MessageSettings messageSettings) {
        return new ReplaceCommandMessage(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests, messageSettings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }

}
