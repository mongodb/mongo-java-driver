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
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.protocol.message.DeleteMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.mongodb.WriteResult;

import java.util.List;

import static java.lang.String.format;

/**
 * An implementation of the MongoDB OP_DELETE wire protocol.
 *
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-delete OP_DELETE
 * @since 3.0
 */
public class DeleteProtocol extends WriteProtocol {
    private static final Logger LOGGER = Loggers.getLogger("protocol.delete");

    private final List<DeleteRequest> deletes;

    /**
     * Construct an instance.
     *
     * @param namespace    the namespace
     * @param ordered      whether the delete are ordered
     * @param writeConcern the write concern to apply
     * @param deletes      the deletes
     */
    public DeleteProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<DeleteRequest> deletes) {
        super(namespace, ordered, writeConcern);
        this.deletes = deletes;
    }

    @Override
    public WriteResult execute(final Connection connection) {
        LOGGER.debug(format("Deleting documents from namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getId(), connection.getServerAddress()));
        WriteResult writeResult = super.execute(connection);
        LOGGER.debug("Delete completed");
        return writeResult;
    }

    @Override
    public MongoFuture<WriteResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously deleting documents in namespace %s on connection [%s] to server %s", getNamespace(),
                            connection.getId(), connection.getServerAddress()));
        final SingleResultFuture<WriteResult> future = new SingleResultFuture<WriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<WriteResult>() {
            @Override
            public void onResult(final WriteResult result, final MongoException e) {
                if (e == null) {
                    LOGGER.debug("Asynchronous delete completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }


    @Override
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new DeleteMessage(getNamespace().getFullName(), deletes, settings);
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
