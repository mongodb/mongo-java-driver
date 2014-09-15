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
import com.mongodb.operation.InsertRequest;
import com.mongodb.protocol.message.InsertMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.mongodb.WriteResult;

import java.util.List;

import static java.lang.String.format;

/**
 * An implementation of the insert wire protocol.  This class also takes care of applying the write concern.
 *
 * @since 3.0
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-insert OP_INSERT
 */
public class InsertProtocol extends WriteProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest> insertRequestList;

    /**
     * Construct a new instance.
     *
     * @param namespace the namespace
     * @param ordered whether the inserts are ordered
     * @param writeConcern the write concern
     * @param insertRequestList the list of documents to insert
     */
    public InsertProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<InsertRequest> insertRequestList) {
        super(namespace, ordered, writeConcern);
        this.insertRequestList = insertRequestList;
    }

    @Override
    public WriteResult execute(final Connection connection) {
        LOGGER.debug(format("Inserting %d documents into namespace %s on connection [%s] to server %s", insertRequestList.size(),
                            getNamespace(), connection.getId(), connection.getServerAddress()));
        WriteResult writeResult = super.execute(connection);
        LOGGER.debug("Insert completed");
        return writeResult;
    }

    @Override
    public MongoFuture<WriteResult> executeAsync(final Connection connection) {
        LOGGER.debug(format("Asynchronously inserting %d documents into namespace %s on connection [%s] to server %s",
                            insertRequestList.size(), getNamespace(), connection.getId(), connection.getServerAddress()));
        final SingleResultFuture<WriteResult> future = new SingleResultFuture<WriteResult>();
        super.executeAsync(connection).register(new SingleResultCallback<WriteResult>() {
            @Override
            public void onResult(final WriteResult result, final MongoException e) {
                if (e == null) {
                    LOGGER.debug("Asynchronous insert completed");
                }
                future.init(result, e);
            }
        });
        return future;
    }

    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new InsertMessage(getNamespace().getFullName(), isOrdered(), getWriteConcern(), insertRequestList, settings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }
}
