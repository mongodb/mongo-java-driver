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

package org.mongodb.protocol;

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.bson.codecs.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteResult;
import org.mongodb.operation.InsertRequest;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.protocol.message.InsertMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.List;

import static java.lang.String.format;

public class InsertProtocol<T> extends WriteProtocol {

    private static final Logger LOGGER = Loggers.getLogger("protocol.insert");

    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertProtocol(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                          final List<InsertRequest<T>> insertRequestList, final Encoder<T> encoder) {
        super(namespace, ordered, writeConcern);
        this.insertRequestList = insertRequestList;
        this.encoder = encoder;
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
        return new InsertMessage<T>(getNamespace().getFullName(), isOrdered(), getWriteConcern(), insertRequestList, encoder, settings);
    }

    @Override
    protected com.mongodb.diagnostics.logging.Logger getLogger() {
        return LOGGER;
    }
}
