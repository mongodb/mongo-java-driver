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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.OperationHelper.CallableWithConnection;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * Unlocks the MongoDB server, allowing write operations to go through. This may be asynchronous on the server, which means
 * there may be a small delay before the database becomes writable.
 *
 * @mongodb.driver.manual reference/command/fsyncUnlock/ fsyncUnlock command
 * @since 3.2
 */
@Deprecated
public class FsyncUnlockOperation implements WriteOperation<BsonDocument>, ReadOperation<BsonDocument> {
    private static final BsonDocument FSYNC_UNLOCK_COMMAND = new BsonDocument("fsyncUnlock", new BsonInt32(1));

    /**
     * Unlocks the MongoDB server, allowing write operations to go through.
     *
     * @param binding the binding to execute in the context of
     * @return the result of the operation
     * @deprecated use {@link #execute(ReadBinding)} instead.
     */
    @Deprecated
    @Override
    public BsonDocument execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<BsonDocument>() {
            @Override
            public BsonDocument call(final Connection connection) {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    return executeCommand(binding, "admin", FSYNC_UNLOCK_COMMAND, connection);
                } else {
                    return queryUnlock(connection);
                }
            }
        });
    }

    @Override
    public BsonDocument execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<BsonDocument>() {
            @Override
            public BsonDocument call(final Connection connection) {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    return executeCommand(binding, "admin", getCommandCreator(), false);
                } else {
                    return queryUnlock(connection);
                }
            }
        });
    }

    private BsonDocument queryUnlock(final Connection connection) {
        return connection.query(new MongoNamespace("admin", "$cmd.sys.unlock"), new BsonDocument(), null, 0, 1, 0,
                false, false, false, false, false, false,
                new BsonDocumentCodec()).getResults().get(0);
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return FSYNC_UNLOCK_COMMAND;
            }
        };
    }
}
