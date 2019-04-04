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
 * An operation that determines the current operation on a MongoDB server.
 *
 * @since 3.2
 * @mongodb.driver.manual reference/method/db.currentOp/ Current Op
 */
@Deprecated
public class CurrentOpOperation implements ReadOperation<BsonDocument> {
    @Override
    public BsonDocument execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<BsonDocument>() {
            @Override
            public BsonDocument call(final Connection connection) {
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    return executeCommand(binding, "admin", getCommandCreator(), true);
                } else {
                    return connection.query(new MongoNamespace("admin", "$cmd.sys.inprog"), new BsonDocument(), null, 0, 1, 0,
                                           binding.getReadPreference().isSlaveOk(), false, false, false, false, false,
                                           new BsonDocumentCodec())
                           .getResults().get(0);
                }
            }
        });
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return new BsonDocument("currentOp", new BsonInt32(1));
            }
        };
    }
}
