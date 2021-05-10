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

package com.mongodb.internal.operation;

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import org.bson.BsonDocument;

/**
 * The Command Creator interface
 */
public interface CommandCreator {
    /**
     * Creates the command to run based on the available context.
     *
     * @param clientSideOperationTimeout the client side timeout
     * @param serverDescription the server description
     * @param connectionDescription the connection description
     * @return the command to run for the given server, connection and timeout.
     */
    BsonDocument create(
            ClientSideOperationTimeout clientSideOperationTimeout, ServerDescription serverDescription,
            ConnectionDescription connectionDescription);
}
