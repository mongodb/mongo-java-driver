/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb;

import com.mongodb.connection.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.notNull;

class ClientSessionContext implements SessionContext {

    private ClientSession clientSession;

    ClientSessionContext(final ClientSession clientSession) {
        this.clientSession = notNull("clientSession", clientSession);
    }

    ClientSession getClientSession() {
        return clientSession;
    }

    @Override
    public boolean hasSession() {
        return true;
    }

    @Override
    public BsonDocument getSessionId() {
        return clientSession.getServerSession().getIdentifier();
    }

    @Override
    public boolean isCausallyConsistent() {
        return clientSession.isCausallyConsistent();
    }

    @Override
    public long advanceTransactionNumber() {
        return clientSession.getServerSession().advanceTransactionNumber();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return clientSession.getOperationTime();
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp operationTime) {
        clientSession.advanceOperationTime(operationTime);
    }

    @Override
    public BsonDocument getClusterTime() {
        return clientSession.getClusterTime();
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        clientSession.advanceClusterTime(clusterTime);
    }
}
