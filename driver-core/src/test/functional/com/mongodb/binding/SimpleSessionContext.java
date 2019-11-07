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

package com.mongodb.binding;

import com.mongodb.ReadConcern;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonTimestamp;
import org.bson.UuidRepresentation;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.UuidCodec;

import java.util.UUID;

class SimpleSessionContext implements SessionContext {
    private final BsonDocument sessionId;
    private BsonTimestamp operationTime;
    private long counter;
    private BsonDocument clusterTime;

    SimpleSessionContext() {
        this.sessionId = createNewServerSessionIdentifier();
    }

    @Override
    public boolean hasSession() {
        return true;
    }

    @Override
    public boolean isImplicitSession() {
        return true;
    }

    @Override
    public BsonDocument getSessionId() {
        return sessionId;
    }

    @Override
    public boolean isCausallyConsistent() {
        return false;
    }

    @Override
    public long getTransactionNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long advanceTransactionNumber() {
        counter++;
        return counter;
    }

    @Override
    public boolean notifyMessageSent() {
        return false;
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return operationTime;
    }

    @Override
    public void advanceOperationTime(final BsonTimestamp operationTime) {
        this.operationTime = operationTime;
    }

    @Override
    public BsonDocument getClusterTime() {
        return clusterTime;
    }

    @Override
    public void advanceClusterTime(final BsonDocument clusterTime) {
        this.clusterTime = clusterTime;
    }

    @Override
    public boolean hasActiveTransaction() {
        return false;
    }

    @Override
    public ReadConcern getReadConcern() {
        return ReadConcern.DEFAULT;
    }

    @Override
    public void setRecoveryToken(final BsonDocument recoveryToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unpinServerAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markSessionDirty() {
    }

    @Override
    public boolean isSessionMarkedDirty() {
        return false;
    }

    private static BsonDocument createNewServerSessionIdentifier() {
        UuidCodec uuidCodec = new UuidCodec(UuidRepresentation.STANDARD);
        BsonDocument holder = new BsonDocument();
        BsonDocumentWriter bsonDocumentWriter = new BsonDocumentWriter(holder);
        bsonDocumentWriter.writeStartDocument();
        bsonDocumentWriter.writeName("id");
        uuidCodec.encode(bsonDocumentWriter, UUID.randomUUID(), EncoderContext.builder().build());
        bsonDocumentWriter.writeEndDocument();
        return holder;
    }
}
