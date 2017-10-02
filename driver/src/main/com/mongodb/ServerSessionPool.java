/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.internal.connection.ConcurrentPool;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.UuidCodec;

import java.util.UUID;

class ServerSessionPool {
    private final ConcurrentPool<ServerSession> serverSessionPool =
            new ConcurrentPool<ServerSession>(Integer.MAX_VALUE, new ServerSessionItemFactory());

    ServerSession get() {
        return serverSessionPool.get();
    }

    void release(final ServerSession serverSession) {
        serverSessionPool.release(serverSession);
    }

    void close() {
        serverSessionPool.close();
    }

    private static class ServerSessionImpl implements ServerSession {
        private final BsonDocument identifier;
        private int transactionNumber;

        ServerSessionImpl(final BsonBinary identifier) {
            this.identifier = new BsonDocument("id", identifier);
        }

        @Override
        public BsonDocument getIdentifier() {
            return identifier;
        }

        @Override
        public long advanceTransactionNumber() {
            return transactionNumber++;
        }
    }

    private static final class ServerSessionItemFactory implements ConcurrentPool.ItemFactory<ServerSession> {
        @Override
        public ServerSession create(final boolean initialize) {
            return new ServerSessionImpl(createNewServerSessionIdentifier());
        }

        @Override
        public void close(final ServerSession serverSession) {
            // TODO: pruning
        }

        @Override
        public Prune shouldPrune(final ServerSession serverSession) {
            return Prune.STOP;
        }

        private BsonBinary createNewServerSessionIdentifier() {
            UuidCodec uuidCodec = new UuidCodec(UuidRepresentation.STANDARD);
            BsonDocument holder = new BsonDocument();
            BsonDocumentWriter bsonDocumentWriter = new BsonDocumentWriter(holder);
            bsonDocumentWriter.writeStartDocument();
            bsonDocumentWriter.writeName("id");
            uuidCodec.encode(bsonDocumentWriter, UUID.randomUUID(), EncoderContext.builder().build());
            bsonDocumentWriter.writeEndDocument();
            return holder.getBinary("id");
        }
    }
}
