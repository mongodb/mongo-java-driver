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

package com.mongodb.operation;

import com.mongodb.ReadPreference;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonDocument;
import org.bson.types.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.util.EnumSet;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.client.Fixture.getBinding;
import static com.mongodb.client.Fixture.isSharded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class MongoQueryCursorExhaustTest extends DatabaseTestCase {

    private final Binary binary = new Binary(new byte[10000]);
    private EnumSet<QueryFlag> exhaustFlag = EnumSet.of(QueryFlag.Exhaust);
    private QueryResult<Document> firstBatch;
    private Connection exhaustConnection;
    private ConnectionSource readConnectionSource;

    @Before
    public void setUp() {
        super.setUp();

        for (int i = 0; i < 1000; i++) {
            collection.insert(new Document("_id", i).append("bytes", binary));
        }

        readConnectionSource = getBinding().getReadConnectionSource();
        exhaustConnection = readConnectionSource.getConnection();
        firstBatch = new QueryProtocol<Document>(collection.getNamespace(), exhaustFlag, 0, 0, new BsonDocument(), null,
                                                 new DocumentCodec())
                     .execute(exhaustConnection);

    }

    @After
    public void tearDown() {
        exhaustConnection.release();
        readConnectionSource.release();
        super.tearDown();
    }

    @Test
    public void testExhaustReadAllDocuments() {
        assumeFalse(isSharded());

        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(), firstBatch, 0, 0,
                                                                           new DocumentCodec(), exhaustConnection);

        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertEquals(1000, count);
    }

    @Test
    public void testExhaustCloseBeforeReadingAllDocuments() {
        assumeFalse(isSharded());
        SingleConnectionBinding singleConnectionBinding = new SingleConnectionBinding(exhaustConnection);
        ConnectionSource source = singleConnectionBinding.getReadConnectionSource();
        Connection connection = source.getConnection();
        try {
            MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(), firstBatch, 0, 0,
                                                                               new DocumentCodec(),
                                                                               connection);

            cursor.next();
            cursor.close();

            new QueryProtocol<Document>(collection.getNamespace(), EnumSet.noneOf(QueryFlag.class), 0, 0, new BsonDocument(), null,
                                        new DocumentCodec())
            .execute(connection);

        } finally {
            connection.release();
            source.release();
            singleConnectionBinding.release();
        }
    }

    private static class SingleConnectionBinding implements ReadBinding {
        private final Connection connection;
        private int referenceCount = 1;

        public SingleConnectionBinding(final Connection connection) {
            this.connection = connection.retain();
        }

        @Override
        public int getCount() {
            return 1;
        }

        @Override
        public ReadPreference getReadPreference() {
            return primary();
        }

        @Override
        public ConnectionSource getReadConnectionSource() {
            return new ConnectionSource() {
                @Override
                public Connection getConnection() {
                    return connection.retain();
                }

                @Override
                public ConnectionSource retain() {
                    referenceCount++;
                    return this;
                }

                @Override
                public int getCount() {
                    return referenceCount;
                }

                @Override
                public void release() {
                    referenceCount--;
                    if (referenceCount == 0) {
                        connection.release();
                    }
                }
            };
        }

        @Override
        public ReadBinding retain() {
            return this;
        }

        @Override
        public void release() {
        }
    }

}
