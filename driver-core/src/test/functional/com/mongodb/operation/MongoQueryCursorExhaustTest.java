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

import com.mongodb.FunctionalTest;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerDescription;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ReadPreference.primary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

public class MongoQueryCursorExhaustTest extends FunctionalTest {

    private final BsonBinary binary = new BsonBinary(new byte[10000]);
    private QueryResult<Document> firstBatch;
    private Connection exhaustConnection;
    private ConnectionSource readConnectionSource;

    @Before
    public void setUp() {
        assumeFalse(isSharded());

        super.setUp();

        for (int i = 0; i < 1000; i++) {
            getCollectionHelper().insertDocuments(new BsonDocument("_id", new BsonInt32(i)).append("bytes", binary));
        }

        readConnectionSource = getBinding().getReadConnectionSource();
        exhaustConnection = readConnectionSource.getConnection();
        firstBatch = exhaustConnection.query(getNamespace(), new BsonDocument(), null, 0, 0,
                                             false, false, false, false, true, false, false,
                                             new DocumentCodec());
    }

    @After
    public void tearDown() {
        if (exhaustConnection != null) {
            exhaustConnection.release();
        }
        if (readConnectionSource != null) {
            readConnectionSource.release();
        }
        super.tearDown();
    }

    @Test
    public void testExhaustReadAllDocuments() {
        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(getNamespace(), firstBatch, 0, 0,
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
        SingleConnectionBinding singleConnectionBinding = new SingleConnectionBinding(readConnectionSource, exhaustConnection);
        ConnectionSource source = singleConnectionBinding.getReadConnectionSource();
        Connection connection = source.getConnection();
        try {
            MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(getNamespace(), firstBatch, 0, 0,
                                                                               new DocumentCodec(),
                                                                               connection);

            cursor.next();
            cursor.close();

            connection.query(getNamespace(), new BsonDocument(), null, 0, 0, false, false, false, false, false, false, false,
                             new DocumentCodec());

        } finally {
            connection.release();
            source.release();
            singleConnectionBinding.release();
        }
    }

    private static class SingleConnectionBinding implements ReadBinding {
        private final Connection connection;
        private final ConnectionSource readConnectionSource;
        private int referenceCount = 1;

        public SingleConnectionBinding(final ConnectionSource readConnectionSource, final Connection connection) {
            this.readConnectionSource = readConnectionSource;
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
                public ServerDescription getServerDescription() {
                    return readConnectionSource.getServerDescription();
                }

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
