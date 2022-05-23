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

package com.mongodb;

import com.mongodb.client.AbstractClientSideEncryptionTest;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
@RunWith(Parameterized.class)
public class ClientSideEncryptionLegacyTest extends AbstractClientSideEncryptionTest {

    private MongoClient mongoClient;

    public ClientSideEncryptionLegacyTest(final String filename, final String description, final BsonDocument specDocument,
                                          final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        super(filename, description, specDocument, data, definition, skipTest);
    }

    @Override
    protected void createMongoClient(final MongoClientSettings mongoClientSettings) {
        mongoClient = new MongoClient(mongoClientSettings);
    }

    @Override
    protected MongoDatabase getDatabase(final String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
