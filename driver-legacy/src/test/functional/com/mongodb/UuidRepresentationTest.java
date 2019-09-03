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

import com.mongodb.client.AbstractUuidRepresentationTest;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.After;

import java.util.UUID;

public class UuidRepresentationTest extends AbstractUuidRepresentationTest {

    private MongoClient mongoClient;

    public UuidRepresentationTest(final UuidRepresentation uuidRepresentation, final BsonBinarySubType subType,
                                  final UUID uuid, final byte[] encodedValue, final byte[] standardEncodedValue) {
        super(uuidRepresentation, subType, uuid, encodedValue, standardEncodedValue);
    }


    @Override
    protected void createMongoClient(final UuidRepresentation uuidRepresentation, final CodecRegistry codecRegistry) {
        mongoClient = new MongoClient(Fixture.getMongoClientURI(MongoClientOptions.builder(Fixture.getOptions())
                .uuidRepresentation(uuidRepresentation)
                .codecRegistry(codecRegistry)));
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
