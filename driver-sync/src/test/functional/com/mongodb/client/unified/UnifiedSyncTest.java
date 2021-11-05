/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.unified;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;

public abstract class UnifiedSyncTest extends UnifiedTest {
    public UnifiedSyncTest(@Nullable final String fileDescription, final String schemaVersion, final BsonArray runOnRequirements,
            final BsonArray entitiesArray, final BsonArray initialData, final BsonDocument definition) {
        super(fileDescription, schemaVersion, runOnRequirements, entitiesArray, initialData, definition);
    }

    public UnifiedSyncTest(final String schemaVersion, final BsonArray runOnRequirements,
            final BsonArray entitiesArray, final BsonArray initialData, final BsonDocument definition) {
        this(null, schemaVersion, runOnRequirements, entitiesArray, initialData, definition);
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    @Override
    protected GridFSBucket createGridFSBucket(final MongoDatabase database) {
        return GridFSBuckets.create(database);
    }
}
