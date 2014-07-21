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

package com.mongodb.client;

import com.mongodb.ReadPreference;
import com.mongodb.annotations.ThreadSafe;
import org.bson.codecs.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
@ThreadSafe
public interface MongoDatabase {
    String getName();

    CommandResult executeCommand(Document command);

    CommandResult executeCommand(final Document command, final ReadPreference readPreference);

    MongoDatabaseOptions getOptions();

    MongoCollection<Document> getCollection(String name);

    MongoCollection<Document> getCollection(String name, MongoCollectionOptions options);

    <T> MongoCollection<T> getCollection(String name, Codec<T> codec);

    <T> MongoCollection<T> getCollection(String name, Codec<T> codec, MongoCollectionOptions options);

    //TODO: still need to come up with a sensible name for this
    DatabaseAdministration tools();

    //    MongoDatabase withClient(MongoClient client);
    //
    //    MongoDatabase withWriteConcern(WriteConcern writeConcern);
}
