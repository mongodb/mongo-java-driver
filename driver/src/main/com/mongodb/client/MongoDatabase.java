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
import org.mongodb.Document;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
@ThreadSafe
public interface MongoDatabase {
    String getName();

    Document executeCommand(Document command);

    Document executeCommand(final Document command, final ReadPreference readPreference);

    MongoDatabaseOptions getOptions();

    MongoCollection<Document> getCollection(String name);

    MongoCollection<Document> getCollection(String name, MongoCollectionOptions options);

    <T> MongoCollection<T> getCollection(String name, Class<T> clazz);

    <T> MongoCollection<T> getCollection(String name, Class<T> clazz, MongoCollectionOptions options);

    <T> NewMongoCollection<T> getNewCollection(String name, Class<T> clazz, MongoCollectionOptions options);

    DatabaseAdministration tools();
}
