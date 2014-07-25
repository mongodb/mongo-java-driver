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


import com.mongodb.MongoClientSettings;
import com.mongodb.annotations.ThreadSafe;

import java.io.Closeable;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 *
 * @since 3.0.0
 */
@ThreadSafe
public interface MongoClient extends Closeable {
    /**
     * @param databaseName the name of the database to retrieve
     * @return a MongoDatabase representing the specified database
     */
    MongoDatabase getDatabase(String databaseName);

    /**
     * @param databaseName the name of the database to retrieve
     * @return a MongoDatabase representing the specified database
     */
    MongoDatabase getDatabase(String databaseName, MongoDatabaseOptions options);

    /**
     * Close the client, releasing all resources.  Implementations of this method should be idempotent.
     */
    void close();

    /**
     * Get the settings for this client.
     *
     * @return the settings
     */
    MongoClientSettings getSettings();

    /**
     * @return the ClientAdministration that provides admin methods that can be performed
     */
    ClientAdministration tools();
}
