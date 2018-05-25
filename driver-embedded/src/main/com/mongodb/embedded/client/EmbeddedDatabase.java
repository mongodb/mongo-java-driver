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

package com.mongodb.embedded.client;

import com.mongodb.MongoException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.sun.jna.Pointer;

import java.io.Closeable;
import java.io.File;

import static java.lang.String.format;

final class EmbeddedDatabase implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("embedded.client");
    private volatile Pointer databasePointer;

    EmbeddedDatabase(final MongoClientSettings mongoClientSettings) {
        File directory = new File(mongoClientSettings.getDbPath());
        try {
            if (directory.mkdirs() && LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Created dbpath directory: %s", mongoClientSettings.getDbPath()));
            }
        } catch (SecurityException e) {
            throw new MongoException(format("Could not validate / create the dbpath: %s", mongoClientSettings.getDbPath()));
        }

        String yamlConfig = createYamlConfig(mongoClientSettings);
        this.databasePointer = MongoDBCAPIHelper.db_new(yamlConfig);
    }

    public EmbeddedConnection createConnection() {
        return new EmbeddedConnection(databasePointer);
    }

    public void pump() {
        MongoDBCAPIHelper.db_pump(databasePointer);
    }

    @Override
    public void close() {
        if (databasePointer != null) {
            MongoDBCAPIHelper.db_destroy(databasePointer);
            databasePointer = null;
        }
    }

    private String createYamlConfig(final MongoClientSettings mongoClientSettings) {
        return format("{ storage: { dbPath: %s } }", mongoClientSettings.getDbPath());
    }

}
