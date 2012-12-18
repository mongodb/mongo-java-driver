/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.mongodb.CommandDocument;
import org.mongodb.DatabaseAdmin;
import org.mongodb.MongoOperations;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

/**
 * Runs the admin commands for a selected database.  This should be accessed from MongoDatabase.  The methods here are
 * not implemented in MongoDatabase in order to keep the API very simple, these should be the methods that are
 * not commonly used by clients of the driver.
 */
public class DatabaseAdminImpl implements DatabaseAdmin {
    private static final DropDatabase DROP_DATABASE = new DropDatabase();

    private final MongoOperations operations;
    private final String databaseName;
    private final DocumentSerializer documentSerializer;

    public DatabaseAdminImpl(final String databaseName, final MongoOperations operations,
                             final PrimitiveSerializers primitiveSerializers) {
        this.operations = operations;
        this.databaseName = databaseName;
        documentSerializer = new DocumentSerializer(primitiveSerializers);
    }

    @Override
    public void drop() {
        new CommandResult(operations.executeCommand(databaseName, DROP_DATABASE, documentSerializer));
    }

    private static final class DropDatabase extends MongoCommandOperation {
        private DropDatabase() {
            super(new CommandDocument("dropDatabase", 1));
        }
    }
}
