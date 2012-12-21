/**
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
 *
 */

package org.mongodb.command;

import org.bson.types.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

public abstract class AbstractCommand implements Command {
    private final MongoDatabase database;

    public AbstractCommand(final MongoDatabase database) {
        this.database = database;
    }

    public MongoClient getMongoClient() {
        return database.getClient();
    }

    public String getDatabaseName() {
        return database.getName();
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public CommandResult execute() {
        return database.executeCommand(new MongoCommandOperation(asMongoCommand()));
    }

    public abstract MongoCommand asMongoCommand();

    // TODO: this may need to use collection's serializers
    protected Serializer<Document> createResultSerializer() {
        return new DocumentSerializer(database.getPrimitiveSerializers());
    }


}
