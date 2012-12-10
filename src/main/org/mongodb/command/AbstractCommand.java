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

import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.result.CommandResult;
import org.mongodb.MongoClient;

public abstract class AbstractCommand implements Command {
    private final MongoClient mongoClient;
    private final String database;

    public AbstractCommand(final MongoClient mongoClient, final String database) {
        this.mongoClient = mongoClient;
        this.database = database;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public String getDatabase() {
        return database;
    }

    public CommandResult execute() {
        return new CommandResult(mongoClient.getOperations().executeCommand(database, new MongoCommandOperation(asMongoCommand())));
    }

    // TODO: the class of the return type is weird for a command
    public abstract org.mongodb.operation.MongoCommand asMongoCommand();


}
