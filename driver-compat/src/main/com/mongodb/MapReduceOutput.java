/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

// MapReduceOutput.java

package com.mongodb;

import org.mongodb.command.MapReduceCommandResult;
import org.mongodb.command.MapReduceInlineCommandResult;

import static com.mongodb.DBObjects.toDBObject;
import static com.mongodb.DBObjects.toDocument;

/**
 * Represents the result of a map/reduce operation
 *
 * @author antoine
 */
public class MapReduceOutput {

    private final org.mongodb.CommandResult commandResult;
    private final DBCollection collection;

    MapReduceOutput(final DBCollection collection, final MapReduceCommandResult commandResult) {
        this.commandResult = commandResult;
        this.collection = getTargetCollection(
                collection,
                commandResult.getDatabaseName(),
                commandResult.getCollectionName()
        );
    }

    MapReduceOutput(final DBCollection collection, final MapReduceInlineCommandResult<DBObject> commandResult) {
        this.collection = null;
        this.commandResult = commandResult;
    }

    public MapReduceOutput(final DBCollection collection, final DBObject command, final CommandResult commandResult) {

        final org.mongodb.CommandResult result = new org.mongodb.CommandResult(
                toDocument(command),
                commandResult.getServerUsed().toNew(),
                toDocument(commandResult),
                0
        );

        if (commandResult.containsField("results")) {
            this.collection = null;
            this.commandResult = new MapReduceInlineCommandResult<DBObject>(result);
        } else {
            final MapReduceCommandResult mapReduceCommandResult = new MapReduceCommandResult(result);
            this.collection = getTargetCollection(
                    collection,
                    mapReduceCommandResult.getDatabaseName(),
                    mapReduceCommandResult.getCollectionName()
            );
            this.commandResult = mapReduceCommandResult;
        }

    }

    /**
     * returns a cursor to the results of the operation
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        return commandResult instanceof MapReduceInlineCommandResult
                ? ((MapReduceInlineCommandResult) commandResult).getResults()
                : getOutputCollection().find();
    }

    /**
     * drops the collection that holds the results
     *
     * @throws MongoException
     */
    public void drop() {
        if (collection != null) {
            collection.drop();
        }
    }

    /**
     * Gets the collection that holds the results (Will return null if results are Inline)
     *
     * @return
     */
    public DBCollection getOutputCollection() {
        return collection;
    }

    @Deprecated
    public BasicDBObject getRaw() {
        return getCommandResult();
    }

    public CommandResult getCommandResult() {
        return new CommandResult(commandResult);
    }

    public DBObject getCommand() {
        return toDBObject(commandResult.getCommand());
    }

    public ServerAddress getServerUsed() {
        return new ServerAddress(commandResult.getAddress());
    }

    public String toString() {
        return commandResult.toString();
    }

    private static DBCollection getTargetCollection(final DBCollection collection,
                                                    final String databaseName, final String collectionName) {
        final DB database = databaseName != null ? collection.getDB().getSisterDB(databaseName) : collection.getDB();
        return database.getCollection(collectionName);
    }

}
