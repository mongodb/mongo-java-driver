/*
 * Copyright (c) 2008 - 2013 MongoDB, Inc. <http://mongodb.com>
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

import org.mongodb.Document;
import org.mongodb.operation.InlineMongoCursor;

import java.util.Iterator;

/**
 * Represents the result of a map/reduce operation.  Users should interact with the results of the map reduce via the results() method, or
 * by interacting directly with the collection the results were input into.
 * <p/>
 * There will be substantial changes to this class in the 3.x release, please check the deprecation tags for the methods that will be
 * removed.
 */
public class MapReduceOutput {

    private final DBCollection collection;
    private final DBObject command;
    private final ServerAddress serverAddress;
    private final org.mongodb.MongoCursor<DBObject> results;

    private org.mongodb.CommandResult commandResult;

    //TODO: not really sure about outputCollection here.  Only needed for drop after all.
    MapReduceOutput(final DBObject command, final org.mongodb.MongoCursor<DBObject> results, final DBCollection outputCollection) {
        this.command = command;
        this.results = results;

        this.serverAddress = new ServerAddress(results.getServerAddress());
        this.collection = outputCollection;
        if (results instanceof InlineMongoCursor) {
            commandResult = ((InlineMongoCursor) results).getCommandResult();
        }
    }

    /**
     * Returns a cursor to the results of the operation.
     *
     * @return the results in iterable form
     */
    @SuppressWarnings("unchecked")
    public Iterable<DBObject> results() {
        return new SingleShotIterable(results);
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
     * Gets the collection that holds the results (Will return null if results are Inline).
     *
     * @return the collection or null
     */
    public DBCollection getOutputCollection() {
        //TODO: test this
        return collection;
    }

    /**
     * Get the original command that was sent to the database.
     *
     * @return a DBObject containing the values of the original map-reduce command.
     */
    public DBObject getCommand() {
        return command;
    }

    /**
     * Get the server that the map reduce command was run on.
     *
     * @return a ServerAddress of the server that the command ran against.
     */
    public ServerAddress getServerUsed() {
        return serverAddress;
    }

    public String toString() {
        return commandResult.toString();
    }

    /**
     * Get the name of the collection that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the collection that the map reduce results are stored in
     */
    public final String getCollectionName() {
        return collection.getName();
    }

    /**
     * Get the name of the database that the results of the map reduce were saved into.  If the map reduce was an inline operation (i.e .
     * the results were returned directly from calling the map reduce) this will return null.
     *
     * @return the name of the database that holds the collection that the map reduce results are stored in
     */
    public String getDatabaseName() {
        return collection.getDB().getName();
    }

    /**
     * Get the amount of time, in milliseconds, that it took to run this map reduce.
     *
     * @return an int representing the number of milliseconds it took to run the map reduce operation
     */
    public int getDuration() {
        return commandResult.getResponse().getInteger("timeMillis");
    }

    /**
     * Get the number of documents that were input into the map reduce operation
     *
     * @return the number of documents that read while processing this map reduce
     */
    public int getInputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("input");
    }

    /**
     * Get the number of documents generated as a result of this map reduce
     *
     * @return the number of documents output by the map reduce
     */
    public int getOutputCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("output");
    }

    /**
     * Get the number of messages emitted from the provided map function.
     *
     * @return the number of items emitted from the map function
     */
    public int getEmitCount() {
        return ((Document) commandResult.getResponse().get("counts")).getInteger("emit");
    }

    private final class SingleShotIterable implements Iterable<DBObject> {
        private final org.mongodb.MongoCursor<DBObject> cursor;

        private SingleShotIterable(final org.mongodb.MongoCursor<DBObject> cursor) {
            this.cursor = cursor;
        }

        @Override
        public Iterator<DBObject> iterator() {
            return cursor;
        }
    }
}
