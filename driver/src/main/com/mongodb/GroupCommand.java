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

package com.mongodb;

import com.mongodb.client.model.Collation;
import com.mongodb.operation.GroupOperation;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class groups the argument for a group operation and can build the underlying command object
 *
 * @mongodb.driver.manual reference/command/group/ Group
 */
public class GroupCommand {
    private final String collectionName;
    private final DBObject keys;
    private final String keyf;
    private final DBObject condition;
    private final DBObject initial;
    private final String reduce;
    private final String finalize;
    private final Collation collation;

    /**
     * Creates a new group command.
     *
     * @param collection the collection from which to perform the group by operation.
     * @param keys       the field or fields to group.
     * @param condition  optional - a filter to determine which documents in the collection to process.
     * @param initial    the initial state of the aggregation result document.
     * @param reduce     a JavaScript aggregation function that operates on the documents during the grouping operation.
     * @param finalize   optional - a JavaScript function that runs each item in the result set before group returns the final value.
     */
    public GroupCommand(final DBCollection collection, final DBObject keys, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize) {
        this(collection, keys, condition, initial, reduce, finalize, null);
    }

    /**
     * Creates a new group command.
     *
     * @param collection the collection from which to perform the group by operation.
     * @param keys       the field or fields to group.
     * @param condition  optional - a filter to determine which documents in the collection to process.
     * @param initial    the initial state of the aggregation result document.
     * @param reduce     a JavaScript aggregation function that operates on the documents during the grouping operation.
     * @param finalize   optional - a JavaScript function that runs each item in the result set before group returns the final value.
     * @param collation  optional - the collation options
     *
     * @since 3.4
     */
    public GroupCommand(final DBCollection collection, final DBObject keys, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize, final Collation collation) {
        notNull("collection", collection);
        this.collectionName = collection.getName();
        this.keys = keys;
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
        this.keyf = null;
        this.collation = collation;
    }

    /**
     * Creates a new group command.
     *
     * @param collection the collection from which to perform the group by operation.
     * @param keyf       the function that creates a "key object" for use as the grouping key
     * @param condition  optional - a filter to determine which documents in the collection to process.
     * @param initial    the initial state of the aggregation result document.
     * @param reduce     a JavaScript aggregation function that operates on the documents during the grouping operation.
     * @param finalize   optional - a JavaScript function that runs each item in the result set before group returns the final value.
     *
     * @since 3.1
     */
    public GroupCommand(final DBCollection collection, final String keyf, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize) {
        this(collection, keyf, condition, initial, reduce, finalize, null);
    }

    /**
     * Creates a new group command.
     *
     * @param collection the collection from which to perform the group by operation.
     * @param keyf       the function that creates a "key object" for use as the grouping key
     * @param condition  optional - a filter to determine which documents in the collection to process.
     * @param initial    the initial state of the aggregation result document.
     * @param reduce     a JavaScript aggregation function that operates on the documents during the grouping operation.
     * @param finalize   optional - a JavaScript function that runs each item in the result set before group returns the final value.
     * @param collation  optional - the collation options
     *
     * @since 3.4
     */
    public GroupCommand(final DBCollection collection, final String keyf, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize, final Collation collation) {
        notNull("collection", collection);
        this.collectionName = collection.getName();
        this.keyf = notNull("keyf", keyf);
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
        this.keys = null;
        this.collation = collation;
    }

    /**
     * Turns this group command into the DBObject format of the command.
     *
     * @return a DBObject containing the group command as a MongoDB document
     */
    public DBObject toDBObject() {
        DBObject args = new BasicDBObject("ns", collectionName).append("cond", condition)
                                                               .append("$reduce", reduce)
                                                               .append("initial", initial);

        if (keys != null) {
            args.put("key", keys);
        }

        if (keyf != null) {
            args.put("$keyf", keyf);
        }

        if (finalize != null) {
            args.put("finalize", finalize);
        }
        return new BasicDBObject("group", args);
    }

    GroupOperation<DBObject> toOperation(final MongoNamespace namespace, final DBObjectCodec codec) {
        if (initial == null) {
            throw new IllegalArgumentException("Group command requires an initial document for the aggregate result");
        }

        if (reduce == null) {
            throw new IllegalArgumentException("Group command requires a reduce function for the aggregate result");
        }

        GroupOperation<DBObject> operation = new GroupOperation<DBObject>(namespace,
                                                                          new BsonJavaScript(reduce),
                                                                          new BsonDocumentWrapper<DBObject>(initial, codec), codec);

        if (keys != null) {
            operation.key(new BsonDocumentWrapper<DBObject>(keys, codec));
        }

        if (keyf != null) {
            operation.keyFunction(new BsonJavaScript(keyf));
        }

        if (condition != null) {
            operation.filter(new BsonDocumentWrapper<DBObject>(condition, codec));
        }

        if (finalize != null) {
            operation.finalizeFunction(new BsonJavaScript(finalize));
        }
        operation.collation(collation);
        return operation;
    }

}
