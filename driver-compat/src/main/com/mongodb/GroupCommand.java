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

import com.mongodb.operation.Group;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonJavaScript;

/**
 * This class groups the argument for a group operation and can build the underlying command object
 *
 * @mongodb.driver.manual reference/command/group/ Group
 */
public class GroupCommand {
    private final String input;
    private final DBObject keys;
    private final DBObject condition;
    private final DBObject initial;
    private final String reduce;
    private final String finalize;

    public GroupCommand(final DBCollection input, final DBObject keys, final DBObject condition,
                        final DBObject initial, final String reduce, final String finalize) {
        this.input = input.getName();
        this.keys = keys;
        this.condition = condition;
        this.initial = initial;
        this.reduce = reduce;
        this.finalize = finalize;
    }

    public DBObject toDBObject() {
        DBObject args = new BasicDBObject("ns", input).append("key", keys)
                                                      .append("cond", condition)
                                                      .append("$reduce", reduce)
                                                      .append("initial", initial);
        if (finalize != null) {
            args.put("finalize", finalize);
        }
        return new BasicDBObject("group", args);
    }

    Group toNew(final DBObjectCodec codec) {
        Group group = new Group(keys == null ? null : new BsonDocumentWrapper<DBObject>(keys, codec),
                                reduce == null ? null : new BsonJavaScript(reduce),
                                initial == null ? null : new BsonDocumentWrapper<DBObject>(initial, codec));

        group.finalizeFunction(finalize == null ? null : new BsonJavaScript(finalize));
        group.filter(condition == null ? null : new BsonDocumentWrapper<DBObject>(condition, codec));

        return group;
    }

}
