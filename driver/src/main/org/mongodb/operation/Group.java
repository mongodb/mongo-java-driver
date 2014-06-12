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

package org.mongodb.operation;

import org.bson.types.BsonDocument;
import org.bson.types.Code;

public class Group {

    private final BsonDocument key;
    private final Code keyFunction;
    private final Code reduceFunction;
    private final BsonDocument initial;
    private BsonDocument filter;
    private Code finalizeFunction;

    private Group(final BsonDocument key, final Code keyFunction, final Code reduceFunction, final BsonDocument initial) {
        if (initial == null) {
            throw new IllegalArgumentException("Group command requires an initial document for the aggregate result");
        }

        if (reduceFunction == null) {
            throw new IllegalArgumentException("Group command requires a reduce function for the aggregate result");
        }

        this.keyFunction = keyFunction;
        this.key = key;
        this.reduceFunction = reduceFunction;
        this.initial = initial;
    }

    public Group(final BsonDocument key, final Code reduceFunction, final BsonDocument initial) {
        this(key, null, reduceFunction, initial);
    }

    public Group(final Code keyFunction, final Code reduceFunction, final BsonDocument initial) {
        this(null, keyFunction, reduceFunction, initial);
    }

    public Group filter(final BsonDocument aCond) {
        this.filter = aCond;
        return this;
    }

    public Group finalizeFunction(final Code finalize) {
        this.finalizeFunction = finalize;
        return this;
    }

    public BsonDocument getKey() {
        return key;
    }

    public BsonDocument getFilter() {
        return filter;
    }

    public BsonDocument getInitial() {
        return initial;
    }

    public Code getReduceFunction() {
        return reduceFunction;
    }

    public Code getFinalizeFunction() {
        return finalizeFunction;
    }

    public Code getKeyFunction() {
        return keyFunction;
    }
}
