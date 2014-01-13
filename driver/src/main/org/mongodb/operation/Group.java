/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.bson.types.Code;
import org.mongodb.Document;

public class Group {

    private final Document key;
    private final Code keyFunction;
    private final Code reduceFunction;
    private final Document initial;
    private Document filter;
    private Code finalizeFunction;

    private Group(final Document key, final Code keyFunction, final Code reduceFunction, final Document initial) {
        this.keyFunction = keyFunction;
        this.key = key;
        this.reduceFunction = reduceFunction;
        this.initial = initial;
    }

    public Group(final Document key, final Code reduceFunction, final Document initial) {
        this(key, null, reduceFunction, initial);
    }

    public Group(final Code keyFunction, final Code reduceFunction, final Document initial) {
        this(null, keyFunction, reduceFunction, initial);
    }

    public Group filter(final Document aCond) {
        this.filter = aCond;
        return this;
    }

    public Group finalizeFunction(final Code finalize) {
        this.finalizeFunction = finalize;
        return this;
    }

    public Document getKey() {
        return key;
    }

    public Document getFilter() {
        return filter;
    }

    public Document getInitial() {
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
