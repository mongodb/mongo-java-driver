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

package org.mongodb.operation;

import org.mongodb.Document;

public class MongoGroup {

    private final Document key;
    private final String keyf;
    private final String reduce;
    private final Document initial;
    private Document cond;
    private String finalize;

    private MongoGroup(final Document key, final String keyf, final String reduce, final Document initial) {
        this.keyf = keyf;
        this.key = key;
        this.reduce = reduce;
        this.initial = initial;
    }

    public MongoGroup(final Document key, final String reduce, final Document initial) {
        this(key, null, reduce, initial);
    }

    public MongoGroup(final String keyf, final String reduce, final Document initial) {
        this(null, keyf, reduce, initial);
    }

    public MongoGroup cond(final Document aCond) {
        this.cond = aCond;
        return this;
    }

    public MongoGroup finalize(final String finalizeFunction) {
        this.finalize = finalizeFunction;
        return this;
    }

    public Document getKey() {
        return key;
    }

    public Document getCond() {
        return cond;
    }

    public Document getInitial() {
        return initial;
    }

    public String getReduce() {
        return reduce;
    }

    public String getFinalize() {
        return finalize;
    }

    public String getKeyf() {
        return keyf;
    }
}
