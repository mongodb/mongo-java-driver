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

package org.mongodb;

import java.io.Closeable;
import java.util.Iterator;

public class MongoCursor<T> implements Iterator<T>, Closeable {
    private final MongoClient mongoClient;
    private final MongoCollectionName namespace;
    private final MongoDocument query;
    private final MongoDocument fields;

    public MongoCursor(final MongoClient mongoClient, final MongoCollectionName namespace, final MongoDocument query,
                       final MongoDocument fields) {
        this.mongoClient = mongoClient;
        this.namespace = namespace;
        this.query = query;
        this.fields = fields;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public T next() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
