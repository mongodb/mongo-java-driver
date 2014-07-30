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

import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.types.ObjectId;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * TODO Documentation.
 */
public class DefaultDBCallback extends BasicBSONCallback implements DBCallback {

    private final DB db;
    private final DBObjectFactory objectFactory;

    public DefaultDBCallback(final DBCollection collection) {
        if (collection != null) {
            this.db = collection.getDB();
            this.objectFactory = collection.getObjectFactory();
        } else {
            this.db = null;
            this.objectFactory = new DBCollectionObjectFactory();
        }
    }

    @Override
    public BSONObject create() {
        return objectFactory.getInstance();
    }

    @Override
    public BSONObject create(final boolean array, final List<String> path) {
        return array ? new BasicDBList() : objectFactory.getInstance(path != null ? path : Collections.<String>emptyList());
    }

    @Override
    public void gotDBRef(final String name, final String ns, final ObjectId id) {
        _put(name, new DBRef(db, ns, id));
    }

    @Override
    public Object objectDone() {
        String name = curName();
        BSONObject document = (BSONObject) super.objectDone();
        Iterator<String> iterator = document.keySet().iterator();
        if (iterator.hasNext() && iterator.next().equals("$ref")
            && iterator.hasNext() && iterator.next().equals("$id")) {
            _put(name, new DBRef(db, document));
        }

        return document;
    }

    public static final DBCallbackFactory FACTORY = new DBCallbackFactory() {
        @Override
        public DBCallback create(final DBCollection collection) {
            return new DefaultDBCallback(collection);
        }
    };
}
