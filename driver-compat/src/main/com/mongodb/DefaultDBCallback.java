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

package com.mongodb;

import org.bson.BSONObject;
import org.bson.BasicBSONCallback;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO Documentation. Should be deprecated.
 */
public class DefaultDBCallback extends BasicBSONCallback implements DBCallback {

    private final DB db;
    private final DBObjectFactory objectFactory;

    public DefaultDBCallback(final DBCollection collection) {
        this.db = collection.getDB();
        this.objectFactory = collection.getObjectFactory();
    }

    @Override
    public BSONObject create() {
        return objectFactory.getInstance();
    }

    @Override
    public BSONObject create(final boolean array, final List<String> path) {
        return array ? new BasicDBList() : objectFactory.getInstance(path != null ? path : new ArrayList<String>());
    }

    @Override
    public Object objectDone() {
        final String name = curName();
        BSONObject document = (BSONObject) super.objectDone();
        if (document.containsField("$ref") && document.containsField("$id")) {
            _put(name, new DBRef(db, document));
        }

        return document;
    }

    public static DBCallbackFactory FACTORY = new DBCallbackFactory() {
        @Override
        public DBCallback create(final DBCollection collection) {
            return new DefaultDBCallback(collection);
        }
    };
}
