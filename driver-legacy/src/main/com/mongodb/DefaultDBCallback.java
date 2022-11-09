/*
 * Copyright 2008-present MongoDB, Inc.
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
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of DBCallback that decodes into a DBObject.
 */
public class DefaultDBCallback extends BasicBSONCallback implements DBCallback {

    private final DBObjectFactory objectFactory;

    /**
     * Creates a new DefaultDBCallback. If the Collection is null, it uses {@link DBCollectionObjectFactory} to create documents, otherwise
     * it uses the collection's object factory.
     *
     * @param collection an optionally null Collection that the documents created by this callback belong to.
     */
    public DefaultDBCallback(final DBCollection collection) {
        if (collection != null) {
            this.objectFactory = collection.getObjectFactory();
        } else {
            this.objectFactory = new DBCollectionObjectFactory();
        }
    }

    @Override
    public BSONObject create() {
        return objectFactory.getInstance();
    }

    @Override
    public BSONObject create(final boolean array, final List<String> path) {
        return array ? new BasicDBList() : objectFactory.getInstance(path != null ? path : Collections.emptyList());
    }

    @Override
    public void gotDBRef(final String name, final String namespace, final ObjectId id) {
        _put(name, new DBRef(namespace, id));
    }

    @Override
    public Object objectDone() {
        String name = curName();
        BSONObject document = (BSONObject) super.objectDone();
        if (!(document instanceof BasicBSONList)) {
            Iterator<String> iterator = document.keySet().iterator();
            if (iterator.hasNext() && iterator.next().equals("$ref") && iterator.hasNext() && iterator.next().equals("$id")) {
                _put(name, new DBRef((String) document.get("$db"), (String) document.get("$ref"), document.get("$id")));
            }
        }
        return document;
    }

    /**
     * The {@code DBCallbackFactory} for {@code DefaultDBCallback} instances.
     */
    public static final DBCallbackFactory FACTORY = new DBCallbackFactory() {
        @Override
        public DBCallback create(final DBCollection collection) {
            return new DefaultDBCallback(collection);
        }
    };
}
