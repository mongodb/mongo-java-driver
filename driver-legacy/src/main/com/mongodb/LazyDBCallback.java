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

import org.bson.LazyBSONCallback;
import org.bson.types.ObjectId;

import java.util.Iterator;
import java.util.List;

/**
 * A {@code BSONCallback} for the creation of {@code LazyDBObject} and {@code LazyDBList} instances.
 */
public class LazyDBCallback extends LazyBSONCallback implements DBCallback {

    /**
     * Construct an instance.
     *
     * @param collection the {@code DBCollection} containing the document.  This parameter is no longer used.
     */
    public LazyDBCallback(final DBCollection collection) {
    }

    @Override
    public Object createObject(final byte[] bytes, final int offset) {
        LazyDBObject document = new LazyDBObject(bytes, offset, this);
        Iterator<String> iterator = document.keySet().iterator();
        if (iterator.hasNext() && iterator.next().equals("$ref") && iterator.hasNext() && iterator.next().equals("$id")) {
            return new DBRef((String) document.get("$db"), (String) document.get("$ref"), document.get("$id"));
        }
        return document;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List createArray(final byte[] bytes, final int offset) {
        return new LazyDBList(bytes, offset, this);
    }

    @Override
    public Object createDBRef(final String ns, final ObjectId id) {
        return new DBRef(ns, id);
    }
}
