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

package org.mongodb.impl;

import org.mongodb.Datastore;
import org.mongodb.MongoDatabase;
import org.mongodb.codecs.PrimitiveCodecs;

//This will change - I need a way to separate morphia-like functionality from standard Collection functionality
//I'm using morphia terminology at the moment, and will refactor when it becomes clearer what the correct
//approach is
public class PojoDatastore implements Datastore {
    private final MongoDatabase database;
    private final PrimitiveCodecs primitiveCodecs;

    public PojoDatastore(final MongoDatabase database, final PrimitiveCodecs primitiveCodecs) {
        this.database = database;
        this.primitiveCodecs = primitiveCodecs;
    }

    @Override
    public void insert(final Object object) {
//        final String collectionName = object.getClass().getSimpleName().toLowerCase();
//        final MongoCollection<Object> collection = database.getCollection(collectionName,
//                                                                          new PojoSerializer(primitiveCodecs));
//        collection.insert(object);
        throw new UnsupportedOperationException("Not implemented");
    }
}
