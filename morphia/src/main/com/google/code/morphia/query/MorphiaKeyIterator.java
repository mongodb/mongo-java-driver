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

package com.google.code.morphia.query;

import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author Scott Hernandez
 */
public class MorphiaKeyIterator<T> extends MorphiaIterator<T, Key<T>> {
    public MorphiaKeyIterator(final DBCursor cursor, final Mapper m, final Class<T> clazz, final String kind) {
        super(cursor, m, clazz, kind, null);
    }

    @Override
    protected Key<T> convertItem(final DBObject dbObj) {
        final Key<T> key = new Key<T>(kind, dbObj.get(Mapper.ID_KEY));
        key.setKindClass(this.clazz);
        return key;
    }

}