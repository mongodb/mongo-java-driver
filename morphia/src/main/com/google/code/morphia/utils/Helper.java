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

package com.google.code.morphia.utils;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.MorphiaIterator;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.QueryImpl;
import com.google.code.morphia.query.UpdateOperations;
import com.google.code.morphia.query.UpdateOpsImpl;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Exposes driver related DBOBject stuff from Morphia objects
 *
 * @author scotthernandez
 */
@SuppressWarnings("rawtypes")
public final class Helper {
    private Helper() {
    }

    public static DBObject getCriteria(final Query q) {
        final QueryImpl qi = (QueryImpl) q;
        return qi.getQueryObject();
    }

    public static DBObject getSort(final Query q) {
        final QueryImpl qi = (QueryImpl) q;
        return qi.getSortObject();
    }

    public static DBObject getFields(final Query q) {
        final QueryImpl qi = (QueryImpl) q;
        return qi.getFieldsObject();
    }

    public static DBCollection getCollection(final Query q) {
        final QueryImpl qi = (QueryImpl) q;
        return qi.getCollection();
    }

    public static DBCursor getCursor(final Iterable it) {
        return ((MorphiaIterator) it).getCursor();
    }

    public static DBObject getUpdateOperations(final UpdateOperations ops) {
        final UpdateOpsImpl uo = (UpdateOpsImpl) ops;
        return uo.getOps();
    }

    public static DB getDB(final Datastore ds) {
        return ds.getDB();
    }
}