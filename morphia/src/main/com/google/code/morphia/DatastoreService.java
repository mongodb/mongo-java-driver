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

package com.google.code.morphia;

/**
 * @author Scott Hernnadez
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class DatastoreService {
    private static final Morphia MORPHIA;
    private static Datastore ds;

    static {
        MORPHIA = new Morphia();
        ds = MORPHIA.createDatastore("test");
    }

    private DatastoreService() {
    }

    /**
     * Connects to "test" database on localhost by default
     */
    public static Datastore getDatastore() {
        return ds;
    }

    public static void setDatabase(final String dbName) {
        if (!ds.getDB().getName().equals(dbName)) {
            ds = MORPHIA.createDatastore(dbName);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mapClass(final Class c) {
        MORPHIA.map(c);
    }

    @SuppressWarnings("unchecked")
    public static void mapClasses(final Class[] classes) {
        for (final Class c : classes) {
            mapClass(c);
        }
    }

    public static void mapPackage(final String pkg) {
        MORPHIA.mapPackage(pkg, true);
    }
}