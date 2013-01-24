/*
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
 */

/**
 *
 */
package com.google.code.morphia;

import com.google.code.morphia.mapping.MappedClass;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.junit.After;
import org.junit.Before;

public abstract class TestBase {
    public static final String DEFAULT_DB_NAME = "morphia-test";
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    protected static Mongo mongo;
    protected static DB db;

    protected Datastore ds;
    protected AdvancedDatastore ads;
    protected Morphia morphia;

    protected TestBase() {
        synchronized (TestBase.class) {
            if (mongo == null) {
                String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
                final String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty() ?
                        DEFAULT_URI : mongoURIProperty;
                System.out.println("URI: " + mongoURIString);
                try {
                    mongo = new MongoClient(new MongoClientURI(mongoURIString));
                    db = mongo.getDB(DEFAULT_DB_NAME);
                    db.dropDatabase();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURIString, e);
                }
            }
        }
    }

    @Before
    public void setUp() {
        morphia = new Morphia();
        ds = morphia.createDatastore(mongo, db.getName());
        ads = (AdvancedDatastore) ds;
    }

    protected void cleanup() {
        for (final MappedClass mc : morphia.getMapper().getMappedClasses()) {
            db.getCollection(mc.getCollectionName()).drop();
        }
    }

    @After
    public void tearDown() {
        cleanup();
    }
}
