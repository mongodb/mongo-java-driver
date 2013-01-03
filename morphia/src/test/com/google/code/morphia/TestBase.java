/**
 *
 */
package com.google.code.morphia;

import com.google.code.morphia.mapping.MappedClass;
import com.mongodb.DB;
import com.mongodb.Mongo;
import org.junit.After;
import org.junit.Before;

public abstract class TestBase {
    protected static Mongo mongo;
    protected static DB db;
    protected Datastore ds;
    protected AdvancedDatastore ads;
    protected Morphia morphia;

    protected TestBase() {
        try {
            if (mongo == null) {
                mongo = new Mongo();
                db = mongo.getDB("morphia_test");
                db.dropDatabase();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Before
    public void setUp() {
        morphia = new Morphia();
        ds = morphia.createDatastore(mongo, db.getName());
        ads = (AdvancedDatastore) ds;
    }

    protected void cleanup() {
        for (MappedClass mc : morphia.getMapper().getMappedClasses()) {
            db.getCollection(mc.getCollectionName()).drop();
        }
    }

    @After
    public void tearDown() {
        cleanup();
    }
}
