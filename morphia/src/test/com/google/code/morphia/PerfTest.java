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

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Date;

import static org.junit.Assert.assertTrue;

/**
 * @author Scott Hernandez
 */
public class PerfTest extends TestBase {
    private static final double WRITE_FAIL_FACTOR = 1.10;
    //    private static final double READ_FAIL_FACTOR = 1.75;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    @SuppressWarnings("UnusedDeclaration")
    @Entity
    static class Address {
        @Id
        private ObjectId id;
        private String name = "Scott";
        private String street = "3400 Maple";
        private String city = "Manhattan Beach";
        private String state = "CA";
        private int zip = 90266;
        private Date added = new Date();

        public Address() {
        }

        public Address(final BasicDBObject dbObj) {
            name = dbObj.getString("name");
            street = dbObj.getString("street");
            city = dbObj.getString("city");
            state = dbObj.getString("state");
            zip = dbObj.getInt("zip");
            added = (Date) dbObj.get("added");
        }

        public DBObject toDBObject() {
            final DBObject dbObj = new BasicDBObject();
            dbObj.put("name", name);
            dbObj.put("street", street);
            dbObj.put("city", city);
            dbObj.put("state", state);
            dbObj.put("zip", zip);
            dbObj.put("added", new Date());
            return dbObj;
        }
    }

    @Test
    @Ignore
    public void testAddressInsertPerf() throws Exception {
        final int count = 10000;
        final boolean strict = true;
        long startTicks = new Date().getTime();
        insertAddresses(count, true, strict);
        long endTicks = new Date().getTime();
        final long rawInsertTime = endTicks - startTicks;

        ds.delete(ds.find(Address.class));
        startTicks = new Date().getTime();
        insertAddresses(count, false, strict);
        endTicks = new Date().getTime();
        final long insertTime = endTicks - startTicks;

        final String msg = String.format("Insert (%s) performance is too slow: %sX slower (%s/%s)", count,
                                        DECIMAL_FORMAT.format((double) insertTime / rawInsertTime), insertTime,
                                        rawInsertTime);
        assertTrue(msg, insertTime < (rawInsertTime * WRITE_FAIL_FACTOR));
    }


    public void insertAddresses(final int count, final boolean raw, final boolean strict) {
        final DBCollection dbColl = db.getCollection(((DatastoreImpl) ds).getMapper().getCollectionName(Address.class));

        for (int i = 0; i < count; i++) {
            final Address addr = new Address();
            if (raw) {
                final DBObject dbObj = addr.toDBObject();
                if (strict) {
                    dbColl.save(dbObj, com.mongodb.WriteConcern.SAFE);
                }
                else {
                    dbColl.save(dbObj, com.mongodb.WriteConcern.NORMAL);
                }
            }
            else {
                if (strict) {
                    ds.save(addr, com.mongodb.WriteConcern.SAFE);
                }
                else {
                    ds.save(addr, com.mongodb.WriteConcern.NORMAL);
                }
            }
        }
    }

    @Entity(value = "imageMeta", noClassnameStored = true)
    @SuppressWarnings("UnusedDeclaration")
    static class TestObj {
        @Id
        private ObjectId id = new ObjectId();
        private long var1;
        private long var2;
    }

/*
    @Test
    public void testDifference() throws UnknownHostException {
        final Morphia morphia = new Morphia();
        morphia.map(TestObj.class);
        final AdvancedDatastore ds = (AdvancedDatastore) morphia.createDatastore(mongo, "my_database");
        //create the list
        List<TestObj> objList = new ArrayList<TestObj>();
        for (int i = 0; i < 1000; i++) {
            final TestObj obj = new TestObj();
            obj.id = new ObjectId();
            obj.var1 = 3345345L + i;
            obj.var2 = 6785678L + i;
            objList.add(obj);
        }

        long start = System.currentTimeMillis();
        for (final TestObj to : objList) {
            ds.insert(to, WriteConcern.SAFE);
        }
        System.out.println("Time taken morphia: " + (System.currentTimeMillis() - start) + "ms");

        final Mongo mongoConn = new Mongo();
        final DB mongoDB = mongoConn.getDB("my_database");
        List<DBObject> batchPush = new ArrayList<DBObject>();
        for (int i = 0; i < 1000; i++) {
            final DBObject doc = new BasicDBObject();
            doc.put("_id", new ObjectId());
            doc.put("var1", 3345345L + i);
            doc.put("var2", 6785678L + i);
            batchPush.add(doc);
        }
        final DBCollection c = mongoDB.getCollection("imageMeta2");
        c.setWriteConcern(WriteConcern.SAFE);
        start = System.currentTimeMillis();
        for (final DBObject doc : batchPush) {
            c.insert(doc);
        }
        System.out.println("Time taken regular: " + (System.currentTimeMillis() - start) + "ms");

        objList = new ArrayList<TestObj>();
        for (int i = 0; i < 1000; i++) {
            final TestObj obj = new TestObj();
            obj.id = new ObjectId();
            obj.var1 = 3345345L + i;
            obj.var2 = 6785678L + i;
            objList.add(obj);
        }

        start = System.currentTimeMillis();
        ds.insert(objList, WriteConcern.SAFE);
        System.out.println("Time taken batch morphia: " + (System.currentTimeMillis() - start) + "ms");


        batchPush = new ArrayList<DBObject>();
        for (int i = 0; i < 1000; i++) {
            final DBObject doc = new BasicDBObject();
            doc.put("_id", new ObjectId());
            doc.put("var1", 3345345L + i);
            doc.put("var2", 6785678L + i);
            batchPush.add(doc);
        }

        start = System.currentTimeMillis();
        c.insert(batchPush);
        System.out.println("Time taken batch regular: " + (System.currentTimeMillis() - start) + "ms");
    }
*/
}
