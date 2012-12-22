/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author Scott Hernandez
 */
//@Ignore
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestPerf  extends TestBase{
	static double WriteFailFactor = 1.10;
	static double ReadFailFactor = 1.75;
	static DecimalFormat DF = new DecimalFormat("#.##");
	@Entity
	public static class Address {
		@Id ObjectId id;
		String name = "Scott";
		String street = "3400 Maple";
		String city = "Manhattan Beach";
		String state = "CA";
		int zip = 90266;
		Date added = new Date();

		public Address() {}

		public Address(BasicDBObject dbObj) {
			name = dbObj.getString("name");
			street = dbObj.getString("street");
			city = dbObj.getString("city");
			state = dbObj.getString("state");
			zip = dbObj.getInt("zip");
			added = (Date) dbObj.get("added");
		}

		public DBObject toDBObject() {
			DBObject dbObj = new BasicDBObject();
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
    	int count = 10000;
    	boolean strict = true;
    	long startTicks = new Date().getTime();
    	insertAddresses(count, true, strict);
    	long endTicks = new Date().getTime();
    	long rawInsertTime = endTicks - startTicks;

    	ds.delete(ds.find(Address.class));
    	startTicks = new Date().getTime();
    	insertAddresses(count, false, strict);
    	endTicks = new Date().getTime();
    	long insertTime = endTicks - startTicks;

    	String msg = String.format("Insert (%s) performance is too slow: %sX slower (%s/%s)",
    							count,
    							DF.format((double)insertTime/rawInsertTime),
    							insertTime,
    							rawInsertTime);
    	assertTrue(msg, insertTime < (rawInsertTime * WriteFailFactor));
    }


	public void insertAddresses(int count, boolean raw, boolean strict) {
    	DBCollection dbColl = db.getCollection(((DatastoreImpl)ds).getMapper().getCollectionName(Address.class));

    	for(int i=0;i<count;i++) {
			Address addr = new Address();
    		if(raw) {
    			DBObject dbObj = addr.toDBObject();
    			if (strict)
    				dbColl.save(dbObj, com.mongodb.WriteConcern.SAFE);
    			else
    				dbColl.save(dbObj, com.mongodb.WriteConcern.NORMAL);
    		}else {
    			if (strict)
    				ds.save(addr, com.mongodb.WriteConcern.SAFE);
    			else
    				ds.save(addr, com.mongodb.WriteConcern.NORMAL);
    		}
    	}
    }

	@Entity(value="imageMeta", noClassnameStored=true)
	public static class TestObj {
		@Id public ObjectId id = new ObjectId();
		public long var1;
		public long var2;
	}

	@Test
	public void testDifference() {
        try {
                Morphia morphia = new Morphia();
                morphia.map(TestObj.class);
                AdvancedDatastore ds = (AdvancedDatastore) morphia.createDatastore("my_database");
                //create the list
                List<TestObj> objList = new ArrayList<TestObj>();
                for (int i=0; i<1000; i++){
                        TestObj obj = new TestObj();
                        obj.id = new ObjectId();
                        obj.var1 = 3345345l+i;
                        obj.var2 = 6785678l+i;
                        objList.add(obj);
                }

                long start = System.currentTimeMillis();
                for(TestObj to : objList)
                	ds.insert(to, WriteConcern.SAFE);
                System.out.println("Time taken morphia: "+(System.currentTimeMillis()-start)+"ms");

                Mongo mongoConn = new Mongo();
                DB mongoDB = mongoConn.getDB("my_database");
                List<DBObject> batchPush = new ArrayList<DBObject>();
                for (int i=0; i<1000; i++){
                	DBObject doc = new BasicDBObject();
                    doc.put("_id", new ObjectId());
                    doc.put("var1", 3345345l+i);
                    doc.put("var2", 6785678l+i);
                    batchPush.add(doc);
                }
                DBCollection c = mongoDB.getCollection("imageMeta2");
                c.setWriteConcern(WriteConcern.SAFE);
                start = System.currentTimeMillis();
                for (DBObject doc : batchPush)
                	c.insert(doc);
                System.out.println("Time taken regular: "+(System.currentTimeMillis()-start)+"ms");

                objList = new ArrayList<TestObj>();
                for (int i=0; i<1000; i++){
                        TestObj obj = new TestObj();
                        obj.id = new ObjectId();
                        obj.var1 = 3345345l+i;
                        obj.var2 = 6785678l+i;
                        objList.add(obj);
                }

                start = System.currentTimeMillis();
                ds.insert(objList, WriteConcern.SAFE);
                System.out.println("Time taken batch morphia: "+(System.currentTimeMillis()-start)+"ms");


                batchPush = new ArrayList<DBObject>();
                for (int i=0; i<1000; i++){
                	DBObject doc = new BasicDBObject();
                    doc.put("_id", new ObjectId());
                    doc.put("var1", 3345345l+i);
                    doc.put("var2", 6785678l+i);
                    batchPush.add(doc);
                }

                start = System.currentTimeMillis();
                c.insert(batchPush);
                System.out.println("Time taken batch regular: "+(System.currentTimeMillis()-start)+"ms");
        }catch(Exception ex){
        	ex.printStackTrace();
        }

	}
}
