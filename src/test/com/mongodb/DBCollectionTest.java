/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import java.util.*;
import java.util.regex.*;
import java.io.IOException;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class DBCollectionTest extends TestCase {

    public DBCollectionTest()
        throws IOException , MongoException {
        super();
        _db = new Mongo( "127.0.0.1" , "cursortest" );
    }

    @Test(groups = {"basic"})
    public void testFindOne() {
        DBCollection c = _db.getCollection("test");
        c.drop();
        
        DBObject obj = c.findOne();
        assertEquals(obj, null);

        DBObject inserted = BasicDBObjectBuilder.start().add("x",1).add("y",2).get();
        c.insert(inserted);
        c.insert(BasicDBObjectBuilder.start().add("_id", 123).add("x",2).add("z",2).get());

        obj = c.findOne(123);
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.get("z"), 2);

        obj = c.findOne(123, new BasicDBObject("x", 1));
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.containsField("z"), false);

        obj = c.findOne(new BasicDBObject("x", 1));
        assertEquals(obj.get("x"), 1);
        assertEquals(obj.get("y"), 2);

        obj = c.findOne(new BasicDBObject("x", 1), new BasicDBObject("y", 1));
        assertEquals(obj.containsField("x"), false);
        assertEquals(obj.get("y"), 2);
    }

    final DBBase _db;

    public static void main( String args[] )
        throws Exception {
        (new DBCollectionTest()).runConsole();
    }

}
