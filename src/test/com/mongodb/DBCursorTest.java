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

public class DBCursorTest extends TestCase {

    public DBCursorTest()
        throws IOException , MongoException {
        super();
        _db = new Mongo( "127.0.0.1" , "cursortest" );
    }

    @Test(groups = {"basic"})
    public void testCount() {
        try {
            DBCollection c = _db.getCollection("test");
            c.drop();
            
            assertEquals(c.find().count(), 0);
        
            BasicDBObject obj = new BasicDBObject();
            obj.put("x", "foo");
            c.insert(obj);

            assertEquals(c.find().count(), 1);

            BasicDBObject query = new BasicDBObject();

            
            BasicDBObject fields = new BasicDBObject();
            fields.put("y", 1);
            assertEquals(c.find(query,fields).count(), 0);
            

            query.put("x", "bar");
            assertEquals(c.find(query).count(), 0);
        }
        catch (MongoException e) {
            assertTrue(false);
        }
    }

    @Test(groups = {"basic"})
    public void testSnapshot() {
        DBCollection c = _db.getCollection("foo");
        DBCursor cursor = c.find().snapshot().limit(4);
    }


    final DBBase _db;

    public static void main( String args[] )
        throws Exception {
        (new DBCursorTest()).runConsole();

    }

}
