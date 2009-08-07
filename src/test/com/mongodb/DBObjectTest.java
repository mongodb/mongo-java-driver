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

import java.net.*;
import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class DBObjectTest extends TestCase {

    @Test(groups = {"basic"})
    public void testBasicDBObjectCTOR()  {
        Map m = new HashMap();
        m.put("key", "value");
        m.put("foo", 1);
        m.put("bar", null);

        DBObject obj = new BasicDBObject(m);
        assertEquals(obj.get("key"), "value");
        assertEquals(obj.get("foo"), 1);
        assertEquals(obj.get("bar"), null);
    }

    @Test(groups = {"basic"})
    public void testBasicDBObjectToString()  {
        Map m = new HashMap();
        m.put("key", new DBPointer("foo", new ObjectId("123456789012123456789012")));

        DBObject obj = new BasicDBObject(m);
        assertEquals(obj.get("key").toString(), "{ \"$ref\" : \"foo\", \"$id\" : ObjectId(\"123456789012123456789012\") }");
    }

    @Test(groups = {"basic"})
    public void testDBObjectBuilder() {
        Map m = new HashMap();
        m.put("foo", "bar");

        BasicDBObjectBuilder b = BasicDBObjectBuilder.start(m);
        b.add("bar", "baz");

        DBObject obj = b.get();
        assertEquals(obj.get("foo"), "bar");
        assertEquals(obj.get("bar"), "baz");

    }

    @Test(groups = {"basic"})
    public void testToMap() {
        Map m = BasicDBObjectBuilder.start().add("y", "z").add("z","a").get().toMap();
        assertEquals(m.get("y"), "z");
        assertEquals(m.get("z"), "a");
    }

    @Test(groups = {"basic"})
    public void testBasicDBList() {
        BasicDBList l = new BasicDBList();
        l.put(10, "x");
        assertEquals(l.get("10"), "x");
        assertEquals(l.get(3), null);
        l.put("10", "y");
        assertEquals(l.get("10"), "y");

        Mongo db;
        try {
            db = new Mongo( "127.0.0.1" , "test" );
        } 
        catch (UnknownHostException e2) {
            return;
        }
        DBCollection c = db.getCollection("dblist");
        c.drop();
        c.insert(BasicDBObjectBuilder.start().add("array", l).get());
        DBObject obj = c.findOne();
        assertEquals(obj.get("array") instanceof List, true);
    }

    @Test(groups = {"basic"})
    public void testPutAll() {
        DBObject start = BasicDBObjectBuilder.start().add( "a" , 1 ).add( "b" , 2 ).get();

        assertEquals( 1 , start.get( "a" ) );

        BasicDBObject next = new BasicDBObject();
        next.put( "a" , 3 );
        assertEquals( 3 , next.get( "a" ) );
        next.putAll( start );
        assertEquals( 2 , next.get( "b" ) );
        assertEquals( 1 , next.get( "a" ) );

    }

    @Test(groups = {"basic"})
    public void testRemoveField() {
        BasicDBObject obj = new BasicDBObject();
        obj.put("x", "y");
        obj.put("y", "z");

        assertTrue(obj.containsKey("x"));
        assertTrue(obj.containsKey("y"));
        assertEquals(obj.toString(), "{ \"x\" : \"y\" , \"y\" : \"z\"}");

        obj.removeField("x");

        assertFalse(obj.containsKey("x"));
        assertTrue(obj.containsKey("y"));
        assertEquals(obj.toString(), "{ \"y\" : \"z\"}");

        obj.put("x", "y");

        assertTrue(obj.containsKey("x"));
        assertTrue(obj.containsKey("y"));
        assertEquals(obj.toString(), "{ \"y\" : \"z\" , \"x\" : \"y\"}");
    }
    
    public static void main( String args[] ) {
        (new DBObjectTest()).runConsole();
    }
}

