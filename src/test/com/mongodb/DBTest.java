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

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

public class DBTest extends TestCase {
    
    public DBTest() 
        throws UnknownHostException {
        super();
        _db = new Mongo( "127.0.0.1" , "dbbasetest" );
    }

    @Test(groups = {"basic"})
    public void testCreateCollection() {
        _db.getCollection( "foo1" ).drop();
        _db.getCollection( "foo2" ).drop();
        _db.getCollection( "foo3" ).drop();
        _db.getCollection( "foo4" ).drop();

        BasicDBObject o1 = new BasicDBObject("capped", false);
        DBCollection c = _db.createCollection("foo1", o1);

        DBObject o2 = BasicDBObjectBuilder.start().add("capped", true)
            .add("size", 100).get();
        c = _db.createCollection("foo2", o2);
        for (int i=0; i<30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertTrue(c.find().count() < 10);

        DBObject o3 = BasicDBObjectBuilder.start().add("capped", true)
            .add("size", 1000).add("max", 2).get();
        c = _db.createCollection("foo3", o3);
        for (int i=0; i<30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertEquals(c.find().count(), 2);

        try {
            DBObject o4 = BasicDBObjectBuilder.start().add("capped", true)
                .add("size", -20).get();
            c = _db.createCollection("foo4", o4);
        }
        catch(MongoException e) {
            return;
        }
        assertEquals(0, 1);
    }

    /*public static class Person extends DBObject {
        
        public Person(){

        }

        Person( String name ){
            _name = name;
        }

        public String getName(){
            return _name;
        }

        public void setName(String name){
            _name = name;
        }

        String _name;
    }

    public DBTest()
        throws IOException {
        _db = new Mongo( "127.0.0.1" , "dbbasetest" );        
    }
    

    @Test
    public void test1(){
        DBCollection c = _db.getCollection( "persen.test1" );
        c.drop();
        c.setObjectClass( Person.class );
        
        Person p = new Person( "eliot" );
        c.save( p );

        DBObject out = c.findOne();
        assertEquals( "eliot" , out.get( "Name" ) );
        assertTrue( out instanceof Person , "didn't come out as Person" );
    }
    */
    
    final Mongo _db;
    
    public static void main( String args[] )
        throws Exception {
        (new DBTest()).runConsole();
    }
}
