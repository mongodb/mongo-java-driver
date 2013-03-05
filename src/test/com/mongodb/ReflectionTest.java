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

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

public class ReflectionTest extends TestCase {
    
    public static class Person extends ReflectionDBObject {
        
        public Person(){
        }

        public String getName(){
            return _name;
        }

        public void setName(String name){
            _name = name;
        }

        String _name;
    }

    public ReflectionTest() {
	cleanupDB = "com_mongodb_unittest_ReflectionTest";
	_db = cleanupMongo.getDB( cleanupDB );
    }    

    @Test
    public void test1()
        throws MongoException {
        DBCollection c = _db.getCollection( "person.test1" );
        c.drop();
        c.setObjectClass( Person.class );
        
        Person p = new Person();
        p.setName( "eliot" );
        c.save( p );

        DBObject out = c.findOne();
        assertEquals( "eliot" , out.get( "Name" ) );
        assertEquals(Person.class, out.getClass());
    }
    
    public static class Outer extends ReflectionDBObject {
        private Inner mInclude;
        private String mName;

        public void setName(final String pName) { mName = pName; }
        public String getName() { return mName; }
        
        public Inner getInner() { return mInclude; }
        public void setInner(final Inner pV) { mInclude = pV; }
    }

    public static class Inner extends ReflectionDBObject {
        
        public int mNumber;
        
        public Inner(){}
        public Inner( int n ){ mNumber = n; }

        public int getNumber() { return mNumber; }
        public void setNumber(final int pV) { mNumber = pV; }
    }
    
    @Test
    public void test2()
        throws MongoException {

        DBCollection c = _db.getCollection( "embedref1" );
        c.drop();
        c.setObjectClass( Outer.class );

        Outer o = new Outer();
        o.setName( "eliot" );
        o.setInner( new Inner( 17 ) );

        c.save( o );
        
        DBObject out = c.findOne();
        assertEquals( "eliot" , out.get( "Name" ) );
        assertEquals(Outer.class, out.getClass());
        o = (Outer)out;
        assertEquals( "eliot" , o.getName() );
        assertEquals( 17 , o.getInner().getNumber() );
    }

    static class Process extends ReflectionDBObject {

        public Process() {}

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        String name;
        int status;
    }

    @Test
    public void testFindAndModify() {
        DBCollection c = _db.getCollection( "findAndModify" );
        c.drop();
        c.setObjectClass( Process.class );

        Process p = new Process();
        p.setName("test");
        p.setStatus(0);
        c.save(p, WriteConcern.SAFE);

        DBObject obj = c.findAndModify(new BasicDBObject(), new BasicDBObject("$set", new BasicDBObject("status", 1)));
        assertEquals(Process.class, obj.getClass());
        Process pModified = (Process) obj;
        assertEquals(0, pModified.getStatus());
        assertEquals("test", pModified.getName());
    }

    final DB _db;
    
    public static void main( String args[] )
        throws Exception {
        (new ReflectionTest()).runConsole();
    }

}
