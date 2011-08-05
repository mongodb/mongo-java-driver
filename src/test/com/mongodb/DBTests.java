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

import org.testng.annotations.Test;

import com.mongodb.util.*;

/**
 *  Tests aspect of the DB - not really driver tests
 */
public class DBTests extends TestCase {

    final Mongo _mongo;
    final DB _db;

    public DBTests()
        throws Exception {
        _mongo = new Mongo();
	cleanupMongo = new Mongo( "127.0.0.1" );
	cleanupDB = "java_com_mongodb_unittest_DBTests";
	_db = cleanupMongo.getDB( cleanupDB );
    }

    @Test
    public void testGetCollectionNames() throws MongoException {
        String name = "testGetCollectionNames";
        DBCollection c = _db.getCollection( name );
        c.drop();
        assertFalse( _db.getCollectionNames().contains( name ) );
        c.save( new BasicDBObject( "x" , 1 ) );
        assertTrue( _db.getCollectionNames().contains( name ) );
        
    }


    @Test
    public void testRename() throws MongoException {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = _db.getCollection( namea );
        DBCollection b = _db.getCollection( nameb );
        
        a.drop();
        b.drop();

        assertEquals( 0 , a.find().count() );
        assertEquals( 0 , b.find().count() );

        a.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , a.find().count() );
        assertEquals( 0 , b.find().count() );
        
        DBCollection b2 = a.rename( nameb );
        assertEquals( 0 , a.find().count() );
        assertEquals( 1 , b.find().count() );
        assertEquals( 1 , b2.find().count() );
        
        assertEquals( b.getName() , b2.getName() );
        
    }

    @Test
    public void testRenameAndDrop() throws MongoException {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = _db.getCollection( namea );
        DBCollection b = _db.getCollection( nameb );

        a.drop();
        b.drop();

        assertEquals( 0 , a.find().count() );
        assertEquals( 0 , b.find().count() );

        a.save( new BasicDBObject( "x" , 1 ) );
        b.save( new BasicDBObject( "x" , 1 ) );
        assertEquals( 1 , a.find().count() );
        assertEquals( 1 , b.find().count() );

        try {
            DBCollection b2 = a.rename( nameb );
            assertTrue(false, "Rename to existing collection must fail");
        } catch (MongoException e) {
            assertEquals(e.getCode(), 10027);
        }

        DBCollection b2 = a.rename( nameb, true );
        assertEquals( 0 , a.find().count() );
        assertEquals( 1 , b.find().count() );
        assertEquals( 1 , b2.find().count() );

        assertEquals( b.getName() , b2.getName() );

    }
}
