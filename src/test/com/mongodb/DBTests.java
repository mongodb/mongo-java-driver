package com.mongodb;

import java.util.*;

import org.testng.annotations.Test;

import com.mongodb.util.*;

/**
 *  Tests aspect of the DB - not really driver tests
 */
public class DBTests extends MyAsserts {

    final Mongo _mongo;
    final DB _db;

    public DBTests()
        throws Exception {
        _mongo = new Mongo();
        _db = _mongo.getDB( "java_com_mongodb_DBTests" );
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
        
        a.rename( nameb );
        assertEquals( 0 , a.find().count() );
        assertEquals( 1 , b.find().count() );
        
    }
}
