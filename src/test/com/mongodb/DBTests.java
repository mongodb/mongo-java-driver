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

}
