// QueryTest.java

/**
 *      Copyright (C) 2010 10gen Inc.
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
import java.util.regex.Pattern;
import org.testng.annotations.*;

import com.mongodb.util.TestCase;

/**
 * Test for various methods of <code/>Query</code>
 * @author Wes Freeeman
 *
 */
public class QueryTest extends TestCase {
    private static TestDB _testDB;
	
    @BeforeClass
    public static void setup() {
        _testDB = new TestDB("queryTest");
    }
	
    @Test
    public void greaterThanTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("gt-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = Query.GT(key, -1);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.GT(key,0);
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void greaterThanEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("gte-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = Query.GTE(key,0);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryTrue2 = Query.GTE(key,-1);
        assertTrue(testQuery(collection, queryTrue2));
		
        DBObject queryFalse = Query.GTE(key,1);
        assertFalse(testQuery(collection, queryFalse));

    }

    @Test
    public void lessThanTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("lt-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = Query.LT(key,1);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.LT(key,0);
        assertFalse(testQuery(collection, queryFalse));

    }

    @Test
    public void lessThanEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("lte-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = Query.LTE(key,1);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryTrue2 = Query.LTE(key,0);
        assertTrue(testQuery(collection, queryTrue2));
		
        DBObject queryFalse = Query.LTE(key,-1);
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void equalsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("is-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = Query.EQ(key,"test");
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.EQ(key,"test1");
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void notEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("ne-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = Query.NE(key,"test1");
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.NE(key,"test");
        assertFalse(testQuery(collection, queryFalse));

    }
	
    @Test    
    public void inTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("in-test");
        saveTestDocument(collection, key, 1);
		
        DBObject queryTrue = Query.In(key,Arrays.asList(1, 2, 3));
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.In(key,Arrays.asList(2, 3, 4));
        assertFalse(testQuery(collection, queryFalse));
    }
	

    @Test
    public void notInTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("nin-test");
        saveTestDocument(collection, key, 1);

        DBObject queryTrue = Query.NotIn(key,Arrays.asList(2, 3, 4));
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.NotIn(key,Arrays.asList(1, 2, 3));
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void modTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("mod-test");
        saveTestDocument(collection, key, 9);
		
        DBObject queryTrue = Query.Mod(key, 2, 1);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.Mod(key, 2, 0);
        assertFalse(testQuery(collection, queryFalse));
    }	
	
    @Test
    public void allTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("all-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject query = Query.All(key,Arrays.asList(1, 2, 3));
        assertTrue(testQuery(collection, query));
		
        DBObject queryFalse = Query.All(key,Arrays.asList(2, 3, 4));
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void sizeTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("size-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject queryTrue = Query.Size(key,3);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.Size(key,4);
        assertFalse(testQuery(collection, queryFalse));
		
        DBObject queryFalse2 = Query.Size(key,2);
        assertFalse(testQuery(collection, queryFalse2));
    }

    @Test
    public void existsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("exists-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = Query.Exists(key,true);
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = Query.Exists(key, false);
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void regexTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("regex-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = Query.Regex(key,Pattern.compile("\\w*"));
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void rangeChainTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("range-test");
        saveTestDocument(collection, key, 2);
		
        DBObject queryTrue = Query.GT(key,0).LT(3);
        assertTrue(testQuery(collection, queryTrue));

        DBObject queryTrue2 = Query.LT(key,3).GT(0);
        assertTrue(testQuery(collection, queryTrue2));

        DBObject queryTrue3 = Query.LT(key,3).GTE(2);
        assertTrue(testQuery(collection, queryTrue3));

        DBObject queryTrue4 = Query.GT(key,0).LTE(2);
        assertTrue(testQuery(collection, queryTrue2));
    }
	
    @Test
    public void compoundChainTest() {
        String key = "x";
        String key2 = "y";
        String value = key;
        DBCollection collection = _testDB.getCollection("compound-test");
        DBObject testDocument = new BasicDBObject();
        testDocument.put(key, value);
        testDocument.put(key2, 9);
        collection.save(testDocument);
		
        DBObject queryTrue = Query.And(Query.EQ(key,value), Query.Mod(key2,2,1));
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void arrayChainTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("array-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject queryTrue = Query.All(key,Arrays.asList(1,2,3)).Size(3);
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void testOr() {
        DBCollection c = _testDB.getCollection( "or1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , 1 ) );
        c.insert( new BasicDBObject( "b" , 1 ) );
        
        DBObject q = Query.Or( Query.EQ( "a" , 1 ) , 
                               Query.EQ( "b" , 1 ) );
        assertEquals( 2 , c.find( q ).itcount() );
        DBObject q2 = Query.EQ( "a" , 1 ).or( Query.EQ( "b" , 1 ) );
        assertEquals( 2 , c.find( q2 ).itcount() );
    }

    @Test
    public void testAnd() {
        DBCollection c = _testDB.getCollection( "and1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , 1 ).append( "b" , 1) );
        c.insert( new BasicDBObject( "b" , 1 ) );
        
        DBObject q = Query.And( Query.EQ( "a" , 1 ) , 
                                Query.EQ( "b" , 1 ) );
        assertEquals( 1 , c.find( q ).itcount() );
        DBObject q2 = Query.EQ( "a" , 1 ).and( Query.EQ( "b" , 1 ) );
        assertEquals( 1 , c.find( q2 ).itcount() );
    }

    @Test
    public void testElemMatch() {
        DBCollection c = _testDB.getCollection( "elemmatch1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , Arrays.asList(new BasicDBObject("b",1).append("c",1), new BasicDBObject("c",1) ) ));
        c.insert( new BasicDBObject( "a" , 1 ) );
        
        DBObject q = Query.ElemMatch( "a", Query.And(Query.EQ("b",1),Query.EQ("c",1) ));
        assertEquals( 1 , c.find( q ).itcount() );
    }

    @Test
    public void testNear() {
        DBCollection c = _testDB.getCollection( "near" );
        c.drop();
        c.ensureIndex(new BasicDBObject("loc","2d"));
        c.insert( new BasicDBObject( "loc" , Arrays.asList(1.1,2.2) ));
        
        DBObject q = Query.Near( "loc", 1.2, 2.3 );
        assertEquals( 1 , c.find( q ).itcount() );
    }

    @Test
    public void testNearMaxDist() {
        DBCollection c = _testDB.getCollection( "near" );
        c.drop();
        c.ensureIndex(new BasicDBObject("loc","2d"));
        c.insert( new BasicDBObject( "loc" , Arrays.asList(1.1,2.2) ));
        
        DBObject q = Query.Near( "loc", 1.2, 2.3, 0.2 );
        assertEquals( 1 , c.find( q ).itcount() );
        DBObject q2 = Query.Near( "loc", 1.2, 2.3, 0.1 );
        assertEquals( 0 , c.find( q2 ).itcount() );
    }

    @Test
    public void testNearMaxDistSphere() {
        DBCollection c = _testDB.getCollection( "near" );
        c.drop();
        c.ensureIndex(new BasicDBObject("loc","2d"));
        c.insert( new BasicDBObject( "loc" , Arrays.asList(1.1,2.2) ));
        
        DBObject q = Query.Near( "loc", 1.2, 2.3, 0.2, true );
        assertEquals( 1 , c.find( q ).itcount() );
        DBObject q2 = Query.Near( "loc", 1.2, 2.3, 0.001, true );
        assertEquals( 0 , c.find( q2 ).itcount() );
    }

    @Test
    public void testComplex() {
        DBCollection c = _testDB.getCollection( "complex1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , 1 ).append( "b" , 2).append("d",4) );
        c.insert( new BasicDBObject( "b" , 2 ).append( "e" , 5) );
        c.insert( new BasicDBObject( "b" , 1 ) );
        c.insert( new BasicDBObject( "e" , 5 ) );
        c.insert( new BasicDBObject( "d" , 4 ) );
        c.insert( new BasicDBObject( "b" , 2 ) );
        
        DBObject q = Query.And(
            Query.Or( Query.EQ( "a" , 1 ) , 
                 Query.EQ( "b" , 2 ) ,
                 Query.EQ( "c" , 3 ) ),
            Query.Or( 
                     Query.EQ( "d" , 4 ), 
                     Query.EQ( "e" , 5 ) )
                );
        
        assertEquals( 2 , c.find( q ).itcount() );
    }
    
    @AfterClass
    public static void tearDown() {
        _testDB.cleanup();
    }
	
    /**
     * Convenience method that
     * creates a new MongoDB Document with a key-value pair and saves it inside the specified collection
     * @param collection Collection to save the new document to
     * @param key key of the field to be inserted to the new document
     * @param value value of the field to be inserted to the new document
     */
    private void saveTestDocument(DBCollection collection, String key, Object value) {
        DBObject testDocument = new BasicDBObject();
        testDocument.put(key, value);
        collection.save(testDocument);
    }
	
    private boolean testQuery(DBCollection collection, DBObject query) {
        DBCursor cursor = collection.find(query);
        return cursor.hasNext();
    }
		
}
