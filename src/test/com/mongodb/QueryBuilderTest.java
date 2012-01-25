// QueryBuilderTest.java

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

import com.mongodb.QueryBuilder.QueryBuilderException;
import com.mongodb.util.TestCase;

/**
 * Test for various methods of <code/>QueryBuilder</code>
 * @author Julson Lim
 *
 */
public class QueryBuilderTest extends TestCase {
    private static TestDB _testDB;
	
    @BeforeClass
    public static void setup() {
        _testDB = new TestDB("queryBuilderTest");
    }
	
    @Test
    public void greaterThanTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("gt-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = QueryBuilder.start(key).greaterThan(-1).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).greaterThan(0).get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void greaterThanEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("gte-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = QueryBuilder.start(key).greaterThanEquals(0).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryTrue2 = QueryBuilder.start(key).greaterThanEquals(-1).get();
        assertTrue(testQuery(collection, queryTrue2));
		
        DBObject queryFalse = QueryBuilder.start(key).greaterThanEquals(1).get();
        assertFalse(testQuery(collection, queryFalse));

    }
	
    @Test
    public void lessThanTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("lt-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = QueryBuilder.start(key).lessThan(1).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).lessThan(0).get();
        assertFalse(testQuery(collection, queryFalse));

    }
	
    @Test
    public void lessThanEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("lte-test");
        saveTestDocument(collection, key, 0);
		
        DBObject queryTrue = QueryBuilder.start(key).lessThanEquals(1).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryTrue2 = QueryBuilder.start(key).lessThanEquals(0).get();
        assertTrue(testQuery(collection, queryTrue2));
		
        DBObject queryFalse = QueryBuilder.start(key).lessThanEquals(-1).get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void isTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("is-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = QueryBuilder.start(key).is("test").get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).is("test1").get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void notEqualsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("ne-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = QueryBuilder.start(key).notEquals("test1").get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).notEquals("test").get();
        assertFalse(testQuery(collection, queryFalse));

    }
	
    @Test    
    public void inTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("in-test");
        saveTestDocument(collection, key, 1);
		
        DBObject queryTrue = QueryBuilder.start(key).in(Arrays.asList(1, 2, 3)).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).in(Arrays.asList(2, 3, 4)).get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void notInTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("nin-test");
        saveTestDocument(collection, key, 1);

        DBObject queryTrue = QueryBuilder.start(key).notIn(Arrays.asList(2, 3, 4)).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).notIn(Arrays.asList(1, 2, 3)).get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void modTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("mod-test");
        saveTestDocument(collection, key, 9);
		
        DBObject queryTrue = QueryBuilder.start(key).mod(Arrays.asList(2, 1)).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).mod(Arrays.asList(2, 0)).get();
        assertFalse(testQuery(collection, queryFalse));
    }	
	
    @Test
    public void allTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("all-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject query = QueryBuilder.start(key).all(Arrays.asList(1, 2, 3)).get();
        assertTrue(testQuery(collection, query));
		
        DBObject queryFalse = QueryBuilder.start(key).all(Arrays.asList(2, 3, 4)).get();
        assertFalse(testQuery(collection, queryFalse));
    }
	
    @Test
    public void sizeTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("size-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject queryTrue = QueryBuilder.start(key).size(3).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).size(4).get();
        assertFalse(testQuery(collection, queryFalse));
		
        DBObject queryFalse2 = QueryBuilder.start(key).size(2).get();
        assertFalse(testQuery(collection, queryFalse2));
    }

    @Test
    public void existsTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("exists-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = QueryBuilder.start(key).exists(true).get();
        assertTrue(testQuery(collection, queryTrue));
		
        DBObject queryFalse = QueryBuilder.start(key).exists(false).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void regexTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("regex-test");
        saveTestDocument(collection, key, "test");
		
        DBObject queryTrue = QueryBuilder.start(key).regex(Pattern.compile("\\w*")).get();
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void rangeChainTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("range-test");
        saveTestDocument(collection, key, 2);
		
        DBObject queryTrue = QueryBuilder.start(key).greaterThan(0).lessThan(3).get();
        assertTrue(testQuery(collection, queryTrue));
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
		
        DBObject queryTrue = QueryBuilder.start(key).is(value).and(key2).mod(Arrays.asList(2,1)).get();
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void arrayChainTest() {
        String key = "x";
        DBCollection collection = _testDB.getCollection("array-test");
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));
		
        DBObject queryTrue = QueryBuilder.start(key).all(Arrays.asList(1,2,3)).size(3).get();
        assertTrue(testQuery(collection, queryTrue));
    }
	
    @Test
    public void failureTest() {
        boolean thrown = false;
        try {
            QueryBuilder.start("x").get();
        } catch(QueryBuilderException e) {
            thrown = true;
        }
        assertTrue(thrown);
		
        boolean thrown2 = false;
        try {
            QueryBuilder.start("x").exists(true).and("y").get();
        } catch(QueryBuilderException e) {
            thrown2 = true;
        }
        assertTrue(thrown2);
		
        boolean thrown3 = false;
        try {
            QueryBuilder.start("x").and("y").get();
        } catch(QueryBuilderException e) {
            thrown3 = true;
        }
        assertTrue(thrown3);
    }

    @Test
    public void testOr() {
        DBCollection c = _testDB.getCollection( "or1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , 1 ) );
        c.insert( new BasicDBObject( "b" , 1 ) );
        
        DBObject q = QueryBuilder.start()
            .or( new BasicDBObject( "a" , 1 ) , 
                 new BasicDBObject( "b" , 1 ) )
            .get();
        
        assertEquals( 2 , c.find( q ).itcount() );
    }

    @Test
    public void testAnd() {
        DBCollection c = _testDB.getCollection( "and1" );
        c.drop();
        c.insert( new BasicDBObject( "a" , 1 ).append( "b" , 1) );
        c.insert( new BasicDBObject( "b" , 1 ) );
        
        DBObject q = QueryBuilder.start()
            .and( new BasicDBObject( "a" , 1 ) , 
                  new BasicDBObject( "b" , 1 ) )
            .get();
        
        assertEquals( 1 , c.find( q ).itcount() );
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
