/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;
import com.mongodb.QueryBuilder.QueryBuilderException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryBuilderTest extends DatabaseTestCase {

    @Test
    public void elemMatchTest() {
        final DBObject query = QueryBuilder.start("array").elemMatch(
                                                              QueryBuilder.start("x").is(1).and("y").is(2).get()).get();
        final DBObject expected = new BasicDBObject("array", new BasicDBObject("$elemMatch",
                                                                        new BasicDBObject("x", 1).append("y", 2)));
        assertEquals(expected, query);
        // TODO: add integration test
    }

    @Test
    public void notTest() {
        final Pattern pattern = Pattern.compile("\\w*");
        DBObject query = QueryBuilder.start("x").not().regex(pattern).get();
        DBObject expected = new BasicDBObject("x", new BasicDBObject("$not", pattern));
        assertEquals(expected, query);

        query = QueryBuilder.start("x").not().regex(pattern).and("y").is("foo").get();
        expected = new BasicDBObject("x", new BasicDBObject("$not", pattern)).append("y", "foo");
        assertEquals(expected, query);

        query = QueryBuilder.start("x").not().greaterThan(2).get();
        expected = new BasicDBObject("x", new BasicDBObject("$not", new BasicDBObject("$gt", 2)));
        assertEquals(expected, query);

        query = QueryBuilder.start("x").not().greaterThan(2).and("y").is("foo").get();
        expected = new BasicDBObject("x", new BasicDBObject("$not", new BasicDBObject("$gt", 2))).append("y", "foo");
        assertEquals(expected, query);


        query = QueryBuilder.start("x").not().greaterThan(2).lessThan(0).get();
        expected = new BasicDBObject("x", new BasicDBObject("$not", new BasicDBObject("$gt", 2).append("$lt", 0)));
        assertEquals(expected, query);

    }

    @Test
    public void greaterThanTest() {
        final String key = "x";
        saveTestDocument(collection, key, 0);

        final DBObject queryTrue = QueryBuilder.start(key).greaterThan(-1).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).greaterThan(0).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void greaterThanEqualsTest() {
        final String key = "x";
        saveTestDocument(collection, key, 0);

        final DBObject queryTrue = QueryBuilder.start(key).greaterThanEquals(0).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryTrue2 = QueryBuilder.start(key).greaterThanEquals(-1).get();
        assertTrue(testQuery(collection, queryTrue2));

        final DBObject queryFalse = QueryBuilder.start(key).greaterThanEquals(1).get();
        assertFalse(testQuery(collection, queryFalse));

    }

    @Test
    public void lessThanTest() {
        final String key = "x";
        saveTestDocument(collection, key, 0);

        final DBObject queryTrue = QueryBuilder.start(key).lessThan(1).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).lessThan(0).get();
        assertFalse(testQuery(collection, queryFalse));

    }

    @Test
    public void lessThanEqualsTest() {
        final String key = "x";
        saveTestDocument(collection, key, 0);

        final DBObject queryTrue = QueryBuilder.start(key).lessThanEquals(1).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryTrue2 = QueryBuilder.start(key).lessThanEquals(0).get();
        assertTrue(testQuery(collection, queryTrue2));

        final DBObject queryFalse = QueryBuilder.start(key).lessThanEquals(-1).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void isTest() {
        final String key = "x";
        saveTestDocument(collection, key, "test");

        final DBObject queryTrue = QueryBuilder.start(key).is("test").get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).is("test1").get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void notEqualsTest() {
        final String key = "x";
        saveTestDocument(collection, key, "test");

        final DBObject queryTrue = QueryBuilder.start(key).notEquals("test1").get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).notEquals("test").get();
        assertFalse(testQuery(collection, queryFalse));

    }

    @Test
    public void inTest() {
        final String key = "x";
        saveTestDocument(collection, key, 1);

        final DBObject queryTrue = QueryBuilder.start(key).in(Arrays.asList(1, 2, 3)).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).in(Arrays.asList(2, 3, 4)).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void notInTest() {
        final String key = "x";
        saveTestDocument(collection, key, 1);

        final DBObject queryTrue = QueryBuilder.start(key).notIn(Arrays.asList(2, 3, 4)).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).notIn(Arrays.asList(1, 2, 3)).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void modTest() {
        final String key = "x";
        saveTestDocument(collection, key, 9);

        final DBObject queryTrue = QueryBuilder.start(key).mod(Arrays.asList(2, 1)).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).mod(Arrays.asList(2, 0)).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void allTest() {
        final String key = "x";
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));

        final DBObject query = QueryBuilder.start(key).all(Arrays.asList(1, 2, 3)).get();
        assertTrue(testQuery(collection, query));

        final DBObject queryFalse = QueryBuilder.start(key).all(Arrays.asList(2, 3, 4)).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void sizeTest() {
        final String key = "x";
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));

        final DBObject queryTrue = QueryBuilder.start(key).size(3).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).size(4).get();
        assertFalse(testQuery(collection, queryFalse));

        final DBObject queryFalse2 = QueryBuilder.start(key).size(2).get();
        assertFalse(testQuery(collection, queryFalse2));
    }

    @Test
    public void existsTest() {
        final String key = "x";
        saveTestDocument(collection, key, "test");

        final DBObject queryTrue = QueryBuilder.start(key).exists(true).get();
        assertTrue(testQuery(collection, queryTrue));

        final DBObject queryFalse = QueryBuilder.start(key).exists(false).get();
        assertFalse(testQuery(collection, queryFalse));
    }

    @Test
    public void regexTest() {
        final String key = "x";
        saveTestDocument(collection, key, "test");

        final DBObject queryTrue = QueryBuilder.start(key).regex(Pattern.compile("\\w*")).get();
        assertTrue(testQuery(collection, queryTrue));
    }

    @Test
    public void rangeChainTest() {
        final String key = "x";
        saveTestDocument(collection, key, 2);

        final DBObject queryTrue = QueryBuilder.start(key).greaterThan(0).lessThan(3).get();
        assertTrue(testQuery(collection, queryTrue));
    }

    @Test
    public void compoundChainTest() {
        final String key = "x";
        final String key2 = "y";
        final String value = key;
        final DBObject testDocument = new BasicDBObject();
        testDocument.put(key, value);
        testDocument.put(key2, 9);
        collection.save(testDocument);

        final DBObject queryTrue = QueryBuilder.start(key).is(value).and(key2).mod(Arrays.asList(2, 1)).get();
        assertTrue(testQuery(collection, queryTrue));
    }

    @Test
    public void arrayChainTest() {
        final String key = "x";
        saveTestDocument(collection, key, Arrays.asList(1, 2, 3));

        final DBObject queryTrue = QueryBuilder.start(key).all(Arrays.asList(1, 2, 3)).size(3).get();
        assertTrue(testQuery(collection, queryTrue));
    }

    @Test
    public void nearTest() {
        final String key = "loc";
        final BasicDBObject geoSpatialIndex = new BasicDBObject();
        geoSpatialIndex.put(key, "2d");
        collection.ensureIndex(geoSpatialIndex);

        final Double[] coordinates = {(double) 50, (double) 30};
        saveTestDocument(collection, key, coordinates);

        DBObject queryTrue = QueryBuilder.start(key).near(45, 45).get();
        assertTrue(testQuery(collection, queryTrue));

        queryTrue = QueryBuilder.start(key).near(45, 45, 16).get();
        assertTrue(testQuery(collection, queryTrue));

        queryTrue = QueryBuilder.start(key).nearSphere(45, 45).get();
        assertTrue(testQuery(collection, queryTrue));

        queryTrue = QueryBuilder.start(key).nearSphere(45, 45, 0.5).get();
        assertTrue(testQuery(collection, queryTrue));

        queryTrue = QueryBuilder.start(key).withinCenterSphere(50, 30, 0.5).get();
        assertTrue(testQuery(collection, queryTrue));

        final ArrayList<Double[]> points = new ArrayList<Double[]>();
        points.add(new Double[]{(double) 30, (double) 30});
        points.add(new Double[]{(double) 70, (double) 30});
        points.add(new Double[]{(double) 70, (double) 30});
        queryTrue = QueryBuilder.start(key).withinPolygon(points).get();
        assertTrue(testQuery(collection, queryTrue));

        try {
            QueryBuilder.start(key).withinPolygon(null);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
        }

        try {
            QueryBuilder.start(key).withinPolygon(new ArrayList<Double[]>());
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
        }

        try {
            final ArrayList<Double[]> tooFew = new ArrayList<Double[]>();
            tooFew.add(new Double[]{(double) 30, (double) 30});
            QueryBuilder.start(key).withinPolygon(tooFew);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void failureTest() {
        boolean thrown = false;
        try {
            QueryBuilder.start("x").get();
        } catch (QueryBuilderException e) {
            thrown = true;
        }
        assertTrue(thrown);

        boolean thrown2 = false;
        try {
            QueryBuilder.start("x").exists(true).and("y").get();
        } catch (QueryBuilderException e) {
            thrown2 = true;
        }
        assertTrue(thrown2);

        boolean thrown3 = false;
        try {
            QueryBuilder.start("x").and("y").get();
        } catch (QueryBuilderException e) {
            thrown3 = true;
        }
        assertTrue(thrown3);
    }

    @Test
    public void testOr() {
        collection.drop();
        collection.insert(new BasicDBObject("a", 1));
        collection.insert(new BasicDBObject("b", 1));

        final DBObject q = QueryBuilder.start().or(new BasicDBObject("a", 1), new BasicDBObject("b", 1)).get();

        assertEquals(2, collection.find(q).count());
    }

    @Test
    public void testAnd() {
        collection.drop();
        collection.insert(new BasicDBObject("a", 1).append("b", 1));
        collection.insert(new BasicDBObject("b", 1));

        final DBObject q = QueryBuilder.start().and(new BasicDBObject("a", 1), new BasicDBObject("b", 1)).get();

        assertEquals(1, collection.find(q).count());
    }

    @Test
    public void testMultipleAnd() {
        collection.drop();
        collection.insert(new BasicDBObject("a", 1).append("b", 1));
        collection.insert(new BasicDBObject("b", 1));

        final DBObject q = QueryBuilder.start().and(new BasicDBObject("a", 1), new BasicDBObject("b", 1)).get();

        assertEquals(1, collection.find(q).count());
    }

    /**
     * Convenience method that creates a new MongoDB Document with a key-value pair and saves it inside the specified
     * collection
     *
     * @param collection Collection to save the new document to
     * @param key        key of the field to be inserted to the new document
     * @param value      value of the field to be inserted to the new document
     */
    private void saveTestDocument(final DBCollection collection, final String key, final Object value) {
        final DBObject testDocument = new BasicDBObject();
        testDocument.put(key, value);
        collection.save(testDocument);
    }

    private boolean testQuery(final DBCollection collection, final DBObject query) {
        final DBCursor cursor = collection.find(query);
        return cursor.hasNext();
    }
}

