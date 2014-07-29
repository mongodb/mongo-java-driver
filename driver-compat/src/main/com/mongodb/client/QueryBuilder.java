/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.client;

import com.mongodb.QueryOperators;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

//TODO: this is a basic conversion of the old QueryBuilder, but I believe it needs to be a) easier to use and b) a lot better documented
public class QueryBuilder implements ConvertibleToDocument {
    private final Document query;
    private String currentKey;
    private boolean hasNot;

    /**
     * Creates a builder with an empty query
     */
    public QueryBuilder() {
        query = new Document();
    }

    /**
     * Factory for QueryBuilder
     *
     * @return a new QueryBuilder
     */
    public static QueryBuilder query() {
        return new QueryBuilder();
    }

    /**
     * Creates a new query with a document key
     *
     * @param key MongoDB document key
     * @return {@code this}
     */
    public static QueryBuilder query(final String key) {
        return (new QueryBuilder()).put(key);
    }

    /**
     * Adds a new key to the query if not present yet. Sets this key as the current key.
     *
     * @param key MongoDB document key
     * @return {@code this}
     */
    public QueryBuilder put(final String key) {
        currentKey = key;
        if (query.get(key) == null) {
            query.put(currentKey, new NullObject());
        }
        return this;
    }

    /**
     * Equivalent to {@code QueryBuilder.put(key)}. Intended for compound query chains to be more readable, e.g. {@code
     * QueryBuilder.start("a").greaterThan(1).and("b").lessThan(3) }
     *
     * @param key MongoDB document key
     * @return {@code this}
     */
    public QueryBuilder and(final String key) {
        return put(key);
    }

    /**
     * Equivalent to the $gt operator
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder greaterThan(final Object object) {
        addOperand(QueryOperators.GT, object);
        return this;
    }

    /**
     * Equivalent to the $gte operator
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder greaterThanEquals(final Object object) {
        addOperand(QueryOperators.GTE, object);
        return this;
    }

    /**
     * Equivalent to the $lt operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder lessThan(final Object object) {
        addOperand(QueryOperators.LT, object);
        return this;
    }

    /**
     * Equivalent to the $lte operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder lessThanEquals(final Object object) {
        addOperand(QueryOperators.LTE, object);
        return this;
    }

    /**
     * Equivalent of the find({key:value})
     *
     * @param value Value to query
     * @return {@code this}
     */
    public QueryBuilder is(final Object value) {
        addOperand(null, value);
        return this;
    }

    /**
     * Equivalent of the find({key:value})
     *
     * @param value value to query
     * @return {@code this}
     */
    public QueryBuilder is(final QueryBuilder value) {
        addOperand(null, value.toDocument());
        return this;
    }

    /**
     * Equivalent of the $ne operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder notEquals(final Object object) {
        addOperand(QueryOperators.NE, object);
        return this;
    }

    /**
     * Equivalent of the $in operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder in(final Object object) {
        addOperand(QueryOperators.IN, object);
        return this;
    }

    /**
     * Equivalent of the $nin operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder notIn(final Object object) {
        addOperand(QueryOperators.NIN, object);
        return this;
    }

    /**
     * Equivalent of the $mod operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder mod(final Object object) {
        addOperand(QueryOperators.MOD, object);
        return this;
    }

    /**
     * Equivalent of the $all operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder all(final Object object) {
        addOperand(QueryOperators.ALL, object);
        return this;
    }

    /**
     * Equivalent of the $size operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder size(final Object object) {
        addOperand(QueryOperators.SIZE, object);
        return this;
    }

    /**
     * Equivalent of the $exists operand
     *
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder exists(final Object object) {
        addOperand(QueryOperators.EXISTS, object);
        return this;
    }

    /**
     * Passes a regular expression for a query
     *
     * @param regex Regex pattern object
     * @return {@code this}
     */
    public QueryBuilder regex(final Pattern regex) {
        addOperand(null, regex);
        return this;
    }

    /**
     * Equivalent to the $elemMatch operand
     *
     * @param match the object to match
     * @return {@code this}
     */
    public QueryBuilder elemMatch(final Document match) {
        addOperand(QueryOperators.ELEM_MATCH, match);
        return this;
    }


    /**
     * Equivalent of the $within operand, used for geospatial operation
     *
     * @param x      x coordinate
     * @param y      y coordinate
     * @param radius radius
     * @return {@code this}
     */
    public QueryBuilder withinCenter(final double x, final double y, final double radius) {
        addOperand(QueryOperators.WITHIN,
                   new Document(QueryOperators.CENTER, new Object[]{new Double[]{x, y}, radius}));
        return this;
    }

    /**
     * Equivalent of the $near operand
     * @param x x coordinate
     * @param y y coordinate
     * @return {@code this}
     */
    public QueryBuilder near(final double x, final double y){
        addOperand(QueryOperators.NEAR,
                   Arrays.asList(x, y));
        return this;
    }

    /**
     * Equivalent of the $near operand
     * @param x x coordinate
     * @param y y coordinate
     * @param maxDistance max distance
     * @return {@code this}
     */
    public QueryBuilder near(final double x, final double y, final double maxDistance){
        addOperand(QueryOperators.NEAR,
                   Arrays.asList(x, y));
        addOperand(QueryOperators.MAX_DISTANCE,
                   maxDistance);

        return this;
    }

    /**
     * Equivalent of the $nearSphere operand
     * @param longitude coordinate in decimal degrees
     * @param latitude coordinate in decimal degrees
     * @return {@code this}
     */
    public QueryBuilder nearSphere(final double longitude, final double latitude){
        addOperand(QueryOperators.NEAR_SPHERE,
                   Arrays.asList(longitude, latitude));
        return this;
    }

    /**
     * Equivalent of the $nearSphere operand
     * @param longitude coordinate in decimal degrees
     * @param latitude coordinate in decimal degrees
     * @param maxDistance max spherical distance
     * @return {@code this}
     */
    public QueryBuilder nearSphere(final double longitude, final double latitude, final double maxDistance){
        addOperand(QueryOperators.NEAR_SPHERE,
                   Arrays.asList(longitude, latitude));
        addOperand(QueryOperators.MAX_DISTANCE,
                   maxDistance);
        return this;
    }

    /**
     * Equivalent of the $centerSphere operand mostly intended for queries up to a few hundred miles or km.
     *
     * @param longitude   coordinate in decimal degrees
     * @param latitude    coordinate in decimal degrees
     * @param maxDistance max spherical distance
     * @return {@code this}
     */
    public QueryBuilder withinCenterSphere(final double longitude, final double latitude, final double maxDistance) {
        addOperand(QueryOperators.WITHIN,
                   new Document(QueryOperators.CENTER_SPHERE,
                                new Object[]{new Double[]{longitude, latitude}, maxDistance}));
        return this;
    }

    /**
     * Equivalent to a $within operand, based on a bounding box using represented by two corners
     *
     * @param x  the x coordinate of the first box corner.
     * @param y  the y coordinate of the first box corner.
     * @param x2 the x coordinate of the second box corner.
     * @param y2 the y coordinate of the second box corner.
     * @return {@code this}
     */
    public QueryBuilder withinBox(final double x, final double y, final double x2, final double y2) {
        addOperand(QueryOperators.WITHIN,
                   new Document(QueryOperators.BOX, new Object[]{new Double[]{x, y}, new Double[]{x2, y2}}));
        return this;
    }

    /**
     * Equivalent to a $within operand, based on a bounding polygon represented by an array of points
     *
     * @param points an array of Double[] defining the vertices of the search area
     * @return {@code this}
     */
    public QueryBuilder withinPolygon(final List<Double[]> points) {
        if (points == null || points.isEmpty() || points.size() < 3) {
            throw new IllegalArgumentException("Polygon insufficient number of vertices defined");
        }
        addOperand(QueryOperators.WITHIN,
                   new Document(QueryOperators.POLYGON, points));
        return this;
    }

    /**
     * Equivalent to a $text operand.
     * @param search the search terms to apply to the text index.
     * @return {@code this}
     */
    public QueryBuilder text(final String search) {
        return text(search, null);
    }

    /**
     * Equivalent to a $text operand.
     * @param search the search terms to apply to the text index.
     * @param language the language to use.
     * @return {@code this}
     */
    public QueryBuilder text(final String search, final String language) {
        if (currentKey != null) {
            throw new QueryBuilderException("The text operand may only occur at the top-level of a query. It does"
                    + " not apply to a specific element, but rather to a document as a whole.");
        }

        put(QueryOperators.TEXT);
        addOperand(QueryOperators.SEARCH, search);
        if (language != null) {
            addOperand(QueryOperators.LANGUAGE, language);
        }

        return this;
    }

    /**
     * Equivalent to $not meta operator. Must be followed by an operand, not a value, e.g. {@code
     * QueryBuilder.start("val").not().mod(Arrays.asList(10, 1)) }
     *
     * @return {@code this}
     */
    public QueryBuilder not() {
        hasNot = true;
        return this;
    }

    /**
     * Equivalent to an $or operand
     *
     * @param ors the list of conditions to or together
     * @return Returns the current QueryBuilder with appended "or" operator
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public QueryBuilder or(final Document... ors) {
        List l = (List) query.get(QueryOperators.OR);
        if (l == null) {
            l = new ArrayList();
            query.put(QueryOperators.OR, l);
        }
        Collections.addAll(l, ors);
        return this;
    }

    /**
     * Equivalent to an $or operand.
     *
     * @param operand a QueryBuilder containing the values to OR
     * @return Returns the current QueryBuilder with appended "or" operator
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public QueryBuilder or(final QueryBuilder operand) {
        List<Document> orOperands = (List<Document>) query.get(QueryOperators.OR);
        if (orOperands == null) {
            orOperands = new ArrayList<Document>();
            query.put(QueryOperators.OR, orOperands);
        }
        orOperands.add(operand.toDocument());
        return this;
    }

    /**
     * Equivalent to an $and operand
     *
     * @param ands the list of conditions to and together
     * @return Returns the current QueryBuilder with appended "and" operator
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public QueryBuilder and(final Document... ands) {
        List l = (List) query.get(QueryOperators.AND);
        if (l == null) {
            l = new ArrayList();
            query.put(QueryOperators.AND, l);
        }
        Collections.addAll(l, ands);
        return this;
    }

    /**
     * Creates a {@code Document} query to be used for the driver's find operations
     *
     * @return Returns a Document query instance
     * @throws RuntimeException if a key does not have a matching operand
     */
    @Override
    public Document toDocument() {
        for (final String key : query.keySet()) {
            if (query.get(key) instanceof NullObject) {
                throw new QueryBuilderException("No operand for key:" + key);
            }
        }
        return query;
    }

    private void addOperand(final String op, final Object value) {
        if (op == null) {
            Object valueToAdd = value;
            if (hasNot) {
                valueToAdd = new Document(QueryOperators.NOT, value);
                hasNot = false;
            }
            query.put(currentKey, valueToAdd);
            return;
        }

        Object storedValue = query.get(currentKey);
        Document operand;
        if (!(storedValue instanceof Document)) {
            operand = new Document();
            if (hasNot) {
                Document notOperand = new Document(QueryOperators.NOT, operand);
                query.put(currentKey, notOperand);
                hasNot = false;
            } else {
                query.put(currentKey, operand);
            }
        } else {
            operand = (Document) query.get(currentKey);
            if (operand.get(QueryOperators.NOT) != null) {
                operand = (Document) operand.get(QueryOperators.NOT);
            }
        }
        operand.put(op, value);
    }

    @SuppressWarnings("serial")
    static class QueryBuilderException extends RuntimeException {
        QueryBuilderException(final String message) {
            super(message);
        }
    }

    private static class NullObject {
    }

}
