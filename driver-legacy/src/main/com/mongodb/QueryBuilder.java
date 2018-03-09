/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * Utility for creating DBObject queries
 *
 * @mongodb.driver.manual tutorial/query-documents/ Querying
 */
@SuppressWarnings("rawtypes")
public class QueryBuilder {

    /**
     * Creates a builder with an empty query
     */
    public QueryBuilder() {
        _query = new BasicDBObject();
    }

    /**
     * Returns a new QueryBuilder.
     *
     * @return a builder
     */
    public static QueryBuilder start() {
        return new QueryBuilder();
    }

    /**
     * Creates a new query with a document key
     *
     * @param key MongoDB document key
     * @return {@code this}
     */
    public static QueryBuilder start(final String key) {
        return (new QueryBuilder()).put(key);
    }

    /**
     * Adds a new key to the query if not present yet. Sets this key as the current key.
     *
     * @param key MongoDB document key
     * @return {@code this}
     */
    public QueryBuilder put(final String key) {
        _currentKey = key;
        if (_query.get(key) == null) {
            _query.put(_currentKey, new NullObject());
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
     * @param object Value to query
     * @return {@code this}
     */
    public QueryBuilder is(final Object object) {
        addOperand(null, object);
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
    public QueryBuilder elemMatch(final DBObject match) {
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
                   new BasicDBObject(QueryOperators.CENTER, asList(asList(x, y), radius)));
        return this;
    }

    /**
     * Equivalent of the $near operand
     *
     * @param x x coordinate
     * @param y y coordinate
     * @return {@code this}
     */
    public QueryBuilder near(final double x, final double y) {
        addOperand(QueryOperators.NEAR,
                   asList(x, y));
        return this;
    }

    /**
     * Equivalent of the $near operand
     *
     * @param x           x coordinate
     * @param y           y coordinate
     * @param maxDistance max distance
     * @return {@code this}
     */
    public QueryBuilder near(final double x, final double y, final double maxDistance) {
        addOperand(QueryOperators.NEAR,
                   asList(x, y));
        addOperand(QueryOperators.MAX_DISTANCE,
                   maxDistance);
        return this;
    }

    /**
     * Equivalent of the $nearSphere operand
     *
     * @param longitude coordinate in decimal degrees
     * @param latitude  coordinate in decimal degrees
     * @return {@code this}
     */
    public QueryBuilder nearSphere(final double longitude, final double latitude) {
        addOperand(QueryOperators.NEAR_SPHERE,
                   asList(longitude, latitude));
        return this;
    }

    /**
     * Equivalent of the $nearSphere operand
     *
     * @param longitude   coordinate in decimal degrees
     * @param latitude    coordinate in decimal degrees
     * @param maxDistance max spherical distance
     * @return {@code this}
     */
    public QueryBuilder nearSphere(final double longitude, final double latitude, final double maxDistance) {
        addOperand(QueryOperators.NEAR_SPHERE,
                   asList(longitude, latitude));
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
                   new BasicDBObject(QueryOperators.CENTER_SPHERE,
                                     asList(asList(longitude, latitude), maxDistance)));
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
    @SuppressWarnings("unchecked")
    public QueryBuilder withinBox(final double x, final double y, final double x2, final double y2) {
        addOperand(QueryOperators.WITHIN,
                   new BasicDBObject(QueryOperators.BOX, new Object[]{new Double[]{x, y}, new Double[]{x2, y2}}));
        return this;
    }

    /**
     * Equivalent to a $within operand, based on a bounding polygon represented by an array of points
     *
     * @param points an array of Double[] defining the vertices of the search area
     * @return {@code this}
     */
    public QueryBuilder withinPolygon(final List<Double[]> points) {
        notNull("points", points);
        if (points.isEmpty() || points.size() < 3) {
            throw new IllegalArgumentException("Polygon insufficient number of vertices defined");
        }
        addOperand(QueryOperators.WITHIN,
                   new BasicDBObject(QueryOperators.POLYGON, convertToListOfLists(points)));
        return this;
    }

    private List<List<Double>> convertToListOfLists(final List<Double[]> points) {
        List<List<Double>> listOfLists = new ArrayList<List<Double>>(points.size());
        for (Double[] cur : points) {
            List<Double> list = new ArrayList<Double>(cur.length);
            Collections.addAll(list, cur);
            listOfLists.add(list);
        }
        return listOfLists;
    }

    /**
     * Equivalent to a $text operand.
     *
     * @param search the search terms to apply to the text index.
     * @return {@code this}
     * @mongodb.server.release 2.6
     */
    public QueryBuilder text(final String search) {
        return text(search, null);
    }

    /**
     * Equivalent to a $text operand.
     *
     * @param search   the search terms to apply to the text index.
     * @param language the language to use.
     * @return {@code this}
     * @mongodb.server.release 2.6
     */
    public QueryBuilder text(final String search, @Nullable final String language) {
        if (_currentKey != null) {
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
        _hasNot = true;
        return this;
    }

    /**
     * Equivalent to an $or operand
     *
     * @param ors the list of conditions to or together
     * @return {@code this}
     */
    @SuppressWarnings("unchecked")
    public QueryBuilder or(final DBObject... ors) {
        List l = (List) _query.get(QueryOperators.OR);
        if (l == null) {
            l = new ArrayList();
            _query.put(QueryOperators.OR, l);
        }
        Collections.addAll(l, ors);
        return this;
    }

    /**
     * Equivalent to an $and operand
     *
     * @param ands the list of conditions to and together
     * @return {@code this}
     */
    @SuppressWarnings("unchecked")
    public QueryBuilder and(final DBObject... ands) {
        List l = (List) _query.get(QueryOperators.AND);
        if (l == null) {
            l = new ArrayList();
            _query.put(QueryOperators.AND, l);
        }
        Collections.addAll(l, ands);
        return this;
    }

    /**
     * Creates a {@code DBObject} query to be used for the driver's find operations
     *
     * @return {@code this}
     * @throws RuntimeException if a key does not have a matching operand
     */
    public DBObject get() {
        for (final String key : _query.keySet()) {
            if (_query.get(key) instanceof NullObject) {
                throw new QueryBuilderException("No operand for key:" + key);
            }
        }
        return _query;
    }

    private void addOperand(@Nullable final String op, final Object value) {
        Object valueToPut = value;
        if (op == null) {
            if (_hasNot) {
                valueToPut = new BasicDBObject(QueryOperators.NOT, valueToPut);
                _hasNot = false;
            }
            _query.put(_currentKey, valueToPut);
            return;
        }

        Object storedValue = _query.get(_currentKey);
        BasicDBObject operand;
        if (!(storedValue instanceof DBObject)) {
            operand = new BasicDBObject();
            if (_hasNot) {
                DBObject notOperand = new BasicDBObject(QueryOperators.NOT, operand);
                _query.put(_currentKey, notOperand);
                _hasNot = false;
            } else {
                _query.put(_currentKey, operand);
            }
        } else {
            operand = (BasicDBObject) _query.get(_currentKey);
            if (operand.get(QueryOperators.NOT) != null) {
                operand = (BasicDBObject) operand.get(QueryOperators.NOT);
            }
        }
        operand.put(op, valueToPut);
    }

    @SuppressWarnings("serial")
    static class QueryBuilderException extends RuntimeException {
        QueryBuilderException(final String message) {
            super(message);
        }
    }

    private static class NullObject {
    }

    private final DBObject _query;
    private String _currentKey;
    private boolean _hasNot;

}
