/* QueryBuilder.java
 *
 * modified April 11, 2012 by Bryan Reinero
 *  added $nearSphere, $centerSphere and $within $polygon query support
 */

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility for creating DBObject queries
 * @author Julson Lim
 *
 */
public class QueryBuilder {

    /**
     * Creates a builder with an empty query
     */
    public QueryBuilder() {
        _query = new BasicDBObject();
    }

    /**
     * returns a new QueryBuilder
     * @return
     */
    public static QueryBuilder start() {
        return new QueryBuilder();
    }
	
    /**
     * Creates a new query with a document key
     * @param key MongoDB document key
     * @return Returns a new QueryBuilder
     */
    public static QueryBuilder start(String key) {
        return (new QueryBuilder()).put(key);
    }
	
    /**
     * Adds a new key to the query if not present yet.
     * Sets this key as the current key.
     * @param key MongoDB document key
     * @return this
     */
    public QueryBuilder put(String key) {
        _currentKey = key;
        if(_query.get(key) == null) {
            _query.put(_currentKey, new NullObject());
        }
        return this;
    }
	
    /**
     * Equivalent to <code>QueryBuilder.put(key)</code>. Intended for compound query chains to be more readable, e.g.
     * {@code QueryBuilder.start("a").greaterThan(1).and("b").lessThan(3) }
     * @param key MongoDB document key
     * @return this
     */
    public QueryBuilder and(String key) {
        return put(key);
    }
	
    /**
     * Equivalent to the $gt operator
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "greater than" query  
     */
    public QueryBuilder greaterThan(Object object) {
        addOperand(QueryOperators.GT, object);
        return this;
    }
	
    /**
     * Equivalent to the $gte operator
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "greater than or equals" query
     */
    public QueryBuilder greaterThanEquals(Object object) {
        addOperand(QueryOperators.GTE, object);
        return this;
    }
	
    /**
     * Equivalent to the $lt operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "less than" query
     */
    public QueryBuilder lessThan(Object object) {
        addOperand(QueryOperators.LT, object);
        return this;
    }
	
    /**
     * Equivalent to the $lte operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "less than or equals" query
     */
    public QueryBuilder lessThanEquals(Object object) {
        addOperand(QueryOperators.LTE, object);
        return this;
    }
	
    /**
     * Equivalent of the find({key:value})
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended equality query
     */
    public QueryBuilder is(Object object) {
        addOperand(null, object);
        return this;
    }
	
    /**
     * Equivalent of the $ne operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended inequality query
     */
    public QueryBuilder notEquals(Object object) {
        addOperand(QueryOperators.NE, object);
        return this;
    }
	
    /**
     * Equivalent of the $in operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "in array" query
     */
    public QueryBuilder in(Object object) {
        addOperand(QueryOperators.IN, object);
        return this;
    }
	
    /**
     * Equivalent of the $nin operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "not in array" query
     */
    public QueryBuilder notIn(Object object) {
        addOperand(QueryOperators.NIN, object);
        return this;
    }
	
    /**
     * Equivalent of the $mod operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended modulo query
     */
    public QueryBuilder mod(Object object) {
        addOperand(QueryOperators.MOD, object);
        return this;
    }
	
    /**
     * Equivalent of the $all operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended "matches all array contents" query
     */
    public QueryBuilder all(Object object) {
        addOperand(QueryOperators.ALL, object);
        return this;
    }
	
    /**
     * Equivalent of the $size operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended size operator
     */
    public QueryBuilder size(Object object) {
        addOperand(QueryOperators.SIZE, object);
        return this;
    }
	
    /**
     * Equivalent of the $exists operand
     * @param object Value to query
     * @return Returns the current QueryBuilder with an appended exists operator
     */
    public QueryBuilder exists(Object object) {
        addOperand(QueryOperators.EXISTS, object);
        return this;
    }
	
    /**
     * Passes a regular expression for a query
     * @param regex Regex pattern object
     * @return Returns the current QueryBuilder with an appended regex query
     */
    public QueryBuilder regex(Pattern regex) {
        addOperand(null, regex);
        return this;
    }

    /**
     * Equivalent to the $elemMatch operand
     * @param match  the object to match
     * @return Returns the current QueryBuilder with an appended elemMatch operator
     */
    public QueryBuilder elemMatch(final DBObject match) {
        addOperand(QueryOperators.ELEM_MATCH, match);
        return this;
    }



    /**
     * Equivalent of the $within operand, used for geospatial operation
     * @param x x coordinate
     * @param y y coordinate
     * @param radius radius
     * @return
     */
    public QueryBuilder withinCenter( double x , double y , double radius ){
        addOperand( QueryOperators.WITHIN ,
                    new BasicDBObject(QueryOperators.CENTER, new Object[]{ new Double[]{ x , y } , radius } ) );
        return this;
    }
    
    /**
     * Equivalent of the $near operand
     * @param x x coordinate
     * @param y y coordinate
     * @return
     */
    public QueryBuilder near( double x , double y  ){
        addOperand(QueryOperators.NEAR,
                    new Double[]{ x , y } );
        return this;
    }

    /**
     * Equivalent of the $near operand
     * @param x x coordinate
     * @param y y coordinate
     * @param maxDistance max distance
     * @return
     */
    public QueryBuilder near( double x , double y , double maxDistance ){
        addOperand( QueryOperators.NEAR ,
                    new Double[]{ x , y , maxDistance } );
        return this;
    }
    
    /**
     * Equivalent of the $nearSphere operand
     * @param longitude coordinate in decimal degrees 
     * @param latitude coordinate in decimal degrees
     * @return
     */
    public QueryBuilder nearSphere( double longitude , double latitude ){
        addOperand(QueryOperators.NEAR_SPHERE,
                    new Double[]{ longitude , latitude } );
        return this;
    }
    
    /**
     * Equivalent of the $nearSphere operand
     * @param longitude coordinate in decimal degrees 
     * @param latitude coordinate in decimal degrees
     * @param maxDistance max spherical distance
     * @return
     */
    public QueryBuilder nearSphere( double longitude , double latitude , double maxDistance ){
        addOperand( QueryOperators.NEAR_SPHERE ,
                    new Double[]{ longitude , latitude , maxDistance } );
        return this;
    }
    
    /**
     * Equivalent of the $centerSphere operand
     * mostly intended for queries up to a few hundred miles or km.
     * @param longitude coordinate in decimal degrees 
     * @param latitude coordinate in decimal degrees
     * @param maxDistance max spherical distance
     * @return
     */
    public QueryBuilder withinCenterSphere( double longitude , double latitude , double maxDistance ){
        addOperand( QueryOperators.WITHIN ,
                new BasicDBObject(QueryOperators.CENTER_SPHERE, new Object[]{ new Double[]{longitude , latitude} , maxDistance } ) );
        return this;
    }
    
    /**
     * Equivalent to a $within operand, based on a bounding box using represented by two corners
     * 
     * @param x the x coordinate of the first box corner.
     * @param y the y coordinate of the first box corner.
     * @param x2 the x coordinate of the second box corner.
     * @param y2 the y coordinate of the second box corner.
     * @return
     */
    public QueryBuilder withinBox(double x, double y, double x2, double y2) {
    	addOperand( QueryOperators.WITHIN ,
                    new BasicDBObject(QueryOperators.BOX, new Object[] { new Double[] { x, y }, new Double[] { x2, y2 } } ) );
    	return this;
    }
    
    /**
     * Equivalent to a $within operand, based on a bounding polygon represented by an array of points
     * 
     * @param points an array of Double[] defining the vertices of the search area
     * @return this
     */
    public QueryBuilder withinPolygon(List<Double[]> points) {
        if(points == null || points.isEmpty() || points.size() < 3)
            throw new IllegalArgumentException("Polygon insufficient number of vertices defined");
        addOperand( QueryOperators.WITHIN ,
                    new BasicDBObject(QueryOperators.POLYGON, points ) );
        return this;
    }

    /**
     * Equivalent to $not meta operator. Must be followed by an operand, not a value, e.g.
     * {@code QueryBuilder.start("val").not().mod(Arrays.asList(10, 1)) }
     *
     * @return Returns the current QueryBuilder with an appended "not" meta operator
     */
    public QueryBuilder not() {
        _hasNot = true;
        return this;
    }

    /**
     * Equivalent to an $or operand
     * @param ors the list of conditions to or together
     * @return Returns the current QueryBuilder with appended "or" operator
     */
    @SuppressWarnings("unchecked")
    public QueryBuilder or( DBObject ... ors ){
        List l = (List)_query.get( QueryOperators.OR );
        if ( l == null ){
            l = new ArrayList();
            _query.put( QueryOperators.OR , l );
        }
        Collections.addAll(l, ors);
        return this;
    }

    /**
     * Equivalent to an $and operand
     * @param ands the list of conditions to and together
     * @return Returns the current QueryBuilder with appended "and" operator
     */
    @SuppressWarnings("unchecked")
    public QueryBuilder and( DBObject ... ands ){
        List l = (List)_query.get( QueryOperators.AND );
        if ( l == null ){
            l = new ArrayList();
            _query.put( QueryOperators.AND , l );
        }
        Collections.addAll(l, ands);
        return this;
    }

    /**
     * Creates a <code>DBObject</code> query to be used for the driver's find operations
     * @return Returns a DBObject query instance
     * @throws RuntimeException if a key does not have a matching operand
     */
    public DBObject get() {
        for(String key : _query.keySet()) {
            if(_query.get(key) instanceof NullObject) {
                throw new QueryBuilderException("No operand for key:" + key);
            }
        }
        return _query;
    }
	
    private void addOperand(String op, Object value) {
        if(op == null) {
            if (_hasNot) {
                value = new BasicDBObject(QueryOperators.NOT, value);
                _hasNot = false;
            }
            _query.put(_currentKey, value);
            return;
        }

        Object storedValue = _query.get(_currentKey);
        BasicDBObject operand;
        if(!(storedValue instanceof DBObject)) {
            operand = new BasicDBObject();
            if (_hasNot) {
                DBObject notOperand = new BasicDBObject(QueryOperators.NOT, operand);
                _query.put(_currentKey, notOperand);
                _hasNot = false;
            } else {
                _query.put(_currentKey, operand);
            }
        } else {
            operand = (BasicDBObject)_query.get(_currentKey);
            if (operand.get(QueryOperators.NOT) != null) {
                operand = (BasicDBObject) operand.get(QueryOperators.NOT);
            }
        }
        operand.put(op, value);
    }
    @SuppressWarnings("serial")
	static class QueryBuilderException extends RuntimeException {
            QueryBuilderException(String message) {
                super(message);
            }
	}
    private static class NullObject {}
	
    private DBObject _query;
    private String _currentKey;
    private boolean _hasNot;
	
}
