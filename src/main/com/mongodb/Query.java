// Query.java

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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.QueryOperators;

/**
 * Utility for creating DBObject queries
 * @author Wes Freeman
 *
 */
public class Query extends BasicDBObject implements DBObject {
	
    public static final long serialVersionUID=98805780708789820L;

    /**
     * Creates a builder with an empty query
     */
    public Query() {
        super();
    }

    /**
     * Creates a builder with an object 
     */
    public Query(String key, Object val) {
        super(key, val);
    }

    /**
     * Equivalent to the $gt operator
     * @param val value to query
     * @return Returns Query with GT  
     */
    public Query GT(Object val) {
        this.appendToLastKeyValue(QueryOperators.GT, val); 
        return this;
    }

    /**
     * Equivalent to the $gt operator
     * @param key key to query
     * @param val value to query
     * @return Returns Query with GT  
     */
    public static Query GT(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.GT,val));
        return q;
    }

    /**
     * Equivalent to the $gte operator
     * @param val value to query
     * @return Returns Query with GTE  
     */
    public Query GTE(Object val) {
        this.appendToLastKeyValue(QueryOperators.GTE, val); 
        return this;
    }

    /**
     * Equivalent to the $gte operator
     * @param key key to query
     * @param val value to query
     * @return Returns Query with GTE  
     */
    public static Query GTE(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.GTE,val));
        return q;
    }

    /**
     * Equivalent to the $lt operator
     * @param val value to query
     * @return Returns Query with LT  
     */
    public Query LT(Object val) {
        this.appendToLastKeyValue(QueryOperators.LT, val); 
        return this;
    }

    /**
     * Equivalent to the $lt operator
     * @param key key to query
     * @param val value to query
     * @return Returns Query with LT  
     */
    public static Query LT(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.LT,val));
        return q;
    }

    /**
     * Equivalent to the $lte operator (chainable version)
     * @param val value to query
     * @return Returns Query with LTE  
     */
    public Query LTE(Object val) {
        this.appendToLastKeyValue(QueryOperators.LTE, val); 
        return this;
    }

    /**
     * Equivalent to the $lte operator
     * @param key key to query
     * @param val value to query
     * @return Returns query with LTE  
     */
    public static Query LTE(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.LTE,val));
        return q;
    }

    /**
     * Equivalent to the is relationship
     * @param key key to query
     * @param val value to query
     * @return Returns query with IS
     */
    public static Query EQ(String key, Object val) {
        Query q = new Query(key,val);
        return q;
    } 

    /**
     * Equivalent to the $ne operator
     * @param key key to query
     * @param val value to query
     * @return Returns query with NE  
     */
    public static Query NE(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.NE,val));
        return q;
    } 

    /**
     * Equivalent to the $in operator
     * @param key key to query
     * @param val value to query
     * @return Returns query with IN 
     */
    public static Query In(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.IN, val));
        return q;
    } 

    /**
     * Equivalent to the $nin operator
     * @param key key to query
     * @param val value to query
     * @return Returns query with NIN 
     */
    public static Query NotIn(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.NIN, val));
        return q;
    } 

    /**
     * Equivalent to the $mod operator
     * @param key key to query
     * @param mod value to mod by
     * @param equals value mod op should equal
     * @return Returns query with MOD 
     */
    public static Query Mod(String key, int mod, int equals) {
        Query q = new Query(key, new Query(QueryOperators.MOD, Arrays.asList(mod,equals)));
        return q;
    } 

    /**
     * Equivalent to the $all operator
     * @param key key to query
     * @param val value to query
     * @return Returns query with ALL
     */
    public static Query All(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.ALL, val));
        return q;
    } 

    /**
     * Equivalent to the $size operator (non-static, chainable version)
     * @param size size to compare
     * @return Returns query with SIZE
     */
    public Query Size(int size) {
        this.appendToLastKeyValue(QueryOperators.SIZE, size); 
        return this;
    } 

    /**
     * Equivalent to the $size operator
     * @param key key to query
     * @param size size to compare
     * @return Returns query with SIZE
     */
    public static Query Size(String key, int size) {
        Query q = new Query(key, new Query(QueryOperators.SIZE, size));
        return q;
    } 

    /**
     * Equivalent to the $exists operator
     * @param key key to query
     * @param exists boolean exists
     * @return Returns query with EXISTS
     */
    public static Query Exists(String key, boolean exists) {
        Query q = new Query(key, new Query(QueryOperators.EXISTS, exists));
        return q;
    } 

    /**
     * Equivalent to the $regex operator
     * @param key key to query
     * @param regex Pattern regex to match
     * @return Returns query with REGEX
     */
    public static Query Regex(String key, Pattern regex) {
        Query q = new Query(key, regex);
        return q;
    } 

    /**
     * Equivalent to the $and operator (non-static, chainable version)
     * @param params array of DBObjects
     * @return Returns query with AND
     */
    public Query and(DBObject... params) {
        List<DBObject> list = new ArrayList<DBObject>();
        list.add(this);
        for(DBObject p : params) {
           list.add(p);
	}
        Query q = new Query(QueryOperators.AND,list);
        return q;
    } 

    /**
     * Equivalent to the $and operator
     * @param params array of DBObjects
     * @return Returns query with AND
     */
    public static Query And(DBObject... params) {
        List<DBObject> list = new ArrayList<DBObject>();
        for(DBObject p : params) {
           list.add(p);
	}
        Query q = new Query(QueryOperators.AND,list);
        return q;
    } 

    /**
     * Equivalent to the $or operator (non-static, chainable version)
     * @param params array of DBObjects
     * @return Returns query with OR 
     */
    public Query or(DBObject... params) {
        List<DBObject> list = new ArrayList<DBObject>();
        list.add(this);
        for(DBObject p : params) {
           list.add(p);
	}
        Query q = new Query(QueryOperators.OR,list);
        return q;
    }

    /**
     * Equivalent to the $or operator
     * @param params array of DBObjects
     * @return Returns query with OR 
     */
    public static Query Or(DBObject... params) {
        List<DBObject> list = new ArrayList<DBObject>();
        for(DBObject p : params) {
           list.add(p);
	}
        Query q = new Query(QueryOperators.OR,list);
        return q;
    }

    /**
     * Equivalent to the $elemMatch operator
     * @param params array of DBObjects
     * @return Returns query with elemMatch 
     */
    public static Query ElemMatch(String key, Object val) {
        Query q = new Query(key, new Query(QueryOperators.ELEMMATCH,val));
        return q;
    }

    /**
     * Equivalent to the $near operator
     * @param key The key to check
     * @param x The x coord
     * @param y The y coord
     * @param dist The max distance
     * @param sphere Boolean -- whether to use nearSphere
     * @return Returns query with Near
     */
    public static Query Near(String key, double x, double y, double dist, boolean sphere) {
        Query q = null;
        if(sphere) {
            q = new Query(key, new Query(QueryOperators.NEARSPHERE,Arrays.asList(x, y)).append(QueryOperators.MAXDISTANCE, dist));
        } else {
            q = new Query(key, new Query(QueryOperators.NEAR,Arrays.asList(x, y)).append(QueryOperators.MAXDISTANCE, dist));
        }
        return q;
    }

    /**
     * Equivalent to the $near operator
     * @param key The key to check
     * @param x The x coord
     * @param y The y coord
     * @param dist The max distance
     * @return Returns query with Near
     */
    public static Query Near(String key, double x, double y, double dist) {
        Query q = new Query(key, new Query(QueryOperators.NEAR,Arrays.asList(x, y)).append(QueryOperators.MAXDISTANCE, dist));
        return q;
    }

    /**
     * Equivalent to the $near operator
     * @param key The key to check
     * @param x The x coord
     * @param y The y coord
     * @return Returns query with Near
     */
    public static Query Near(String key, double x, double y) {
        Query q = new Query(key, new Query(QueryOperators.NEAR,Arrays.asList(x,y)));
        return q;
    }

    @Override
    public final String toString() {
        return super.toString();
    } 

    /** 
     * Helper to append another operator and value.
     * @param k operator to append
     * @param val value to append
     */
    private void appendToLastKeyValue(String k, Object val) {
        String key = (String)this.toMap().keySet().toArray()[this.toMap().keySet().toArray().length-1];
        this.put(key, ((BasicDBObject)this.toMap().get(key)).append(k,val));
    }
}
