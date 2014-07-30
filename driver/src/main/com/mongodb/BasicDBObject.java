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

package com.mongodb;

import com.mongodb.util.JSON;
import org.bson.BasicBSONObject;

import java.util.Map;

/**
 * A basic implementation of BSON object that is mongo specific. A {@code DBObject} can be created as follows, using this class:
 * <blockquote><pre>
 * DBObject obj = new BasicDBObject();
 * obj.put( "foo", "bar" );
 * </pre></blockquote>
 */
@SuppressWarnings({"rawtypes"})
public class BasicDBObject extends BasicBSONObject implements DBObject {
    private static final long serialVersionUID = -4415279469780082174L;

    private boolean isPartialObject;

    /**
     * Creates an empty object.
     */
    public BasicDBObject() {
        super();
    }

    /**
     * creates an empty object
     *
     * @param size an estimate of number of fields that will be inserted
     */
    public BasicDBObject(final int size) {
        super(size);
    }

    /**
     * creates an object with the given key/value
     *
     * @param key   key under which to store
     * @param value value to store
     */
    public BasicDBObject(final String key, final Object value) {
        super(key, value);
    }

    /**
     * Creates an object from a map.
     *
     * @param map map to convert
     */
    public BasicDBObject(final Map map) {
        super(map);
    }

    /**
     * Add a key/value pair to this object
     *
     * @param key the field name
     * @param val the field value
     * @return this BasicDBObject with the new values added
     */
    @Override
    public BasicDBObject append(final String key, final Object val) {
        put(key, val);
        return this;
    }

    /**
     * Whether {@link #markAsPartialObject} was ever called only matters if you are going to upsert and do not want to risk losing fields.
     *
     * @return true if this has been marked as a partial object
     */
    @Override
    public boolean isPartialObject() {
        return isPartialObject;
    }

    /**
     * Returns a JSON serialization of this object
     * <p/>
     * The output will look like: {@code  {"a":1, "b":["x","y","z"]} }
     *
     * @return JSON serialization
     */
    public String toString() {
        return JSON.serialize(this);
    }

    /**
     * If this object was retrieved with only some fields (using a field filter) this method will be called to mark it as such.
     */
    @Override
    public void markAsPartialObject() {
        isPartialObject = true;
    }

    /**
     * @return a BasicDBObject with exactly the same values as this instance.
     */
    public Object copy() {
        // copy field values into new object
        BasicDBObject newCopy = new BasicDBObject(this.toMap());
        // need to clone the sub obj
        for (final String field : keySet()) {
            Object val = get(field);
            if (val instanceof BasicDBObject) {
                newCopy.put(field, ((BasicDBObject) val).copy());
            } else if (val instanceof BasicDBList) {
                newCopy.put(field, ((BasicDBList) val).copy());
            }
        }
        return newCopy;
    }

}
