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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * <p>Utility for building complex objects. For example:</p>
 * <pre>
 *   {@code BasicDBObjectBuilder.start().add( "name" , "eliot").add("number" , 17).get()}
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class BasicDBObjectBuilder {

    /**
     * Creates a builder intialized with an empty document.
     */
    public BasicDBObjectBuilder() {
        _stack = new LinkedList<>();
        _stack.add(new BasicDBObject());
    }

    /**
     * Creates a builder intialized with an empty document.
     *
     * @return The new empty builder
     */
    public static BasicDBObjectBuilder start() {
        return new BasicDBObjectBuilder();
    }

    /**
     * Creates a builder initialized with the given key/value.
     *
     * @param key The field name
     * @param val The value
     * @return the new builder
     */
    public static BasicDBObjectBuilder start(final String key, final Object val) {
        return (new BasicDBObjectBuilder()).add(key, val);
    }

    /**
     * Creates an object builder from an existing map of key value pairs.
     *
     * @param documentAsMap a document in Map form.
     * @return the new builder
     */
    @SuppressWarnings("unchecked")
    public static BasicDBObjectBuilder start(final Map documentAsMap) {
        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        Iterator<Map.Entry> i = documentAsMap.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry entry = i.next();
            builder.add(entry.getKey().toString(), entry.getValue());
        }
        return builder;
    }

    /**
     * Appends the key/value to the active object
     *
     * @param key the field name
     * @param val the value of the field
     * @return {@code this} so calls can be chained
     */
    public BasicDBObjectBuilder append(final String key, final Object val) {
        _cur().put(key, val);
        return this;
    }

    /**
     * Same as append
     *
     * @param key the field name
     * @param val the value of the field
     * @return {@code this} so calls can be chained
     * @see #append(String, Object)
     */
    public BasicDBObjectBuilder add(final String key, final Object val) {
        return append(key, val);
    }

    /**
     * Creates a new empty object and inserts it into the current object with the given key. The new child object becomes the active one.
     *
     * @param key the field name
     * @return {@code this} so calls can be chained
     */
    public BasicDBObjectBuilder push(final String key) {
        BasicDBObject o = new BasicDBObject();
        _cur().put(key, o);
        _stack.addLast(o);
        return this;
    }

    /**
     * Pops the active object, which means that the parent object becomes active
     *
     * @return {@code this} so calls can be chained
     */
    public BasicDBObjectBuilder pop() {
        if (_stack.size() <= 1) {
            throw new IllegalArgumentException("can't pop last element");
        }
        _stack.removeLast();
        return this;
    }

    /**
     * Gets the top level document.
     *
     * @return The base object
     */
    public DBObject get() {
        return _stack.getFirst();
    }

    /**
     * Returns true if no key/value was inserted into the top level document.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return ((BasicDBObject) _stack.getFirst()).size() == 0;
    }

    private DBObject _cur() {
        return _stack.getLast();
    }

    private final LinkedList<DBObject> _stack;

}
