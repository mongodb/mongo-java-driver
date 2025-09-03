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

// BasicDBList.java

package com.mongodb;

import org.bson.types.BasicBSONList;

/**
 * An implementation of List that reflects the way BSON lists work.
 */
public class BasicDBList extends BasicBSONList implements DBObject {

    private static final long serialVersionUID = -4415279469780082174L;

    @Override
    public boolean isPartialObject() {
        return _isPartialObject;
    }

    @Override
    public void markAsPartialObject() {
        _isPartialObject = true;
    }

    /**
     * Copies this instance into a new Object.
     *
     * @return a new BasicDBList with the same values as this instance
     */
    public Object copy() {
        // copy field values into new object
        BasicDBList newobj = new BasicDBList();
        // need to clone the sub obj
        for (int i = 0; i < size(); ++i) {
            Object val = get(i);
            if (val instanceof BasicDBObject) {
                val = ((BasicDBObject) val).copy();
            } else if (val instanceof BasicDBList) {
                val = ((BasicDBList) val).copy();
            }
            newobj.add(val);
        }
        return newobj;
    }

    /**
     * Whether the object is partial
     */
    private boolean _isPartialObject;
}
