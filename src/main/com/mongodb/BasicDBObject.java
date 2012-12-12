/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import java.util.LinkedHashMap;
import java.util.Map;

public class BasicDBObject extends LinkedHashMap<String, Object> implements DBObject {
    @Override
    public void putAll(final Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(final String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map toMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object removeField(final String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(final String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsField(final String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markAsPartialObject() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPartialObject() {
        throw new UnsupportedOperationException();
    }
}
