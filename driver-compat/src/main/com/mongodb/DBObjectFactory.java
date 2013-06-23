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

import org.mongodb.annotations.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Immutable
public final class DBObjectFactory {

    private final Map<List<String>, Class<? extends DBObject>> pathToClassMap;
    private final ReflectionDBObject.JavaWrapper wrapper;

    public DBObjectFactory() {
        this(Collections.<List<String>, Class<? extends DBObject>>emptyMap(), null);
    }

    private DBObjectFactory(final Map<List<String>, Class<? extends DBObject>> pathToClassMap, final ReflectionDBObject.JavaWrapper wrapper) {
        this.pathToClassMap = pathToClassMap;
        this.wrapper = wrapper;
    }

    public DBObject getInstance() {
        return getInstance(Collections.<String>emptyList());
    }

    public DBObject getInstance(final List<String> path) {
        final Class<? extends DBObject> aClass = getClassForPath(path);
        try {
            return aClass.newInstance();
        } catch (InstantiationException e) {
            throw createInternalException(aClass, e);
        } catch (IllegalAccessException e) {
            throw createInternalException(aClass, e);
        }
    }

    public DBObjectFactory update(final Class<? extends DBObject> aClass) {
        return new DBObjectFactory(
                updatePathToClassMap(aClass, Collections.<String>emptyList()),
                isReflectionDBObject(aClass) ? ReflectionDBObject.getWrapper(aClass) : wrapper
        );
    }

    public DBObjectFactory update(final Class<? extends DBObject> aClass, final List<String> path) {
        return new DBObjectFactory(
                updatePathToClassMap(aClass, path),
                wrapper
        );
    }

    private Map<List<String>, Class<? extends DBObject>> updatePathToClassMap(Class<? extends DBObject> aClass, List<String> path) {
        final Map<List<String>, Class<? extends DBObject>> map
                = new HashMap<List<String>, Class<? extends DBObject>>(pathToClassMap);
        map.put(path, aClass);
        return map;
    }

    private Class<? extends DBObject> getClassForPath(final List<String> path) {
        if (pathToClassMap.containsKey(path)) {
            return pathToClassMap.get(path);
        } else {
            final Class<? extends DBObject> aClass = (wrapper != null)
                    ? wrapper.getInternalClass(path)
                    : null;
            return aClass != null ? aClass : BasicDBObject.class;
        }
    }

    private boolean isReflectionDBObject(Class<? extends DBObject> aClass) {
        return ReflectionDBObject.class.isAssignableFrom(aClass);
    }

    private MongoInternalException createInternalException(final Class<? extends DBObject> aClass, final Exception e) {
        throw new MongoInternalException("Can't instantiate class " + aClass, e);
    }
}
