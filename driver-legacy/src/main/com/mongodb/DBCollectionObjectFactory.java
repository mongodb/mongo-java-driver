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

import com.mongodb.annotations.Immutable;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Immutable
@SuppressWarnings("deprecation")
final class DBCollectionObjectFactory implements DBObjectFactory {

    private final Map<List<String>, Class<? extends DBObject>> pathToClassMap;
    private final ReflectionDBObject.JavaWrapper wrapper;

    DBCollectionObjectFactory() {
        this(Collections.<List<String>, Class<? extends DBObject>>emptyMap(), null);
    }

    private DBCollectionObjectFactory(final Map<List<String>, Class<? extends DBObject>> pathToClassMap,
                                      final ReflectionDBObject.JavaWrapper wrapper) {
        this.pathToClassMap = pathToClassMap;
        this.wrapper = wrapper;
    }

    @Override
    public DBObject getInstance() {
        return getInstance(Collections.<String>emptyList());
    }

    @Override
    public DBObject getInstance(final List<String> path) {
        Class<? extends DBObject> aClass = getClassForPath(path);
        try {
            return aClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException e) {
            throw createInternalException(aClass, e);
        } catch (IllegalAccessException e) {
            throw createInternalException(aClass, e);
        } catch (NoSuchMethodException e) {
            throw createInternalException(aClass, e);
        } catch (InvocationTargetException e) {
            throw createInternalException(aClass, e.getTargetException());
        }
    }

    public DBCollectionObjectFactory update(final Class<? extends DBObject> aClass) {
        return new DBCollectionObjectFactory(updatePathToClassMap(aClass, Collections.<String>emptyList()),
                                             isReflectionDBObject(aClass) ? ReflectionDBObject.getWrapper(aClass) : wrapper);
    }

    public DBCollectionObjectFactory update(final Class<? extends DBObject> aClass, final List<String> path) {
        return new DBCollectionObjectFactory(updatePathToClassMap(aClass, path), wrapper);
    }

    private Map<List<String>, Class<? extends DBObject>> updatePathToClassMap(final Class<? extends DBObject> aClass,
                                                                              final List<String> path) {
        Map<List<String>, Class<? extends DBObject>> map = new HashMap<List<String>, Class<? extends DBObject>>(pathToClassMap);
        if (aClass != null) {
            map.put(path, aClass);
        } else {
            map.remove(path);
        }
        return map;
    }

    Class<? extends DBObject> getClassForPath(final List<String> path) {
        if (pathToClassMap.containsKey(path)) {
            return pathToClassMap.get(path);
        } else {
            Class<? extends DBObject> aClass = (wrapper != null) ? wrapper.getInternalClass(path) : null;
            return aClass != null ? aClass : BasicDBObject.class;
        }
    }

    private boolean isReflectionDBObject(final Class<? extends DBObject> aClass) {
        return aClass != null && ReflectionDBObject.class.isAssignableFrom(aClass);
    }

    private MongoInternalException createInternalException(final Class<? extends DBObject> aClass, final Throwable e) {
        throw new MongoInternalException("Can't instantiate class " + aClass, e);
    }
}
