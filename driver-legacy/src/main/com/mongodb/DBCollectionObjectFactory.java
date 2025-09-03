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
final class DBCollectionObjectFactory implements DBObjectFactory {

    private final Map<List<String>, Class<? extends DBObject>> pathToClassMap;

    DBCollectionObjectFactory() {
        this(Collections.emptyMap());
    }

    private DBCollectionObjectFactory(final Map<List<String>, Class<? extends DBObject>> pathToClassMap) {
        this.pathToClassMap = pathToClassMap;
    }

    @Override
    public DBObject getInstance() {
        return getInstance(Collections.emptyList());
    }

    @Override
    public DBObject getInstance(final List<String> path) {
        Class<? extends DBObject> aClass = getClassForPath(path);
        try {
            return aClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw createInternalException(aClass, e);
        } catch (InvocationTargetException e) {
            throw createInternalException(aClass, e.getTargetException());
        }
    }

    public DBCollectionObjectFactory update(final Class<? extends DBObject> aClass) {
        return new DBCollectionObjectFactory(updatePathToClassMap(aClass, Collections.emptyList()));
    }

    public DBCollectionObjectFactory update(final Class<? extends DBObject> aClass, final List<String> path) {
        return new DBCollectionObjectFactory(updatePathToClassMap(aClass, path));
    }

    private Map<List<String>, Class<? extends DBObject>> updatePathToClassMap(final Class<? extends DBObject> aClass,
                                                                              final List<String> path) {
        Map<List<String>, Class<? extends DBObject>> map = new HashMap<>(pathToClassMap);
        if (aClass != null) {
            map.put(path, aClass);
        } else {
            map.remove(path);
        }
        return map;
    }

    Class<? extends DBObject> getClassForPath(final List<String> path) {
        return pathToClassMap.getOrDefault(path, BasicDBObject.class);
    }

    private MongoInternalException createInternalException(final Class<? extends DBObject> aClass, final Throwable e) {
        throw new MongoInternalException("Can't instantiate class " + aClass, e);
    }
}
