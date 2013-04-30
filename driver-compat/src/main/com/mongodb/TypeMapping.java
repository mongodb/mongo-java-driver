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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO This needs to be rethinked.
 */
public class TypeMapping {
    private static final List<String> EMPTY_PATH = Collections.emptyList();

    private final Map<List<String>, Class<? extends DBObject>> pathToClassMap;
    private ReflectionDBObject.JavaWrapper wrapper;

    public TypeMapping(final Class<? extends DBObject> topLevelClass) {
        this(topLevelClass, new HashMap<String, Class<? extends DBObject>>());
    }

    public TypeMapping(final Class<? extends DBObject> topLevelClass,
                       final Map<String, Class<? extends DBObject>> stringPathToClassMap) {
        this.pathToClassMap = createPathToClassMap(topLevelClass, stringPathToClassMap);

        updateWrapper(topLevelClass);
    }

    private void updateWrapper(final Class<? extends DBObject> topLevelClass) {
        if (ReflectionDBObject.class.isAssignableFrom(topLevelClass)) {
            wrapper = ReflectionDBObject.getWrapper(topLevelClass);
        } else {
            wrapper = null;
        }
    }

    public Class<? extends DBObject> getTopLevelClass() {
        return pathToClassMap.get(EMPTY_PATH);
    }

    public synchronized void setTopLevelClass(final Class<? extends DBObject> cls) {
        setInternalClass(EMPTY_PATH, cls);
        updateWrapper(cls);
    }

    public synchronized void setInternalClass(final List<String> path, final Class<? extends DBObject> cls) {
        pathToClassMap.put(path, cls);
    }

    public Class<? extends DBObject> getInternalClass(final List<String> path) {
        return pathToClassMap.get(path);
    }

    public Map<List<String>, Class<? extends DBObject>> getPathToClassMap() {
        return pathToClassMap;
    }

    public DBObject getNewInstance(final List<String> path) {
        final Class<? extends DBObject> cls = getClassForNewObject(path);
        try {
            return cls.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new MongoInternalException("Can't create a new instance of class " + cls, e);
        }
    }

    private Class<? extends DBObject> getClassForNewObject(final List<String> path) {
        if (pathToClassMap.containsKey(path)) {
            return pathToClassMap.get(path);
        } else {
            Class<? extends DBObject> cls = null;
            if (wrapper != null) {
                cls = wrapper.getInternalClass(path);
            }
            if (cls == null) {
                cls = BasicDBObject.class;
            }
            return cls;
        }
    }

    private Map<List<String>, Class<? extends DBObject>> createPathToClassMap(
            final Class<? extends DBObject> topLevelClass,
            final Map<String, Class<? extends DBObject>> stringPathToClassMap) {
        final Map<List<String>, Class<? extends DBObject>> pathToClassMap
                = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(EMPTY_PATH, topLevelClass);
        for (final Map.Entry<String, Class<? extends DBObject>> cur : stringPathToClassMap.entrySet()) {
            final List<String> path = Arrays.asList(cur.getKey().split("\\."));
            pathToClassMap.put(path, cur.getValue());
        }

        return pathToClassMap;
    }
}
