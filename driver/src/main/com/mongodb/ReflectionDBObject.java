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

import org.bson.BSONObject;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class enables to map simple Class fields to a BSON object fields
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ReflectionDBObject implements DBObject {

    public Object get(final String key) {
        return getWrapper().get(this, key);
    }

    public Set<String> keySet() {
        return getWrapper().keySet();
    }

    /**
     * @deprecated
     */
    @Deprecated
    public boolean containsKey(final String key) {
        return containsField(key);
    }

    public boolean containsField(final String s) {
        return getWrapper().containsKey(s);
    }

    public Object put(final String key, final Object v) {
        return getWrapper().set(this, key, v);
    }

    @SuppressWarnings("unchecked")
    public void putAll(final Map m) {
        for (final Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    public void putAll(final BSONObject o) {
        for (final String k : o.keySet()) {
            put(k, o.get(k));
        }
    }

    /**
     * Gets the _id
     *
     * @return the _id of this document
     */
    public Object get_id() {
        return _id;
    }

    /**
     * Sets the _id
     *
     * @param id the unique identifier for this DBObject
     */
    public void set_id(final Object id) {
        _id = id;
    }

    public boolean isPartialObject() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public Map toMap() {
        Map m = new HashMap();
        Iterator i = this.keySet().iterator();
        while (i.hasNext()) {
            Object s = i.next();
            m.put(s, this.get(s + ""));
        }
        return m;
    }

    /**
     * ReflectionDBObjects can't be partial. This operation is not supported.
     */
    public void markAsPartialObject() {
        throw new RuntimeException("ReflectionDBObjects can't be partial");
    }

    /**
     * This operation is not supported.
     */
    public Object removeField(final String key) {
        throw new UnsupportedOperationException("can't remove from a ReflectionDBObject");
    }

    JavaWrapper getWrapper() {
        if (_wrapper != null) {
            return _wrapper;
        }

        _wrapper = getWrapper(this.getClass());
        return _wrapper;
    }

    JavaWrapper _wrapper;
    Object _id;

    /**
     * Represents a wrapper around the DBObject to interface with the Class fields
     */
    public static class JavaWrapper {
        JavaWrapper(final Class c) {
            clazz = c;
            name = c.getName();

            fields = new TreeMap<String, FieldInfo>();
            for (final Method m : c.getMethods()) {
                if (!(m.getName().startsWith("get") || m.getName().startsWith("set"))) {
                    continue;
                }

                String name = m.getName().substring(3);
                if (name.length() == 0 || IGNORE_FIELDS.contains(name)) {
                    continue;
                }

                Class type = m.getName().startsWith("get") ? m.getReturnType() : m.getParameterTypes()[0];

                FieldInfo fi = fields.get(name);
                if (fi == null) {
                    fi = new FieldInfo(name, type);
                    fields.put(name, fi);
                }

                if (m.getName().startsWith("get")) {
                    fi.getter = m;
                } else {
                    fi.setter = m;
                }
            }

            Set<String> names = new HashSet<String>(fields.keySet());
            for (final String name : names) {
                if (!fields.get(name).ok()) {
                    fields.remove(name);
                }
            }

            keys = Collections.unmodifiableSet(fields.keySet());
        }

        public Set<String> keySet() {
            return keys;
        }

        /**
         * @deprecated
         */
        @Deprecated
        public boolean containsKey(final String key) {
            return keys.contains(key);
        }

        public Object get(final ReflectionDBObject t, final String name) {
            FieldInfo i = fields.get(name);
            if (i == null) {
                return null;
            }
            try {
                return i.getter.invoke(t);
            } catch (Exception e) {
                throw new RuntimeException("could not invoke getter for [" + name + "] on [" + this.name + "]", e);
            }
        }

        public Object set(final ReflectionDBObject t, final String name, final Object val) {
            FieldInfo i = fields.get(name);
            if (i == null) {
                throw new IllegalArgumentException("no field [" + name + "] on [" + this.name + "]");
            }
            try {
                return i.setter.invoke(t, val);
            } catch (Exception e) {
                throw new RuntimeException("could not invoke setter for [" + name + "] on [" + this.name + "]", e);
            }
        }

        Class<? extends DBObject> getInternalClass(final List<String> path) {
            String cur = path.get(0);

            FieldInfo fi = fields.get(cur);
            if (fi == null) {
                return null;
            }

            if (path.size() == 1) {
                return fi.clazz;
            }

            JavaWrapper w = getWrapperIfReflectionObject(fi.clazz);
            if (w == null) {
                return null;
            }
            return w.getInternalClass(path.subList(1, path.size()));
        }

        final Class clazz;
        final String name;
        final Map<String, FieldInfo> fields;
        final Set<String> keys;
    }

    static class FieldInfo {
        FieldInfo(final String name, final Class<? extends DBObject> clazz) {
            this.name = name;
            this.clazz = clazz;
        }

        boolean ok() {
            return getter != null && setter != null;
        }

        final String name;
        final Class<? extends DBObject> clazz;
        Method getter;
        Method setter;
    }

    /**
     * Returns the wrapper if this object can be assigned from this class.
     *
     * @param c the class to be wrapped
     * @return the wrapper
     */
    public static JavaWrapper getWrapperIfReflectionObject(final Class c) {
        if (ReflectionDBObject.class.isAssignableFrom(c)) {
            return getWrapper(c);
        }
        return null;
    }

    /**
     * Returns an existing Wrapper instance associated with a class, or creates a new one.
     *
     * @param c the class to be wrapped
     * @return the wrapped
     */
    public static JavaWrapper getWrapper(final Class c) {
        JavaWrapper w = _wrappers.get(c);
        if (w == null) {
            w = new JavaWrapper(c);
            _wrappers.put(c, w);
        }
        return w;
    }

    private static final Map<Class, JavaWrapper> _wrappers = Collections.synchronizedMap(
                                                                                            new HashMap<Class,
                                                                                                           JavaWrapper>());
    private static final Set<String> IGNORE_FIELDS = new HashSet<String>();

    static {
        IGNORE_FIELDS.add("Int");
    }

}
