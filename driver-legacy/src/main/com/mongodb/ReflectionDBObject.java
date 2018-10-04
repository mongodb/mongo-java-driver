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

import com.mongodb.lang.Nullable;
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

import static java.util.Collections.synchronizedMap;

/**
 * This class enables to map simple Class fields to a BSON object fields
 * @deprecated Replaced by {@link org.bson.codecs.pojo.PojoCodecProvider}
 */
@SuppressWarnings({"unchecked", "rawtypes"})
@Deprecated
public abstract class ReflectionDBObject implements DBObject {

    @Override
    @Nullable
    public Object get(final String key) {
        return getWrapper().get(this, key);
    }

    @Override
    public Set<String> keySet() {
        return getWrapper().keySet();
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean containsKey(final String key) {
        return containsField(key);
    }

    @Override
    public boolean containsField(final String fieldName) {
        return getWrapper().containsKey(fieldName);
    }

    @Override
    public Object put(final String key, final Object v) {
        return getWrapper().set(this, key, v);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(final Map m) {
        for (final Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    @Override
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

    @Override
    public boolean isPartialObject() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
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
     *
     * @throws RuntimeException ReflectionDBObjects can't be partial
     */
    @Override
    public void markAsPartialObject() {
        throw new RuntimeException("ReflectionDBObjects can't be partial");
    }

    /**
     * This operation is not supported.
     *
     * @param key The name of the field to remove
     * @return The value removed from this object
     * @throws UnsupportedOperationException can't remove from a ReflectionDBObject
     */
    @Override
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

    //CHECKSTYLE:OFF
    JavaWrapper _wrapper;
    Object _id;
    //CHECKSTYLE:ON

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

        /**
         * Gets all the fields on this object.
         *
         * @return a Set of all the field names.
         */
        public Set<String> keySet() {
            return keys;
        }

        /**
         * Whether the document this represents contains the given field.
         *
         * @param key a field name
         * @return true if the key exists
         * @deprecated
         */
        @Deprecated
        public boolean containsKey(final String key) {
            return keys.contains(key);
        }

        /**
         * Gets the value for the given field from the given document.
         *
         * @param document  a ReflectionDBObject representing a MongoDB document
         * @param fieldName the name of the field to get the value for
         * @return the value for the given field name
         */
        @Nullable
        public Object get(final ReflectionDBObject document, final String fieldName) {
            FieldInfo i = fields.get(fieldName);
            if (i == null) {
                return null;
            }
            try {
                return i.getter.invoke(document);
            } catch (Exception e) {
                throw new RuntimeException("could not invoke getter for [" + fieldName + "] on [" + this.name + "]", e);
            }
        }

        /**
         * Adds or sets the given field to the given value on the document.
         *
         * @param document  a ReflectionDBObject representing a MongoDB document
         * @param fieldName the name of the field to get the value for
         * @param value     the value to set the field to
         * @return the result of setting this value
         */
        public Object set(final ReflectionDBObject document, final String fieldName, final Object value) {
            FieldInfo i = fields.get(fieldName);
            if (i == null) {
                throw new IllegalArgumentException("no field [" + fieldName + "] on [" + this.name + "]");
            }
            try {
                return i.setter.invoke(document, value);
            } catch (Exception e) {
                throw new RuntimeException("could not invoke setter for [" + fieldName + "] on [" + this.name + "]", e);
            }
        }

        @Nullable
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

        //CHECKSTYLE:OFF
        final Class clazz;
        final String name;
        final Map<String, FieldInfo> fields;
        final Set<String> keys;
        //CHECKSTYLE:ON
    }

    static class FieldInfo {
        FieldInfo(final String name, final Class<? extends DBObject> clazz) {
            this.name = name;
            this.clazz = clazz;
        }

        boolean ok() {
            return getter != null && setter != null;
        }

        //CHECKSTYLE:OFF
        final String name;
        final Class<? extends DBObject> clazz;
        Method getter;
        Method setter;
        //CHECKSTYLE:ON
    }

    /**
     * Returns the wrapper if this object can be assigned from this class.
     *
     * @param c the class to be wrapped
     * @return the wrapper
     */
    @Nullable
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

    private static final Map<Class, JavaWrapper> _wrappers = synchronizedMap(new HashMap<Class, JavaWrapper>());
    private static final Set<String> IGNORE_FIELDS = new HashSet<String>();

    static {
        IGNORE_FIELDS.add("Int");
    }

}
