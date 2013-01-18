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
 */

/**
 *
 */
package com.google.code.morphia.mapping.lazy.proxy;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings("unchecked")
public class SerializableMapObjectReference extends AbstractReference implements ProxiedEntityReferenceMap {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final HashMap<String, Key<?>> keyMap;

    public SerializableMapObjectReference(final Map mapToProxy, final Class referenceObjClass,
                                          final boolean ignoreMissing, final DatastoreProvider p) {

        super(p, referenceObjClass, ignoreMissing);
        object = mapToProxy;
        keyMap = new LinkedHashMap<String, Key<?>>();
    }

    public void __put(final String key, final Key k) {
        keyMap.put(key, k);
    }

    @Override
    protected Object fetch() {
        final Map m = (Map) object;
        m.clear();
        // TODO us: change to getting them all at once and yell according to
        // ignoreMissing in order to a) increase performance and b) resolve
        // equals keys to the same instance
        // that should really be done in datastore.
        for (final Map.Entry<?, Key<?>> e : keyMap.entrySet()) {
            final Key<?> entityKey = e.getValue();
            final Object entity = fetch(entityKey);
            m.put(e.getKey(), entity);
        }
        return m;
    }

    @Override
    protected void beforeWriteObject() {
        if (!__isFetched()) {
            return;
        }
        else {
            syncKeys();
            ((Map) object).clear();
        }
    }

    private void syncKeys() {
        final Datastore ds = p.get();

        this.keyMap.clear();
        final Map<String, Object> map = (Map) object;
        for (final Map.Entry<String, Object> e : map.entrySet()) {
            keyMap.put(e.getKey(), ds.getKey(e.getValue()));
        }
    }

    public Map<String, Key<?>> __getReferenceMap() {
        return keyMap;
    }

}
