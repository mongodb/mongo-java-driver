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

/**
 *
 */
package com.google.code.morphia.mapping.lazy.proxy;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SerializableCollectionObjectReference<T> extends AbstractReference implements ProxiedEntityReferenceList {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final ArrayList<Key<?>> listOfKeys;

    public SerializableCollectionObjectReference(final Collection<T> type, final Class<T> referenceObjClass,
                                                 final boolean ignoreMissing, final DatastoreProvider p) {

        super(p, referenceObjClass, ignoreMissing);

        object = type;
        listOfKeys = new ArrayList<Key<?>>();
    }

    @Override
    protected synchronized Object fetch() {
        final Collection<T> c = (Collection<T>) object;
        c.clear();

        final int numberOfEntitiesExpected = listOfKeys.size();
        // does not retain order:
        // List<T> retrievedEntities = p.get().getByKeys(referenceObjClass,
        // (List) __getKeysAsList());

        // so we do it the lousy way: FIXME
        final List<T> retrievedEntities = new ArrayList<T>(listOfKeys.size());
        final Datastore ds = p.get();
        for (final Key<?> k : listOfKeys) {
            retrievedEntities.add((T) ds.getByKey(referenceObjClass, k));
        }

        if (!ignoreMissing && (numberOfEntitiesExpected != retrievedEntities.size())) {
            throw new LazyReferenceFetchingException("During the lifetime of a proxy of type '"
                                                     + c.getClass().getSimpleName() + "', " +
                                                     "some referenced Entities of type '"
                                                     + referenceObjClass.getSimpleName() + "' have " +
                                                     "disappeared from the Datastore.");
        }

        c.addAll(retrievedEntities);
        return c;
    }

    public List<Key<?>> __getKeysAsList() {
        return Collections.unmodifiableList(listOfKeys);
    }

    public void __add(final Key key) {
        listOfKeys.add(key);
    }

    public void __addAll(final Collection<? extends Key<?>> keys) {
        listOfKeys.addAll(keys);
    }

    @Override
    protected void beforeWriteObject() {
        if (__isFetched()) {
            syncKeys();
            ((Collection<T>) object).clear();
        }
    }

    private void syncKeys() {
        final Datastore ds = p.get();

        listOfKeys.clear();
        for (final Object e : ((Collection) object)) {
            listOfKeys.add(ds.getKey(e));
        }
    }
}