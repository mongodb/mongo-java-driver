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

import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SerializableEntityObjectReference extends AbstractReference implements ProxiedEntityReference {
    private static final long serialVersionUID = 1L;
    private final Key key;

    public SerializableEntityObjectReference(final Class targetClass,
                                             final DatastoreProvider p, final Key key) {

        super(p, targetClass, false);
        this.key = key;
    }

    public Key __getKey() {
        return key;
    }

    @Override
    protected Object fetch() {

        final Object entity = p.get().getByKey(referenceObjClass, key);
        if (entity == null) {
            throw new LazyReferenceFetchingException(
                                                    "During the lifetime of the proxy, the Entity identified by '"
                                                    + key + "' disappeared from the Datastore.");
        }
        return entity;
    }

    @Override
    protected void beforeWriteObject() {
        object = null;
    }
}