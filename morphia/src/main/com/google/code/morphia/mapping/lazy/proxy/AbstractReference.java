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
import com.thoughtworks.proxy.kit.ObjectReference;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractReference implements Serializable, ObjectReference, ProxiedReference {

    private static final long serialVersionUID = 1L;
    protected final DatastoreProvider p;
    protected final boolean ignoreMissing;
    protected Object object;
    private boolean isFetched = false;
    protected final Class referenceObjClass;

    protected AbstractReference(final DatastoreProvider p,
                                final Class referenceObjClass, final boolean ignoreMissing) {
        this.p = p;
        this.referenceObjClass = referenceObjClass;
        this.ignoreMissing = ignoreMissing;
    }

    public final synchronized Object get() {
        if (isFetched) {
            return object;
        }

        object = fetch();
        isFetched = true;
        return object;
    }

    protected abstract Object fetch();

    public final void set(final Object arg0) {
        throw new UnsupportedOperationException();
    }

    public final boolean __isFetched() {
        return isFetched;
    }

    protected final Object fetch(final Key<?> id) {
        return p.get().getByKey(referenceObjClass, id);
    }


    private void writeObject(final java.io.ObjectOutputStream out)
    throws IOException {
        // excessive hoop-jumping in order not to have to recreate the
        // instance.
        // as soon as weÂ´d have an ObjectFactory, that would be unnecessary
        beforeWriteObject();
        isFetched = false;
        out.defaultWriteObject();
    }

    protected void beforeWriteObject() {
    }

    public final Class __getReferenceObjClass() {
        return referenceObjClass;
    }

    public Object __unwrap() {
        return get();
    }
}
