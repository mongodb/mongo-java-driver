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
package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReference;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReferenceList;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReferenceMap;
import com.google.code.morphia.mapping.lazy.proxy.SerializableCollectionObjectReference;
import com.google.code.morphia.mapping.lazy.proxy.SerializableEntityObjectReference;
import com.google.code.morphia.mapping.lazy.proxy.SerializableMapObjectReference;
import com.thoughtworks.proxy.factory.CglibProxyFactory;
import com.thoughtworks.proxy.toys.delegate.DelegationMode;
import com.thoughtworks.proxy.toys.dispatch.Dispatching;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * i have to admit, there are plenty of open questions for me on that Key-business...
 *
 * @author uwe schaefer
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CGLibLazyProxyFactory implements LazyProxyFactory {
    private final CglibProxyFactory factory = new CglibProxyFactory();

    public CGLibLazyProxyFactory() {
    }

    public <T> T createProxy(final Class<T> targetClass, final Key<T> key,
                             final DatastoreProvider p) {

        final SerializableEntityObjectReference objectReference = new SerializableEntityObjectReference(
                                                                                                       targetClass, p,
                                                                                                       key);

        final T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[]{targetClass,
                                                                              Serializable.class}, factory,
                                                                 objectReference, DelegationMode.SIGNATURE).proxy();


        final T proxy = Dispatching.proxy(targetClass,
                                         new Class[]{ProxiedEntityReference.class, targetClass,
                                                     Serializable.class}).with(objectReference,


                                                                              backend).build(factory);

        return proxy;

    }

    public <T extends Collection> T createListProxy(final T listToProxy,
                                                    final Class referenceObjClass, final boolean ignoreMissing,
                                                    final DatastoreProvider p) {
        final Class<? extends Collection> targetClass = listToProxy.getClass();
        final SerializableCollectionObjectReference objectReference = new SerializableCollectionObjectReference(













                                                                                                               listToProxy,
                                                                                                               referenceObjClass,
                                                                                                               ignoreMissing,
                                                                                                               p);

        final T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[]{targetClass, Serializable.class}, factory,
                                                                 objectReference, DelegationMode.SIGNATURE).proxy();
        final T proxy = (T) Dispatching.proxy(targetClass,
                                             new Class[]{ProxiedEntityReferenceList.class, targetClass,
                                                         Serializable.class}).with(
                                                                                  objectReference, backend)
                                       .build(factory);

        return proxy;

    }

    public <T extends Map> T createMapProxy(final T mapToProxy,
                                            final Class referenceObjClass, final boolean ignoreMissing,
                                            final DatastoreProvider p) {
        final Class<? extends Map> targetClass = mapToProxy.getClass();
        final SerializableMapObjectReference objectReference = new SerializableMapObjectReference(
                                                                                                 mapToProxy,
                                                                                                 referenceObjClass,
                                                                                                 ignoreMissing, p);

        final T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[]{targetClass, Serializable.class}, factory,
                                                                 objectReference, DelegationMode.SIGNATURE).proxy();
        final T proxy = (T) Dispatching.proxy(targetClass,
                                             new Class[]{ProxiedEntityReferenceMap.class, targetClass,
                                                         Serializable.class}).with(objectReference,
                                                                                  backend).build(factory);

        return proxy;

    }
}
