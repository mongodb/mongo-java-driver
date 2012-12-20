/**
 * 
 */
package com.google.code.morphia.mapping.lazy;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

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

/**
 * i have to admit, there are plenty of open questions for me on that
 * Key-business...
 * 
 * @author uwe schaefer
 */
@SuppressWarnings("unchecked")
public class CGLibLazyProxyFactory implements LazyProxyFactory {
	private final CglibProxyFactory factory = new CglibProxyFactory();
	public CGLibLazyProxyFactory() {
	}

	public <T> T createProxy(final Class<T> targetClass, final Key<T> key,
			final DatastoreProvider p) {

		SerializableEntityObjectReference objectReference = new SerializableEntityObjectReference(
				targetClass, p, key);
		
		T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[] { targetClass,
				Serializable.class }, factory,
				objectReference, DelegationMode.SIGNATURE).proxy();


		T proxy = Dispatching.proxy(targetClass,
				new Class[] { ProxiedEntityReference.class, targetClass, Serializable.class }).with(objectReference,
				backend).build(factory);
		
		return proxy;

	}

	public <T extends Collection> T createListProxy(final T listToProxy,
			final Class referenceObjClass, final boolean ignoreMissing,
			final DatastoreProvider p) {
		Class<? extends Collection> targetClass = listToProxy.getClass();
		SerializableCollectionObjectReference objectReference = new SerializableCollectionObjectReference(
				listToProxy, referenceObjClass, ignoreMissing, p);

		T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[] { targetClass, Serializable.class }, factory,
				objectReference, DelegationMode.SIGNATURE).proxy();
		T proxy = (T) Dispatching.proxy(targetClass,
				new Class[] { ProxiedEntityReferenceList.class, targetClass, Serializable.class }).with(
				objectReference, backend).build(factory);
		
		return proxy;

	}

	public <T extends Map> T createMapProxy(final T mapToProxy,
			final Class referenceObjClass, final boolean ignoreMissing,
			final DatastoreProvider p) {
		Class<? extends Map> targetClass = mapToProxy.getClass();
		SerializableMapObjectReference objectReference = new SerializableMapObjectReference(
				mapToProxy, referenceObjClass, ignoreMissing, p);

		T backend = (T) new NonFinalizingHotSwappingInvoker(new Class[] { targetClass, Serializable.class }, factory,
				objectReference, DelegationMode.SIGNATURE).proxy();
		T proxy = (T) Dispatching.proxy(targetClass,
				new Class[] { ProxiedEntityReferenceMap.class, targetClass, Serializable.class }).with(objectReference,
				backend).build(factory);

		return proxy;

	}
}
