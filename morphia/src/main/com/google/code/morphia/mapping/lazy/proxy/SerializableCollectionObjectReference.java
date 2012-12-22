/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;

@SuppressWarnings("unchecked")
public class SerializableCollectionObjectReference<T> extends AbstractReference implements ProxiedEntityReferenceList {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Key<?>> listOfKeys;
	
	public SerializableCollectionObjectReference(final Collection<T> type, final Class<T> referenceObjClass,
			final boolean ignoreMissing, final DatastoreProvider p) {
		
		super(p, referenceObjClass, ignoreMissing);
		
		object = type;
		listOfKeys = new ArrayList<Key<?>>();
	}
	
	@Override
	protected synchronized Object fetch() {
		Collection<T> c = (Collection<T>) object;
		c.clear();
		
		int numberOfEntitiesExpected = listOfKeys.size();
		// does not retain order:
		// List<T> retrievedEntities = p.get().getByKeys(referenceObjClass,
		// (List) __getKeysAsList());
		
		// so we do it the lousy way: FIXME
		List<T> retrievedEntities = new ArrayList<T>(listOfKeys.size());
		Datastore ds = p.get();
		for (Key<?> k : listOfKeys) {
			retrievedEntities.add((T) ds.getByKey(referenceObjClass, k));
		}
		
		if (!ignoreMissing && (numberOfEntitiesExpected != retrievedEntities.size())) {
			throw new LazyReferenceFetchingException("During the lifetime of a proxy of type '"
					+ c.getClass().getSimpleName() + "', some referenced Entities of type '"
					+ referenceObjClass.getSimpleName() + "' have disappeared from the Datastore.");
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
		if (!__isFetched())
			return;
		else {
			syncKeys();
			((Collection<T>) object).clear();
		}
	}
	
	private void syncKeys() {
		Datastore ds = p.get();
		
		listOfKeys.clear();
		for (Object e : ((Collection) object)) {
			listOfKeys.add(ds.getKey(e));
		}
	}
}