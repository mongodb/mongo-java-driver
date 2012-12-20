/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
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
		Map m = (Map) object;
		m.clear();
		// TODO us: change to getting them all at once and yell according to
		// ignoreMissing in order to a) increase performance and b) resolve
		// equals keys to the same instance
		// that should really be done in datastore.
		for (Map.Entry<?, Key<?>> e : keyMap.entrySet()) {
			Key<?> entityKey = e.getValue();
			Object entity = fetch(entityKey);
			m.put(e.getKey(), entity);
		}
		return m;
	}

	@Override
	protected void beforeWriteObject() {
		if (!__isFetched())
			return;
		else {
			syncKeys();
			((Map) object).clear();
		}
	}
	
	private void syncKeys() {
		Datastore ds = p.get();
		
		this.keyMap.clear();
		Map<String, Object> map = (Map) object;
		for (Map.Entry<String, Object> e : map.entrySet()) {
			keyMap.put(e.getKey(), ds.getKey(e.getValue()));
		}
	}

	public Map<String, Key<?>> __getReferenceMap() {
		return keyMap;
	}

}
