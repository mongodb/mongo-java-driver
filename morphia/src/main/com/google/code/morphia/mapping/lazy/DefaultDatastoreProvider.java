package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.Datastore;

/**
 * Default implementation to be used in the assumtion that one Datastore per
 * classloader is the default wa to use Morphia. Might be discussable.
 * 
 * @author uwe schaefer
 */
public class DefaultDatastoreProvider implements DatastoreProvider {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Datastore get() {
		final Datastore datastore = DatastoreHolder.getInstance().get();
		if (datastore == null) {
			throw new IllegalStateException(
					"DatastoreHolder does not carry a Datastore.");
		}
		return datastore;
	}

}