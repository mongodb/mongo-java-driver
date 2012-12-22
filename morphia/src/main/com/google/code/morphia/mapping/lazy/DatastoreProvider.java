/**
 * 
 */
package com.google.code.morphia.mapping.lazy;

import java.io.Serializable;

import com.google.code.morphia.Datastore;

/**
 * Lightweight object to be created (hopefully by a factoy some day) to create
 * provide a Datastore-reference to a resolving Proxy. If this was created by a
 * common Object factory, it could make use of the current context (like Guice
 * Scopes etc.)
 * 
 * @see LazyProxyFactory
 * @author uwe schaefer
 */
public interface DatastoreProvider extends Serializable {
	Datastore get();
}
