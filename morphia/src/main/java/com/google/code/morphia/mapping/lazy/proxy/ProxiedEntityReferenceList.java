/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.util.Collection;
import java.util.List;

import com.google.code.morphia.Key;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public interface ProxiedEntityReferenceList extends ProxiedReference {

	void __add(Key<?> key);
	void __addAll(Collection<? extends Key<?>> keys);

	List<Key<?>> __getKeysAsList();

}
