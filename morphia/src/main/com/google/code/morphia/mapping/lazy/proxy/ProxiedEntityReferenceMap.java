/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.util.Map;

import com.google.code.morphia.Key;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
public interface ProxiedEntityReferenceMap extends ProxiedReference {

	void __put(String key, Key<?> referenceKey);
	
	Map<String, Key<?>> __getReferenceMap();
}
