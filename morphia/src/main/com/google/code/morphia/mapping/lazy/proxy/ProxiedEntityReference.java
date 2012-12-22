/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import com.google.code.morphia.Key;

/**
 * @author Uwe Schaefer, (schaefer@thomas-daily.de)
 */
public interface ProxiedEntityReference extends ProxiedReference {
	Key<?> __getKey();
}
