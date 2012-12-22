/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
@SuppressWarnings("unchecked")
public class ProxyHelper {

	public static <T> T unwrap(final T entity) {
		if (isProxy(entity)) {
			return (T) asProxy(entity).__unwrap();
		}
		return entity;
	}
	
	private static <T> ProxiedReference asProxy(final T entity) {
		return ((ProxiedReference) entity);
	}
	
	public static boolean isProxy(Object entity) {
		return (entity != null && isProxied(entity.getClass()));
	}
	
	public static boolean isProxied(Class<?> clazz) {
		return ProxiedReference.class.isAssignableFrom(clazz);
	}

	public static Class getReferentClass(Object entity) {
		if (isProxy(entity)) return asProxy(entity).__getReferenceObjClass();
		else
			return entity != null ? entity.getClass() : null;
	}
	
	public static boolean isFetched(Object entity) {
		if (entity == null)
			return true;
		if (!isProxy(entity))
			return true;
		return asProxy(entity).__isFetched();
	}
	
	public static boolean isUnFetched(Object entity) {
		return !isFetched(entity);
	}
}
