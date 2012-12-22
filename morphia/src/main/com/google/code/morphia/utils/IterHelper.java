package com.google.code.morphia.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.BSONObject;

/**
 * Helper to allow for optimizations for different types of Map/Collections
 * @author Scott Hernandez
 *
 * @param <T> The key type of the map
 * @param <V> The value type of the map/collection
 */
public class IterHelper<T, V> {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void loopMap(Object x, MapIterCallback<T, V> iter) {
		if( x == null)
			return;
		
		if(x instanceof Collection)
			throw new IllegalArgumentException("call loop instead");

		if (x instanceof HashMap<?,?>) {
			if (((HashMap) x).size() == 0) 
				return;
			
			HashMap<?,?> hm = (HashMap<?, ?>) x;
			for(Entry<?, ?> e : hm.entrySet())
				iter.eval((T)e.getKey(), (V)e.getValue());
			return;
		}
		if (x instanceof Map<?,?>) {
			Map<?,?> m = (Map<?, ?>) x;
			for(Object k : m.keySet())
				iter.eval((T)k, (V)m.get(k));
			return;
		}
		if (x instanceof BSONObject) {
			BSONObject m = (BSONObject) x;
			for(String k : m.keySet())
				iter.eval((T)k, (V)m.get(k));
			return;
		}
		
	}
	
	@SuppressWarnings({ "unchecked" })
	public void loop(Object x, IterCallback<V> iter) {
		if( x == null)
			return;
		
		if(x instanceof Map)
			throw new IllegalArgumentException("call loopMap instead");
		
		if(x instanceof List<?>) {
			List<?> l = (List<?>) x;
			for(Object o : l)
				iter.eval((V)o);
		}
	}
	
	/**
	 * Calls eval for each entry found, or just once if the "x" isn't iterable/collection/list/etc. with "x"
	 * @param x
	 * @param iter
	 */
	@SuppressWarnings({ "unchecked" })
	public void loopOrSingle(Object x, IterCallback<V> iter) {
		if (x == null)
			return;
	
		//A collection
		if (x instanceof Collection<?>) {
			Collection<?> l = (Collection<?>) x;
			for (Object o : l)
				iter.eval((V) o);
			return;
		}
		
		//An array of Object[]
		if (x.getClass().isArray()) {
			for (Object o : (Object[]) x)
				iter.eval((V) o);
			return;
		}
		
		iter.eval((V) x);
	}
	
	public static abstract class MapIterCallback<T, V> {
		public abstract void eval(T t, V v);
	}
	
	public static abstract class IterCallback<V> {
		public abstract void eval(V v);
	}
}
