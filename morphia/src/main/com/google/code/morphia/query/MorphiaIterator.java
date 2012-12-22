package com.google.code.morphia.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * 
 * @author Scott Hernandez
 */
@SuppressWarnings("unchecked")
public class MorphiaIterator<T, V> implements Iterable<V>, Iterator<V>{
	protected final Iterator<DBObject> wrapped;
	protected final Mapper m;
	protected final Class<T> clazz;
	protected final String kind;
	protected final EntityCache cache;
	protected long  driverTime = 0;
	protected long  mapperTime= 0;

	public MorphiaIterator(Iterator<DBObject> it, Mapper m, Class<T> clazz, String kind, EntityCache cache) {
		this.wrapped = it;
		this.m = m;
		this.clazz = clazz;
		this.kind = kind;
		this.cache = cache;
	}
	
	public Iterator<V> iterator() {
		return this;
	}
	
	public boolean hasNext() {
		if(wrapped == null) return false;
    	long start = System.currentTimeMillis();
		boolean ret = wrapped.hasNext();
    	driverTime += System.currentTimeMillis() - start;
		return ret;
	}
	
	public V next() {
		if(!hasNext()) throw new NoSuchElementException();
    	DBObject dbObj = getNext();
    	return processItem(dbObj);
	}
	
	protected V processItem(DBObject dbObj) {
    	long start = System.currentTimeMillis();
		V item = convertItem(dbObj);
    	mapperTime += System.currentTimeMillis() - start;
		return (V) item;
	}
	
	protected DBObject getNext() {
		long start = System.currentTimeMillis();
		DBObject dbObj = (DBObject) wrapped.next();
    	driverTime += System.currentTimeMillis() - start;
    	return dbObj;
	}
	
	protected V convertItem(DBObject dbObj) {
		return (V) m.fromDBObject(clazz, dbObj, cache);
	}
	
	public void remove() {
		long start = System.currentTimeMillis();
		wrapped.remove();
    	driverTime += System.currentTimeMillis() - start;
	}
	
	/** Returns the time spent calling the driver in ms */
	public long getDriverTime() {
		return driverTime;
	}
	
	/** Returns the time spent calling the mapper in ms */
	public long getMapperTime() {
		return mapperTime;
	}
	
	public DBCursor getCursor() {
		return (DBCursor)wrapped;
	}
	
	public void close() {
		if (wrapped != null && wrapped instanceof DBCursor)
			((DBCursor)wrapped).close();
	}
}