/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.code.morphia.DatastoreImpl;
import com.google.code.morphia.Key;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.google.code.morphia.mapping.lazy.LazyFeatureDependencies;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReference;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReferenceList;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReferenceMap;
import com.google.code.morphia.mapping.lazy.proxy.ProxyHelper;
import com.google.code.morphia.utils.IterHelper;
import com.google.code.morphia.utils.IterHelper.IterCallback;
import com.google.code.morphia.utils.IterHelper.MapIterCallback;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

@SuppressWarnings({ "unchecked", "rawtypes" })
class ReferenceMapper implements CustomMapper {
	public static final Logr log = MorphiaLoggerFactory.get(ReferenceMapper.class);
	
	public void toDBObject(Object entity, MappedField mf, DBObject dbObject, Map<Object, DBObject> involvedObjects, Mapper mapr) {
		String name = mf.getNameToStore();
		
		Object fieldValue = mf.getFieldValue(entity);
		
		if (fieldValue == null && !mapr.getOptions().storeNulls)
			return;
		
		if (mf.isMap()) {
			writeMap(mf, dbObject, name, fieldValue, mapr);
		} else if (mf.isMultipleValues()) {
			writeCollection(mf, dbObject, name, fieldValue, mapr);
		} else {
			writeSingle(dbObject, name, fieldValue, mapr);
		}
		
	}
	
	private void writeSingle(DBObject dbObject, String name, Object fieldValue, Mapper mapr) {
		if (fieldValue == null) 
			if(mapr.getOptions().storeNulls)
				dbObject.put(name, null);
		
		DBRef dbrefFromKey = mapr.keyToRef(getKey(fieldValue, mapr));
		dbObject.put(name, dbrefFromKey);
	}
	
	private void writeCollection(MappedField mf, DBObject dbObject, String name, Object fieldValue, Mapper mapr) {
		if (fieldValue != null) {
			List values = new ArrayList();
			
			if (ProxyHelper.isProxy(fieldValue) && ProxyHelper.isUnFetched(fieldValue)) {
				ProxiedEntityReferenceList p = (ProxiedEntityReferenceList) fieldValue;
				List<Key<?>> getKeysAsList = p.__getKeysAsList();
				for (Key<?> key : getKeysAsList) {
					addValue(values, key, mapr);
				}
			} else {
				
				if (mf.getType().isArray()) {
					for (Object o : (Object[]) fieldValue) {
						addValue(values, o, mapr);
					}
				} else {
					for (Object o : (Iterable) fieldValue) {
						addValue(values, o, mapr);
					}
				}
			}
			if (values.size() > 0 || mapr.getOptions().storeEmpties) {
				dbObject.put(name, values);
			}
		}
	}
	
	private void addValue(List vals, Object o, Mapper mapr) {
		if (o == null && mapr.getOptions().storeNulls) {
			vals.add(null);
			return;
		}
		
		if (o instanceof Key)
			vals.add(mapr.keyToRef((Key)o));
		else
			vals.add(mapr.keyToRef(getKey(o, mapr)));
	}
	
	private void writeMap(final MappedField mf, final DBObject dbObject, String name, Object fieldValue,
			Mapper mapr) {
		Map<Object, Object> map = (Map<Object, Object>) fieldValue;
		if ((map != null)) {
			Map values = mapr.getOptions().objectFactory.createMap(mf);
			
			if (ProxyHelper.isProxy(map) && ProxyHelper.isUnFetched(map)) {
				ProxiedEntityReferenceMap proxy = (ProxiedEntityReferenceMap) map;
				
				Map<String, Key<?>> refMap = proxy.__getReferenceMap();
				for (Map.Entry<String, Key<?>> entry : refMap.entrySet()) {
					String strKey = entry.getKey();
					values.put(strKey, mapr.keyToRef(entry.getValue()));
				}
			} else {
				for (Map.Entry<Object, Object> entry : map.entrySet()) {
					String strKey = mapr.converters.encode(entry.getKey()).toString();
					values.put(strKey, mapr.keyToRef(getKey(entry.getValue(), mapr)));
				}
			}
			if (values.size() > 0 || mapr.getOptions().storeEmpties) {
				dbObject.put(name, values);
			}
		}
	}
	
	private Key<?> getKey(final Object entity, Mapper mapr) {
		try {
			if (entity instanceof ProxiedEntityReference) {
				ProxiedEntityReference proxy = (ProxiedEntityReference) entity;
				return proxy.__getKey();
			}
			MappedClass mappedClass = mapr.getMappedClass(entity);
			Object id = mappedClass.getIdField().get(entity);
			if (id == null)
				throw new MappingException("@Id field cannot be null!");
			Key key = new Key(mappedClass.getCollectionName(), id);
			return key;
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		}
	}
	
	/**
	 * @deprecated use void fromDBObject(final DBObject dbObject, final
	 *             MappedField mf, final Object entity, EntityCache cache)
	 *             instead.
	 */
	@Deprecated
	void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity, Mapper mapr) {
		fromDBObject(dbObject, mf, entity, mapr.createEntityCache(), mapr);
	}

	public void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity, EntityCache cache, Mapper mapr) {
		Class fieldType = mf.getType();
		
		Reference refAnn = mf.getAnnotation(Reference.class);
		if (mf.isMap()) {
			readMap(dbObject, mf, entity, refAnn, cache, mapr);
		} else if (mf.isMultipleValues()) {
			readCollection(dbObject, mf, entity, refAnn, cache, mapr);
		} else {
			readSingle(dbObject, mf, entity, fieldType, refAnn, cache, mapr);
		}
		
	}
	
	private void readSingle(final DBObject dbObject, final MappedField mf, final Object entity, Class fieldType,
			Reference refAnn, EntityCache cache, Mapper mapr) {
		Class referenceObjClass = fieldType;

		DBRef dbRef = (DBRef) mf.getDbObjectValue(dbObject);
		if (dbRef != null) {
			Object resolvedObject = null;
			if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
				if (exists(referenceObjClass, dbRef, cache, mapr)) {
					resolvedObject = createOrReuseProxy(referenceObjClass, dbRef, cache, mapr);
				} else {
					if (!refAnn.ignoreMissing()) {
						throw new MappingException("The reference(" + dbRef.toString() + ") could not be fetched for "
								+ mf.getFullName());
					}
				}
			} else {
				resolvedObject = resolveObject(dbRef, mf, cache, mapr);
			}
			
			if ( resolvedObject != null )
				mf.setFieldValue(entity, resolvedObject);
			
		}
	}
	
	private void readCollection(final DBObject dbObject, final MappedField mf, final Object entity, Reference refAnn,
			final EntityCache cache, final Mapper mapr) {
		// multiple references in a List
		Class referenceObjClass = mf.getSubClass();
		Collection references = mf.isSet() ? mapr.getOptions().objectFactory.createSet(mf) : mapr.getOptions().objectFactory.createList(mf);
		
		if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
			Object dbVal = mf.getDbObjectValue(dbObject);
			if (dbVal != null) {
				references = mapr.proxyFactory.createListProxy(references, referenceObjClass, refAnn.ignoreMissing(),
						mapr.datastoreProvider);
				ProxiedEntityReferenceList referencesAsProxy = (ProxiedEntityReferenceList) references;
				
				if (dbVal instanceof List) {
					List<DBRef> refList = (List) dbVal;
					DatastoreImpl dsi = (DatastoreImpl) mapr.datastoreProvider.get();
					List<Key<Object>> keys = dsi.getKeysByRefs(refList);
					
					if (keys.size() != refList.size()) {
						String msg = "Some of the references could not be fetched for " + mf.getFullName() + ". "
								+ refList + " != " + keys;
						if (!refAnn.ignoreMissing())
							throw new MappingException(msg);
						else
							log.warning(msg);
					}
					
					referencesAsProxy.__addAll(keys);
				} else {
					DBRef dbRef = (DBRef) dbVal;
					if (!exists(mf.getSubClass(), dbRef, cache, mapr)) {
						String msg = "The reference(" + dbRef.toString() + ") could not be fetched for "
								+ mf.getFullName();
						if (!refAnn.ignoreMissing())
							throw new MappingException(msg);
						else
							log.warning(msg);
					} else {
						referencesAsProxy.__add(mapr.refToKey(dbRef));
					}
				}
			}
		} else {
			Object dbVal = mf.getDbObjectValue(dbObject);
			final Collection refs = references;
			new IterHelper<String, Object>().loopOrSingle((Object)dbVal, new IterCallback<Object>() {
				@Override
				public void eval(Object val) {
					DBRef dbRef = (DBRef) val;
					Object ent = resolveObject(dbRef, mf, cache, mapr);
					if (ent == null)
						log.warning("Null reference found when retrieving value for " + mf.getFullName());
					else
						refs.add(ent);
				}});
		}
		
		if (mf.getType().isArray()) {
			mf.setFieldValue(entity, ReflectionUtils.convertToArray(mf.getSubClass(), ReflectionUtils.iterToList(references)));
		} else {
			mf.setFieldValue(entity, references);
		}
	}
	
	boolean exists(Class c, final DBRef dbRef, EntityCache cache, Mapper mapr) {
		Key key = mapr.refToKey(dbRef);
		Boolean cached = cache.exists(key);
		if (cached != null)
			return cached;

		DatastoreImpl dsi = (DatastoreImpl) mapr.datastoreProvider.get();

		DBCollection dbColl = dsi.getCollection(c);
		if (!dbColl.getName().equals(dbRef.getRef()))
			log.warning("Class " + c.getName() + " is stored in the '" + dbColl.getName()
					+ "' collection but a reference was found for this type to another collection, '" + dbRef.getRef()
					+ "'. The reference will be loaded using the class anyway. " + dbRef);
		boolean exists = (dsi.find(dbRef.getRef(), c).disableValidation().filter("_id", dbRef.getId()).asKeyList()
				.size() == 1);
		cache.notifyExists(key, exists);
		return exists;
	}
	
	Object resolveObject(final DBRef dbRef, final MappedField mf, EntityCache cache, Mapper mapr) {
		if (dbRef == null)
			return null;
		
		Key key = mapr.createKey(mf.isSingleValue() ? mf.getType() : mf.getSubClass(), dbRef.getId());
		
		Object cached = cache.getEntity(key);
		if (cached != null)
			return cached;
		
		//TODO: if _db is null, set it?
		DBObject refDbObject = (DBObject) dbRef.fetch();
		
		if (refDbObject != null) {
			Object refObj = mapr.getOptions().objectFactory.createInstance(mapr, mf, refDbObject);
			refObj = mapr.fromDb(refDbObject, refObj, cache);
			cache.putEntity(key, refObj);
			return refObj;
		}
		
		boolean ignoreMissing = mf.getAnnotation(Reference.class) != null && mf.getAnnotation(Reference.class).ignoreMissing();
		if (!ignoreMissing) {
			throw new MappingException("The reference(" + dbRef.toString() + ") could not be fetched for "
					+ mf.getFullName());
		} else {
			return null;
		}
	}
	
	private void readMap(final DBObject dbObject, final MappedField mf, final Object entity, final Reference refAnn,
			final EntityCache cache, final Mapper mapr) {
		Class referenceObjClass = mf.getSubClass();
		Map m = mapr.getOptions().objectFactory.createMap(mf);
		
		DBObject dbVal = (DBObject) mf.getDbObjectValue(dbObject);
		if (dbVal != null) {
			if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
				// replace map by proxy to it.
				m = mapr.proxyFactory.createMapProxy(m, referenceObjClass, refAnn.ignoreMissing(),
						mapr.datastoreProvider);
			}
			
			final Map map = m;
			new IterHelper<String, Object>().loopMap((Object)dbVal, new MapIterCallback<String, Object>() {
				@Override
				public void eval(String key, Object val) {
					DBRef dbRef = (DBRef) val;
					
					if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
						ProxiedEntityReferenceMap proxiedMap = (ProxiedEntityReferenceMap) map;
						proxiedMap.__put(key, mapr.refToKey(dbRef));
					} else {
						Object resolvedObject = resolveObject(dbRef, mf, cache, mapr);
						map.put(key, resolvedObject);
					}
				}});
		}
		mf.setFieldValue(entity, m);
	}
	
	private Object createOrReuseProxy(final Class referenceObjClass, final DBRef dbRef, EntityCache cache, Mapper mapr) {
		Key key = mapr.refToKey(dbRef);
		Object proxyAlreadyCreated = cache.getProxy(key);
		if (proxyAlreadyCreated != null) {
			return proxyAlreadyCreated;
		}
		Object newProxy = mapr.proxyFactory.createProxy(referenceObjClass, key, mapr.datastoreProvider);
		cache.putProxy(key, newProxy);
		return newProxy;
	}
}
