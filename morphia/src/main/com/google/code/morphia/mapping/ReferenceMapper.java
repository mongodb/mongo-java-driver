/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package com.google.code.morphia.mapping;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "rawtypes"})
class ReferenceMapper implements CustomMapper {
    public static final Logr log = MorphiaLoggerFactory.get(ReferenceMapper.class);

    public void toDBObject(final Object entity, final MappedField mf, final DBObject dbObject, final Map<Object,
            DBObject> involvedObjects, final Mapper mapr) {
        final String name = mf.getNameToStore();

        final Object fieldValue = mf.getFieldValue(entity);

        if (fieldValue == null && !mapr.getOptions().storeNulls) {
            return;
        }

        if (mf.isMap()) {
            writeMap(mf, dbObject, name, fieldValue, mapr);
        }
        else if (mf.isMultipleValues()) {
            writeCollection(mf, dbObject, name, fieldValue, mapr);
        }
        else {
            writeSingle(dbObject, name, fieldValue, mapr);
        }

    }

    private void writeSingle(final DBObject dbObject, final String name, final Object fieldValue, final Mapper mapr) {
        if (fieldValue == null) {
            if (mapr.getOptions().storeNulls) {
                dbObject.put(name, null);
            }
        }

        final DBRef dbrefFromKey = mapr.keyToRef(getKey(fieldValue, mapr));
        dbObject.put(name, dbrefFromKey);
    }

    private void writeCollection(final MappedField mf, final DBObject dbObject, final String name,
                                 final Object fieldValue, final Mapper mapr) {
        if (fieldValue != null) {
            final List values = new ArrayList();

            if (ProxyHelper.isProxy(fieldValue) && ProxyHelper.isUnFetched(fieldValue)) {
                final ProxiedEntityReferenceList p = (ProxiedEntityReferenceList) fieldValue;
                final List<Key<?>> getKeysAsList = p.__getKeysAsList();
                for (final Key<?> key : getKeysAsList) {
                    addValue(values, key, mapr);
                }
            }
            else {

                if (mf.getType().isArray()) {
                    for (final Object o : (Object[]) fieldValue) {
                        addValue(values, o, mapr);
                    }
                }
                else {
                    for (final Object o : (Iterable) fieldValue) {
                        addValue(values, o, mapr);
                    }
                }
            }
            if (values.size() > 0 || mapr.getOptions().storeEmpties) {
                dbObject.put(name, values);
            }
        }
    }

    private void addValue(final List vals, final Object o, final Mapper mapr) {
        if (o == null && mapr.getOptions().storeNulls) {
            vals.add(null);
            return;
        }

        if (o instanceof Key) {
            vals.add(mapr.keyToRef((Key) o));
        }
        else {
            vals.add(mapr.keyToRef(getKey(o, mapr)));
        }
    }

    private void writeMap(final MappedField mf, final DBObject dbObject, final String name, final Object fieldValue,
                          final Mapper mapr) {
        final Map<Object, Object> map = (Map<Object, Object>) fieldValue;
        if ((map != null)) {
            final Map values = mapr.getOptions().objectFactory.createMap(mf);

            if (ProxyHelper.isProxy(map) && ProxyHelper.isUnFetched(map)) {
                final ProxiedEntityReferenceMap proxy = (ProxiedEntityReferenceMap) map;

                final Map<String, Key<?>> refMap = proxy.__getReferenceMap();
                for (final Map.Entry<String, Key<?>> entry : refMap.entrySet()) {
                    final String strKey = entry.getKey();
                    values.put(strKey, mapr.keyToRef(entry.getValue()));
                }
            }
            else {
                for (final Map.Entry<Object, Object> entry : map.entrySet()) {
                    final String strKey = mapr.converters.encode(entry.getKey()).toString();
                    values.put(strKey, mapr.keyToRef(getKey(entry.getValue(), mapr)));
                }
            }
            if (values.size() > 0 || mapr.getOptions().storeEmpties) {
                dbObject.put(name, values);
            }
        }
    }

    private Key<?> getKey(final Object entity, final Mapper mapr) {
        try {
            if (entity instanceof ProxiedEntityReference) {
                final ProxiedEntityReference proxy = (ProxiedEntityReference) entity;
                return proxy.__getKey();
            }
            final MappedClass mappedClass = mapr.getMappedClass(entity);
            final Object id = mappedClass.getIdField().get(entity);
            if (id == null) {
                throw new MappingException("@Id field cannot be null!");
            }
            final Key key = new Key(mappedClass.getCollectionName(), id);
            return key;
        } catch (IllegalAccessException iae) {
            throw new RuntimeException(iae);
        }
    }

    /**
     * @deprecated use void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity, EntityCache
     *             cache) instead.
     */
    @Deprecated
    void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity, final Mapper mapr) {
        fromDBObject(dbObject, mf, entity, mapr.createEntityCache(), mapr);
    }

    public void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity,
                             final EntityCache cache, final Mapper mapr) {
        final Class fieldType = mf.getType();

        final Reference refAnn = mf.getAnnotation(Reference.class);
        if (mf.isMap()) {
            readMap(dbObject, mf, entity, refAnn, cache, mapr);
        }
        else if (mf.isMultipleValues()) {
            readCollection(dbObject, mf, entity, refAnn, cache, mapr);
        }
        else {
            readSingle(dbObject, mf, entity, fieldType, refAnn, cache, mapr);
        }

    }

    private void readSingle(final DBObject dbObject, final MappedField mf, final Object entity, final Class fieldType,
                            final Reference refAnn, final EntityCache cache, final Mapper mapr) {
        final Class referenceObjClass = fieldType;

        final DBRef dbRef = (DBRef) mf.getDbObjectValue(dbObject);
        if (dbRef != null) {
            Object resolvedObject = null;
            if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
                if (exists(referenceObjClass, dbRef, cache, mapr)) {
                    resolvedObject = createOrReuseProxy(referenceObjClass, dbRef, cache, mapr);
                }
                else {
                    if (!refAnn.ignoreMissing()) {
                        throw new MappingException("The reference(" + dbRef.toString() + ") could not be fetched for "
                                                           + mf.getFullName());
                    }
                }
            }
            else {
                resolvedObject = resolveObject(dbRef, mf, cache, mapr);
            }

            if (resolvedObject != null) {
                mf.setFieldValue(entity, resolvedObject);
            }

        }
    }

    private void readCollection(final DBObject dbObject, final MappedField mf, final Object entity,
                                final Reference refAnn,
                                final EntityCache cache, final Mapper mapr) {
        // multiple references in a List
        final Class referenceObjClass = mf.getSubClass();
        Collection references = mf.isSet() ? mapr.getOptions().objectFactory.createSet(mf) : mapr.getOptions()
                .objectFactory.createList(mf);

        if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
            final Object dbVal = mf.getDbObjectValue(dbObject);
            if (dbVal != null) {
                references = mapr.proxyFactory.createListProxy(references, referenceObjClass, refAnn.ignoreMissing(),
                                                               mapr.datastoreProvider);
                final ProxiedEntityReferenceList referencesAsProxy = (ProxiedEntityReferenceList) references;

                if (dbVal instanceof List) {
                    final List<DBRef> refList = (List) dbVal;
                    final DatastoreImpl dsi = (DatastoreImpl) mapr.datastoreProvider.get();
                    final List<Key<Object>> keys = dsi.getKeysByRefs(refList);

                    if (keys.size() != refList.size()) {
                        final String msg = "Some of the references could not be fetched for " + mf.getFullName() + ". "
                                + refList + " != " + keys;
                        if (!refAnn.ignoreMissing()) {
                            throw new MappingException(msg);
                        }
                        else {
                            log.warning(msg);
                        }
                    }

                    referencesAsProxy.__addAll(keys);
                }
                else {
                    final DBRef dbRef = (DBRef) dbVal;
                    if (!exists(mf.getSubClass(), dbRef, cache, mapr)) {
                        final String msg = "The reference(" + dbRef.toString() + ") could not be fetched for "
                                + mf.getFullName();
                        if (!refAnn.ignoreMissing()) {
                            throw new MappingException(msg);
                        }
                        else {
                            log.warning(msg);
                        }
                    }
                    else {
                        referencesAsProxy.__add(mapr.refToKey(dbRef));
                    }
                }
            }
        }
        else {
            final Object dbVal = mf.getDbObjectValue(dbObject);
            final Collection refs = references;
            new IterHelper<String, Object>().loopOrSingle(dbVal, new IterCallback<Object>() {
                @Override
                public void eval(final Object val) {
                    final DBRef dbRef = (DBRef) val;
                    final Object ent = resolveObject(dbRef, mf, cache, mapr);
                    if (ent == null) {
                        log.warning("Null reference found when retrieving value for " + mf.getFullName());
                    }
                    else {
                        refs.add(ent);
                    }
                }
            });
        }

        if (mf.getType().isArray()) {
            mf.setFieldValue(entity, ReflectionUtils.convertToArray(mf.getSubClass(),
                                                                    ReflectionUtils.iterToList(references)));
        }
        else {
            mf.setFieldValue(entity, references);
        }
    }

    boolean exists(final Class c, final DBRef dbRef, final EntityCache cache, final Mapper mapr) {
        final Key key = mapr.refToKey(dbRef);
        final Boolean cached = cache.exists(key);
        if (cached != null) {
            return cached;
        }

        final DatastoreImpl dsi = (DatastoreImpl) mapr.datastoreProvider.get();

        final DBCollection dbColl = dsi.getCollection(c);
        if (!dbColl.getName().equals(dbRef.getRef())) {
            log.warning("Class " + c.getName() + " is stored in the '" + dbColl.getName()
                                + "' collection but a reference was found for this type to another collection, " +
                                "'" + dbRef.getRef()
                                + "'. The reference will be loaded using the class anyway. " + dbRef);
        }
        final boolean exists = (dsi.find(dbRef.getRef(), c).disableValidation().filter("_id", dbRef.getId()).asKeyList()
                .size() == 1);
        cache.notifyExists(key, exists);
        return exists;
    }

    Object resolveObject(final DBRef dbRef, final MappedField mf, final EntityCache cache, final Mapper mapr) {
        if (dbRef == null) {
            return null;
        }

        final Key key = mapr.createKey(mf.isSingleValue() ? mf.getType() : mf.getSubClass(), dbRef.getId());

        final Object cached = cache.getEntity(key);
        if (cached != null) {
            return cached;
        }

        //TODO: if _db is null, set it?
        final DBObject refDbObject = dbRef.fetch();

        if (refDbObject != null) {
            Object refObj = mapr.getOptions().objectFactory.createInstance(mapr, mf, refDbObject);
            refObj = mapr.fromDb(refDbObject, refObj, cache);
            cache.putEntity(key, refObj);
            return refObj;
        }

        final boolean ignoreMissing = mf.getAnnotation(Reference.class) != null && mf.getAnnotation(Reference.class).ignoreMissing();
        if (!ignoreMissing) {
            throw new MappingException("The reference(" + dbRef.toString() + ") could not be fetched for "
                                               + mf.getFullName());
        }
        else {
            return null;
        }
    }

    private void readMap(final DBObject dbObject, final MappedField mf, final Object entity, final Reference refAnn,
                         final EntityCache cache, final Mapper mapr) {
        final Class referenceObjClass = mf.getSubClass();
        Map m = mapr.getOptions().objectFactory.createMap(mf);

        final DBObject dbVal = (DBObject) mf.getDbObjectValue(dbObject);
        if (dbVal != null) {
            if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
                // replace map by proxy to it.
                m = mapr.proxyFactory.createMapProxy(m, referenceObjClass, refAnn.ignoreMissing(),
                                                     mapr.datastoreProvider);
            }

            final Map map = m;
            new IterHelper<String, Object>().loopMap((Object) dbVal, new MapIterCallback<String, Object>() {
                @Override
                public void eval(final String key, final Object val) {
                    final DBRef dbRef = (DBRef) val;

                    if (refAnn.lazy() && LazyFeatureDependencies.assertDependencyFullFilled()) {
                        final ProxiedEntityReferenceMap proxiedMap = (ProxiedEntityReferenceMap) map;
                        proxiedMap.__put(key, mapr.refToKey(dbRef));
                    }
                    else {
                        final Object resolvedObject = resolveObject(dbRef, mf, cache, mapr);
                        map.put(key, resolvedObject);
                    }
                }
            });
        }
        mf.setFieldValue(entity, m);
    }

    private Object createOrReuseProxy(final Class referenceObjClass, final DBRef dbRef, final EntityCache cache, final Mapper mapr) {
        final Key key = mapr.refToKey(dbRef);
        final Object proxyAlreadyCreated = cache.getProxy(key);
        if (proxyAlreadyCreated != null) {
            return proxyAlreadyCreated;
        }
        final Object newProxy = mapr.proxyFactory.createProxy(referenceObjClass, key, mapr.datastoreProvider);
        cache.putProxy(key, newProxy);
        return newProxy;
    }
}
