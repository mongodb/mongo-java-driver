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

import com.google.code.morphia.mapping.cache.EntityCache;
import com.google.code.morphia.utils.IterHelper;
import com.google.code.morphia.utils.IterHelper.MapIterCallback;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "rawtypes"})
class EmbeddedMapper implements CustomMapper {
    public void toDBObject(final Object entity, final MappedField mf, final DBObject dbObject, final Map<Object,
            DBObject> involvedObjects, final Mapper mapr) {
        final String name = mf.getNameToStore();

        final Object fieldValue = mf.getFieldValue(entity);

        if (mf.isMap()) {
            writeMap(mf, dbObject, involvedObjects, name, fieldValue, mapr);
        }
        else if (mf.isMultipleValues()) {
            writeCollection(mf, dbObject, involvedObjects, name, fieldValue, mapr);
        }
        else {
            //run converters
            if (mapr.converters.hasDbObjectConverter(mf) || mapr.converters.hasDbObjectConverter(entity.getClass())) {
                mapr.converters.toDBObject(entity, mf, dbObject, mapr.getOptions());
                return;
            }

            final DBObject dbObj = fieldValue == null ? null : mapr.toDBObject(fieldValue, involvedObjects);
            if (dbObj != null) {
                if (!shouldSaveClassName(fieldValue, dbObj, mf)) {
                    dbObj.removeField(Mapper.CLASS_NAME_FIELDNAME);
                }

                if (dbObj.keySet().size() > 0 || mapr.getOptions().storeEmpties) {
                    dbObject.put(name, dbObj);
                }
            }
        }
    }

    private void writeCollection(final MappedField mf, final DBObject dbObject, final Map<Object,
            DBObject> involvedObjects, final String name, final Object fieldValue, final Mapper mapr) {
        Iterable coll = null;

        if (fieldValue != null) {
            if (mf.isArray) {
                coll = Arrays.asList((Object[]) fieldValue);
            }
            else {
                coll = (Iterable) fieldValue;
            }
        }

        if (coll != null) {
            final List values = new ArrayList();
            for (final Object o : coll) {
                if (null == o) {
                    values.add(null);
                }
                else if (mapr.converters.hasSimpleValueConverter(mf) || mapr.converters.hasSimpleValueConverter(o


                                                                                                                        .getClass())) {
                    values.add(mapr.converters.encode(o));
                }
                else {
                    final Object val;
                    if (Collection.class.isAssignableFrom(o.getClass()) || Map.class.isAssignableFrom(o.getClass())) {
                        val = mapr.toMongoObject(o, true);
                    }
                    else {
                        val = mapr.toDBObject(o, involvedObjects);
                    }

                    if (!shouldSaveClassName(o, val, mf)) {
                        ((DBObject) val).removeField(Mapper.CLASS_NAME_FIELDNAME);
                    }

                    values.add(val);
                }
            }
            if (values.size() > 0 || mapr.getOptions().storeEmpties) {
                dbObject.put(name, values);
            }
        }
    }

    private void writeMap(final MappedField mf, final DBObject dbObject, final Map<Object, DBObject> involvedObjects, final String name, final Object fieldValue, final Mapper mapr) {
        final Map<String, Object> map = (Map<String, Object>) fieldValue;
        if (map != null) {
            final BasicDBObject values = new BasicDBObject();

            for (final Map.Entry<String, Object> entry : map.entrySet()) {
                final Object entryVal = entry.getValue();
                final Object val;

                if (entryVal == null) {
                    val = null;
                }
                else if (mapr.converters.hasSimpleValueConverter(mf) || mapr.converters.hasSimpleValueConverter(entryVal.getClass())) {
                    val = mapr.converters.encode(entryVal);
                }
                else {
                    if (Map.class.isAssignableFrom(entryVal.getClass()) || Collection.class.isAssignableFrom(entryVal.getClass())) {
                        val = mapr.toMongoObject(entryVal, true);
                    }
                    else {
                        val = mapr.toDBObject(entryVal, involvedObjects);
                    }

                    if (!shouldSaveClassName(entryVal, val, mf)) {
                        ((DBObject) val).removeField(Mapper.CLASS_NAME_FIELDNAME);
                    }
                }

                final String strKey = mapr.converters.encode(entry.getKey()).toString();
                values.put(strKey, val);
            }

            if (values.size() > 0 || mapr.getOptions().storeEmpties) {
                dbObject.put(name, values);
            }
        }
    }

    public void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity, final EntityCache cache, final Mapper mapr) {
        try {
            if (mf.isMap()) {
                readMap(dbObject, mf, entity, cache, mapr);
            }
            else if (mf.isMultipleValues()) {
                readCollection(dbObject, mf, entity, cache, mapr);
            }
            else {
                // single element

                final Object dbVal = mf.getDbObjectValue(dbObject);
                if (dbVal != null) {
                    final boolean isDBObject = dbVal instanceof DBObject && !(dbVal instanceof BasicDBList);

                    //run converters
                    if (isDBObject && (mapr.converters.hasDbObjectConverter(mf) || mapr.converters.hasDbObjectConverter(mf.getType()))) {
                        mapr.converters.fromDBObject(((DBObject) dbVal), mf, entity);
                        return;
                    }
                    else {
                        Object refObj = null;
                        if (mapr.converters.hasSimpleValueConverter(mf) || mapr.converters.hasSimpleValueConverter(mf.getType())) {
                            refObj = mapr.converters.decode(mf.getType(), dbVal, mf);
                        }
                        else {
                            refObj = mapr.getOptions().objectFactory.createInstance(mapr, mf, ((DBObject) dbVal));
                            refObj = mapr.fromDb(((DBObject) dbVal), refObj, cache);
                        }
                        if (refObj != null) {
                            mf.setFieldValue(entity, refObj);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void readCollection(final DBObject dbObject, final MappedField mf, final Object entity, final EntityCache cache, final Mapper mapr) {
        // multiple documents in a List
        final Collection values = mf.isSet() ? mapr.getOptions().objectFactory.createSet(mf) : mapr.getOptions().objectFactory.createList(mf);

        final Object dbVal = mf.getDbObjectValue(dbObject);
        if (dbVal != null) {

            List dbVals = null;
            if (dbVal instanceof List) {
                dbVals = (List) dbVal;
            }
            else {
                dbVals = new BasicDBList();
                dbVals.add(dbVal);
            }

            for (final Object o : dbVals) {

                final DBObject dbObj = (DBObject) o;
                Object newEntity = null;

                if (dbObj != null) {
                    //run converters
                    if (mapr.converters.hasSimpleValueConverter(mf) || mapr.converters.hasSimpleValueConverter(mf.getSubClass())) {
                        newEntity = mapr.converters.decode(mf.getSubClass(), dbObj, mf);
                    }
                    else {
                        newEntity = readMapOrCollectionOrEntity(dbObj, mf, cache, mapr);
                    }
                }

                values.add(newEntity);
            }
        }
        if (values.size() > 0) {
            if (mf.getType().isArray()) {
                mf.setFieldValue(entity, ReflectionUtils.convertToArray(mf.getSubClass(), ReflectionUtils.iterToList(values)));
            }
            else {
                mf.setFieldValue(entity, values);
            }
        }
    }

    private void readMap(final DBObject dbObject, final MappedField mf, final Object entity, final EntityCache cache, final Mapper mapr) {
        final Map map = mapr.getOptions().objectFactory.createMap(mf);

        final DBObject dbObj = (DBObject) mf.getDbObjectValue(dbObject);
        new IterHelper<Object, Object>().loopMap((Object) dbObj, new MapIterCallback<Object, Object>() {
            @Override
            public void eval(final Object key, final Object val) {
                Object newEntity = null;

                //run converters
                if (val != null) {
                    if (mapr.converters.hasSimpleValueConverter(mf) ||
                            mapr.converters.hasSimpleValueConverter(mf.getSubClass())) {
                        newEntity = mapr.converters.decode(mf.getSubClass(), val, mf);
                    }
                    else {
                        if (val instanceof DBObject) {
                            newEntity = readMapOrCollectionOrEntity((DBObject) val, mf, cache, mapr);
                        }
                        else {
                            throw new MappingException("Embedded element isn't a DBObject! How can it be that is a "
                                                               + val.getClass());
                        }

                    }
                }

                final Object objKey = mapr.converters.decode(mf.getMapKeyClass(), key);
                map.put(objKey, newEntity);
            }
        });

        if (map.size() > 0) {
            mf.setFieldValue(entity, map);
        }
    }

    private Object readMapOrCollectionOrEntity(final DBObject dbObj, final MappedField mf, final EntityCache cache, final Mapper mapr) {
        if (Map.class.isAssignableFrom(mf.getSubClass()) || Iterable.class.isAssignableFrom(mf.getSubClass())) {
            final MapOrCollectionMF mocMF = new MapOrCollectionMF((ParameterizedType) mf.getSubType());
            mapr.fromDb(dbObj, mocMF, cache);
            return mocMF.getValue();
        }
        else {
            final Object newEntity = mapr.getOptions().objectFactory.createInstance(mapr, mf, dbObj);
            return mapr.fromDb(dbObj, newEntity, cache);
        }
    }

    public static boolean shouldSaveClassName(final Object rawVal, final Object convertedVal, final MappedField mf) {
        if (rawVal == null || mf == null) {
            return true;
        }
        if (mf.isSingleValue()) {
            return !(mf.getType().equals(rawVal.getClass()) && !(convertedVal instanceof BasicDBList));
        }
        else if (convertedVal != null &&
                convertedVal instanceof DBObject &&
                !mf.getSubClass().isInterface() &&
                !Modifier.isAbstract(mf.getSubClass().getModifiers()) &&
                mf.getSubClass().equals(rawVal.getClass())) {
            return false;
        }
        else {
            return true;
        }
    }

}
