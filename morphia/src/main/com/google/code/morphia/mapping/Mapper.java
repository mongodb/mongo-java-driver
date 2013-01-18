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

package com.google.code.morphia.mapping;

import com.google.code.morphia.EntityInterceptor;
import com.google.code.morphia.Key;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.NotSaved;
import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.converters.DefaultConverters;
import com.google.code.morphia.converters.TypeConverter;
import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.cache.DefaultEntityCache;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;
import com.google.code.morphia.mapping.lazy.DefaultDatastoreProvider;
import com.google.code.morphia.mapping.lazy.LazyFeatureDependencies;
import com.google.code.morphia.mapping.lazy.LazyProxyFactory;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedEntityReference;
import com.google.code.morphia.mapping.lazy.proxy.ProxyHelper;
import com.google.code.morphia.query.FilterOperator;
import com.google.code.morphia.query.ValidationException;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;

/**
 * <p>This is the heart of Morphia and takes care of mapping from/to POJOs/DBObjects<p> <p>This class is thread-safe and
 * keeps various "cached" data which should speed up processing.</p>
 *
 * @author Olafur Gauti Gudmundsson
 * @author Scott Hernandez
 */
@SuppressWarnings({"unchecked", "rawtypes", "CanBeFinal"})
public class Mapper {
    private static final Logr log = MorphiaLoggerFactory.get(Mapper.class);

    /**
     * The @{@link Id} field name that is stored with mongodb.
     */
    public static final String ID_KEY = "_id";
    /**
     * Special name that can never be used. Used as default for some fields to indicate default state.
     */
    public static final String IGNORED_FIELDNAME = ".";
    /**
     * Special field used by morphia to support various possibly loading issues; will be replaced when discriminators
     * are
     * implemented to support polymorphism
     */
    public static final String CLASS_NAME_FIELDNAME = "className";

    /**
     * Set of classes that registered by this mapper
     */
    private final Map<String, MappedClass> mappedClasses = new ConcurrentHashMap<String, MappedClass>();
    private final ConcurrentHashMap<String, Set<MappedClass>> mappedClassesByCollection = new
            ConcurrentHashMap<String, Set<MappedClass>>();

    //EntityInterceptors; these are called before EntityListeners and lifecycle methods on an Entity, for all Entities
    private final List<EntityInterceptor> interceptors = new LinkedList<EntityInterceptor>();

    //A general cache of instances of classes; used by MappedClass for EntityListerner(s)
    final Map<Class, Object> instanceCache = new ConcurrentHashMap();

    private MapperOptions opts = new MapperOptions();

    // TODO: make these configurable
    LazyProxyFactory proxyFactory = LazyFeatureDependencies.createDefaultProxyFactory();
    DatastoreProvider datastoreProvider = new DefaultDatastoreProvider();
    DefaultConverters converters = new DefaultConverters();

    public Mapper() {
        converters.setMapper(this);
    }

    public Mapper(final MapperOptions opts) {
        this();
        this.opts = opts;
    }

    /**
     * <p> Adds an {@link EntityInterceptor} </p>
     */
    public void addInterceptor(final EntityInterceptor ei) {
        interceptors.add(ei);
    }

    /**
     * <p> Gets list of {@link EntityInterceptor}s </p>
     */
    public Collection<EntityInterceptor> getInterceptors() {
        return interceptors;
    }

    public MapperOptions getOptions() {
        return this.opts;
    }

    public void setOptions(final MapperOptions options) {
        this.opts = options;
    }

    public boolean isMapped(final Class c) {
        return mappedClasses.containsKey(c.getName());
    }

    /**
     * Creates a MappedClass and validates it.
     */
    public MappedClass addMappedClass(final Class c) {
        final MappedClass mc = new MappedClass(c, this);
        return addMappedClass(mc, true);
    }

    /**
     * Validates MappedClass and adds to internal cache.
     */
    public MappedClass addMappedClass(final MappedClass mc) {
        return addMappedClass(mc, true);
    }


    /**
     * Add MappedClass to internal cache, possibly validating first.
     */
    private MappedClass addMappedClass(final MappedClass mc, final boolean validate) {
        if (validate) {
            mc.validate();
        }

        final Converters c = (Converters) mc.getAnnotation(Converters.class);
        if (c != null) {
            for (final Class<? extends TypeConverter> clazz : c.value()) {
                if (!converters.isRegistered(clazz)) {
                    converters.addConverter(clazz);
                }
            }
        }

        mappedClasses.put(mc.getClazz().getName(), mc);

        Set<MappedClass> mcs = mappedClassesByCollection.get(mc.getCollectionName());
        if (mcs == null) {
            mcs = new CopyOnWriteArraySet<MappedClass>();
            final Set<MappedClass> temp = mappedClassesByCollection.putIfAbsent(mc.getCollectionName(), mcs);
            if (temp != null) {
                mcs = temp;
            }
        }

        mcs.add(mc);

        return mc;
    }

    /**
     * Returns collection of MappedClasses
     */
    public Collection<MappedClass> getMappedClasses() {
        return new ArrayList<MappedClass>(mappedClasses.values());
    }

    /**
     * Returns map of MappedClasses by class name
     */
    public Map<String, MappedClass> getMCMap() {
        return Collections.unmodifiableMap(mappedClasses);
    }

    /**
     * <p> Gets the {@link MappedClass} for the object (type). If it isn't mapped,
     * create a new class and cache it (without
     * validating). </p>
     */
    public MappedClass getMappedClass(final Object obj) {
        if (obj == null) {
            return null;
        }

        Class type = (obj instanceof Class) ? (Class) obj : obj.getClass();
        if (ProxyHelper.isProxy(obj)) {
            type = ProxyHelper.getReferentClass(obj);
        }

        MappedClass mc = mappedClasses.get(type.getName());
        if (mc == null) {
            mc = new MappedClass(type, this);
            // no validation
            addMappedClass(mc, false);
        }
        return mc;
    }

    public String getCollectionName(final Object object) {
        if (object == null) {
            throw new IllegalArgumentException();
        }

        final MappedClass mc = getMappedClass(object);
        return mc.getCollectionName();
    }

    /**
     * <p> Updates the @{@link Id} fields. </p>
     *
     * @param entity The object to update
     * @param dbObj  Value to update with; null means skip
     */
    public void updateKeyInfo(final Object entity, final DBObject dbObj, final EntityCache cache) {
        final MappedClass mc = getMappedClass(entity);

        // update id field, if there.
        if ((mc.getIdField() != null) && (dbObj != null) && (dbObj.get(ID_KEY) != null)) {
            try {
                final MappedField mf = mc.getMappedIdField();
                final Object oldIdValue = mc.getIdField().get(entity);
                readMappedField(dbObj, mf, entity, cache);
                final Object dbIdValue = mc.getIdField().get(entity);
                if (oldIdValue != null) {
                    // The entity already had an id set. Check to make sure it
                    // hasn't changed. That would be unexpected, and could
                    // indicate a bad state.
                    if (!dbIdValue.equals(oldIdValue)) {
                        mf.setFieldValue(entity, oldIdValue);//put the value back...

                        throw new RuntimeException("@Id mismatch: " + oldIdValue + " != " + dbIdValue + " for "
                                                           + entity.getClass().getName());
                    }
                }
                else {
                    mc.getIdField().set(entity, dbIdValue);
                }
            } catch (Exception e) {
                if (e.getClass().equals(RuntimeException.class)) {
                    throw (RuntimeException) e;
                }

                throw new RuntimeException("Error setting @Id field after save/insert.", e);
            }
        }
    }

    /**
     * Converts a DBObject back to a type-safe java object (POJO)
     *
     * @param entityClass The type to return, or use; can be overridden by the @see Mapper.CLASS_NAME_FIELDNAME in the
     *                    DBObject
     */
    public Object fromDBObject(final Class entityClass, final DBObject dbObject, final EntityCache cache) {
        if (dbObject == null) {
            final Throwable t = new Throwable();
            log.error("Somebody passed in a null dbObject; bad client!", t);
            return null;
        }

        Object entity = null;
        entity = opts.objectFactory.createInstance(entityClass, dbObject);
        entity = fromDb(dbObject, entity, cache);
        return entity;
    }

    /**
     * <p> Converts a java object to a mongo-compatible object (possibly a DBObject for complex mappings). Very
     * similar to
     * {@link Mapper.toDBObject} </p> <p> Used (mainly) by query/update operations </p>
     */
    Object toMongoObject(final Object javaObj, final boolean includeClassName) {
        if (javaObj == null) {
            return null;
        }
        Class origClass = javaObj.getClass();

        if (origClass.isAnonymousClass() && origClass.getSuperclass().isEnum()) {
            origClass = origClass.getSuperclass();
        }

        final Object newObj = converters.encode(origClass, javaObj);
        if (newObj == null) {
            log.warning("converted " + javaObj + " to null");
            return newObj;
        }
        final Class type = newObj.getClass();
        final boolean bSameType = origClass.equals(type);

        //TODO: think about this logic a bit more.
        //Even if the converter changed it, should it still be processed?
        if (!bSameType && !(Map.class.isAssignableFrom(type) || Iterable.class.isAssignableFrom(type))) {
            return newObj;
        }
        else { //The converter ran, and produced another type, or it is a list/map

            boolean isSingleValue = true;
            boolean isMap = false;
            Class subType = null;

            if (type.isArray() || Map.class.isAssignableFrom(type) || Iterable.class.isAssignableFrom(type)) {
                isSingleValue = false;
                isMap = ReflectionUtils.implementsInterface(type, Map.class);
                // subtype of Long[], List<Long> is Long
                subType = (type.isArray()) ? type.getComponentType() : ReflectionUtils.getParameterizedClass(type,
                                                                                                             (isMap)


                                                                                                                     ? 1 : 0);
            }

            if (isSingleValue && !ReflectionUtils.isPropertyType(type)) {
                final DBObject dbObj = toDBObject(newObj);
                if (!includeClassName) {
                    dbObj.removeField(CLASS_NAME_FIELDNAME);
                }
                return dbObj;
            }
            else if (newObj instanceof DBObject) {
                return newObj;
            }
            else if (isMap) {
                if (ReflectionUtils.isPropertyType(subType)) {
                    return toDBObject(newObj);
                }
                else {
                    final HashMap m = new HashMap();
                    for (final Map.Entry e : (Iterable<Map.Entry>) ((Map) newObj).entrySet()) {
                        m.put(e.getKey(), toMongoObject(e.getValue(), includeClassName));
                    }

                    return m;
                }
                //Set/List but needs elements converted
            }
            else if (!isSingleValue && !ReflectionUtils.isPropertyType(subType)) {
                final ArrayList<Object> vals = new ArrayList<Object>();
                if (type.isArray()) {
                    for (final Object obj : (Object[]) newObj) {
                        vals.add(toMongoObject(obj, includeClassName));
                    }
                }
                else {
                    for (final Object obj : (Iterable) newObj) {
                        vals.add(toMongoObject(obj, includeClassName));
                    }
                }

                return vals;
            }
            else {
                return newObj;
            }
        }
    }


    /**
     * <p> Converts a java object to a mongo-compatible object (possibly a DBObject for complex mappings). Very similar to
     * {@link Mapper.toDBObject} </p> <p> Used (mainly) by query/update operations </p>
     */
    public Object toMongoObject(final MappedField mf, final MappedClass mc, final Object value) {
        Object mappedValue = value;

        //convert the value to Key (DBRef) if the field is @Reference or type is Key/DBRef, or if the destination class is an @Entity
        if ((mf != null && (mf.hasAnnotation(Reference.class) ||
                mf.getType().isAssignableFrom(Key.class) ||
                mf.getType().isAssignableFrom(DBRef.class) ||
                //Collection/Array/???
                (value instanceof Iterable && mf.isMultipleValues() && (
                        mf.getSubClass().isAssignableFrom(Key.class) ||
                                mf.getSubClass().isAssignableFrom(DBRef.class))
                )
        )) || (mc != null && mc.getEntityAnnotation() != null)) {
            try {
                if (value instanceof Iterable) {
                    final ArrayList<DBRef> refs = new ArrayList<DBRef>();
                    final Iterable it = (Iterable) value;
                    for (final Object o : it) {
                        final Key<?> k = (o instanceof Key) ? (Key<?>) o : getKey(o);
                        final DBRef dbref = keyToRef(k);
                        refs.add(dbref);
                    }
                    mappedValue = refs;
                }
                else {
                    if (value == null) {
                        mappedValue = null;
                    }

                    final Key<?> k = (value instanceof Key) ? (Key<?>) value : getKey(value);
                    mappedValue = keyToRef(k);
                    if (mappedValue == value) {
                        throw new ValidationException("cannnot map to @Reference/Key<T>/DBRef field: " + value);
                    }
                }
            } catch (Exception e) {
                log.error("Error converting value(" + value + ") to reference.", e);
                mappedValue = toMongoObject(value, false);
            }
        }//serialized
        else if (mf != null && mf.hasAnnotation(Serialized.class)) {
            try {
                mappedValue = Serializer.serialize(value, !mf.getAnnotation(Serialized.class).disableCompression());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        //pass-through
        else if (value instanceof DBObject) {
            mappedValue = value;
        }
        else {
            mappedValue = toMongoObject(value, EmbeddedMapper.shouldSaveClassName(value, mappedValue, mf));
            if (mappedValue instanceof DBObject && !EmbeddedMapper.shouldSaveClassName(value, mappedValue, mf)) {
                ((DBObject) mappedValue).removeField(CLASS_NAME_FIELDNAME);
            }
        }

        return mappedValue;
    }

    public Object getId(Object entity) {
        if (entity == null) {
            return null;
        }
        entity = ProxyHelper.unwrap(entity);
//		String keyClassName = entity.getClass().getName();
        final MappedClass mc = getMappedClass(entity.getClass());
//		
//		if (getMappedClasses().containsKey(keyClassName))
//			mc = getMappedClasses().get(keyClassName);
//		else
//			mc = new MappedClass(entity.getClass(), getMapper());
        try {
            return mc.getIdField().get(entity);
        } catch (Exception e) {
            return null;
        }
    }

    public <T> Key<T> getKey(T entity) {
        if (entity instanceof ProxiedEntityReference) {
            final ProxiedEntityReference proxy = (ProxiedEntityReference) entity;
            return (Key<T>) proxy.__getKey();
        }

        entity = ProxyHelper.unwrap(entity);
        if (entity instanceof Key) {
            return (Key<T>) entity;
        }

        final Object id = getId(entity);
        if (id == null) {
            throw new MappingException("Could not get id for " + entity.getClass().getName());
        }
        return new Key<T>((Class<T>) entity.getClass(), id);
    }

    /**
     * <p> Converts an entity (POJO) to a DBObject; A special field will be added to keep track of the class: {@link
     * Mapper.CLASS_NAME_FIELDNAME} </p>
     *
     * @param entity The POJO
     */
    public DBObject toDBObject(final Object entity) {
        return toDBObject(entity, null);
    }

    /**
     * <p> Converts an entity (POJO) to a DBObject (for use with low-level driver); A special field will be added to
     * keep
     * track of the class: {@link Mapper.CLASS_NAME_FIELDNAME} </p>
     *
     * @param entity          The POJO
     * @param involvedObjects A Map of (already converted) POJOs
     */
    public DBObject toDBObject(final Object entity, final Map<Object, DBObject> involvedObjects) {
        return toDBObject(entity, involvedObjects, true);
    }

    DBObject toDBObject(final Object entity, final Map<Object, DBObject> involvedObjects, final boolean lifecycle) {

        DBObject dbObject = new BasicDBObject();
        final MappedClass mc = getMappedClass(entity);

        if (mc.getEntityAnnotation() == null || !mc.getEntityAnnotation().noClassnameStored()) {
            dbObject.put(CLASS_NAME_FIELDNAME, entity.getClass().getName());
        }

        if (lifecycle) {
            dbObject = mc.callLifecycleMethods(PrePersist.class, entity, dbObject, this);
        }

        for (final MappedField mf : mc.getPersistenceFields()) {
            try {
                writeMappedField(dbObject, mf, entity, involvedObjects);
            } catch (Exception e) {
                throw new MappingException("Error mapping field:" + mf.getFullName(), e);
            }
        }
        if (involvedObjects != null) {
            involvedObjects.put(entity, dbObject);
        }

        if (lifecycle) {
            mc.callLifecycleMethods(PreSave.class, entity, dbObject, this);
        }

        return dbObject;
    }

    Object fromDb(DBObject dbObject, final Object entity, final EntityCache cache) {
        //hack to bypass things and just read the value.
        if (entity instanceof MappedField) {
            readMappedField(dbObject, (MappedField) entity, entity, cache);
            return entity;
        }

        // check the history key (a key is the namespace + id)

        if (dbObject.containsField(ID_KEY) && getMappedClass(entity).getIdField() != null
                && getMappedClass(entity).getEntityAnnotation() != null) {
            final Key key = new Key(entity.getClass(), dbObject.get(ID_KEY));
            final Object cachedInstance = cache.getEntity(key);
            if (cachedInstance != null) {
                return cachedInstance;
            }
            else {
                cache.putEntity(key, entity); // to avoid stackOverflow in recursive refs
            }
        }

        final MappedClass mc = getMappedClass(entity);

        dbObject = mc.callLifecycleMethods(PreLoad.class, entity, dbObject, this);
        try {
            for (final MappedField mf : mc.getPersistenceFields()) {
                readMappedField(dbObject, mf, entity, cache);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (dbObject.containsField(ID_KEY) && getMappedClass(entity).getIdField() != null) {
            final Key key = new Key(entity.getClass(), dbObject.get(ID_KEY));
            cache.putEntity(key, entity);
        }
        mc.callLifecycleMethods(PostLoad.class, entity, dbObject, this);
        return entity;
    }

    private void readMappedField(final DBObject dbObject, final MappedField mf, final Object entity, final EntityCache cache) {
        if (mf.hasAnnotation(Property.class) || mf.hasAnnotation(Serialized.class)
                || mf.isTypeMongoCompatible() || converters.hasSimpleValueConverter(mf)) {
            opts.valueMapper.fromDBObject(dbObject, mf, entity, cache, this);
        }
        else if (mf.hasAnnotation(Embedded.class)) {
            opts.embeddedMapper.fromDBObject(dbObject, mf, entity, cache, this);
        }
        else if (mf.hasAnnotation(Reference.class)) {
            opts.referenceMapper.fromDBObject(dbObject, mf, entity, cache, this);
        }
        else {
            opts.defaultMapper.fromDBObject(dbObject, mf, entity, cache, this);
        }
    }

    private void writeMappedField(final DBObject dbObject, final MappedField mf, final Object entity, final Map<Object, DBObject> involvedObjects) {
        Class<? extends Annotation> annType = null;

        //skip not saved fields.
        if (mf.hasAnnotation(NotSaved.class)) {
            return;
        }

        // get the annotation from the field.
        for (final Class<? extends Annotation> testType : new Class[]{Property.class,
                Embedded.class,
                Serialized.class,
                Reference.class}) {
            if (mf.hasAnnotation(testType)) {
                annType = testType;
                break;
            }
        }

        if (Property.class.equals(annType) || Serialized.class.equals(annType) || mf.isTypeMongoCompatible() ||
                (converters.hasSimpleValueConverter(mf) || (converters.hasSimpleValueConverter(mf.getFieldValue(entity))))) {
            opts.valueMapper.toDBObject(entity, mf, dbObject, involvedObjects, this);
        }
        else if (Reference.class.equals(annType)) {
            opts.referenceMapper.toDBObject(entity, mf, dbObject, involvedObjects, this);
        }
        else if (Embedded.class.equals(annType)) {
            opts.embeddedMapper.toDBObject(entity, mf, dbObject, involvedObjects, this);
        }
        else {
            log.debug("No annotation was found, using default mapper " + opts.defaultMapper + " for " + mf);
            opts.defaultMapper.toDBObject(entity, mf, dbObject, involvedObjects, this);
        }

    }

    // TODO might be better to expose via some "options" object?
    public DefaultConverters getConverters() {
        return converters;
    }

    public EntityCache createEntityCache() {
        return new DefaultEntityCache();// TODO choose impl
    }

    public <T> Key<T> refToKey(final DBRef ref) {
        if (ref == null) {
            return null;
        }
        final Key<T> key = new Key<T>(ref.getRef(), ref.getId());
        return key;
    }

    public DBRef keyToRef(final Key key) {
        if (key == null) {
            return null;
        }
        if (key.getKindClass() == null && key.getKind() == null) {
            throw new IllegalStateException("How can it be missing both?");
        }
        if (key.getKind() == null) {
            key.setKind(getCollectionName(key.getKindClass()));
        }

        return new DBRef(null, key.getKind(), key.getId());
    }

    public String updateKind(final Key key) {
        if (key.getKind() == null && key.getKindClass() == null) {
            throw new IllegalStateException("Key is invalid! " + toString());
        }
        else if (key.getKind() == null) {
            key.setKind(getMappedClass(key.getKindClass()).getCollectionName());
        }

        return key.getKind();
    }

    <T> Key<T> createKey(final Class<T> clazz, final Serializable id) {
        return new Key<T>(clazz, id);
    }

    <T> Key<T> createKey(final Class<T> clazz, final Object id) {
        if (id instanceof Serializable) {
            return createKey(clazz, (Serializable) id);
        }
        else {
            throw new IllegalArgumentException("" + id.getClass() + " does not implement Serializable");
        }
    }

    /**
     * Validate the path, and value type, returning the mappedfield for the field at the path
     */
    public static MappedField validate(final Class clazz, final Mapper mapr, final StringBuffer origProp, final FilterOperator op, final Object val, final boolean validateNames, final boolean validateTypes) {
        //TODO: cache validations (in static?).

        MappedField mf = null;
        final String prop = origProp.toString();
        boolean hasTranslations = false;

        if (validateNames) {
            final String[] parts = prop.split("\\.");
            if (clazz == null) {
                return null;
            }

            MappedClass mc = mapr.getMappedClass(clazz);
            for (int i = 0; ; ) {
                final String part = parts[i];
                mf = mc.getMappedField(part);

                //translate from java field name to stored field name
                if (mf == null) {
                    mf = mc.getMappedFieldByJavaField(part);
                    if (mf == null) {
                        throw new ValidationException("The field '" + part + "' could not be found in '" + clazz
                                .getName() +
                                                              "' while validating - " + prop +
                                                              "; if you wish to continue please disable validation.");
                    }
                    hasTranslations = true;
                    parts[i] = mf.getNameToStore();
                }

                i++;
                if (mf.isMap()) {
                    //skip the map key validation, and move to the next part
                    i++;
                }

                //catch people trying to search/update into @Reference/@Serialized fields
                if (i < parts.length && !canQueryPast(mf)) {
                    throw new ValidationException("Can not use dot-notation past '" + part + "' could not be found in" +
                                                          " '" + clazz.getName() + "' while validating - " + prop);
                }

                if (i >= parts.length) {
                    break;
                }
                //get the next MappedClass for the next field validation
                mc = mapr.getMappedClass((mf.isSingleValue()) ? mf.getType() : mf.getSubClass());
            }

            //record new property string if there has been a translation to any part
            if (hasTranslations) {
                origProp.setLength(0); // clear existing content
                origProp.append(parts[0]);
                for (int i = 1; i < parts.length; i++) {
                    origProp.append('.');
                    origProp.append(parts[i]);
                }
            }

            if (validateTypes) {
                if ((mf.isSingleValue() && !isCompatibleForOperator(mf.getType(), op, val)) ||
                        ((mf.isMultipleValues() && !(isCompatibleForOperator(mf.getSubClass(), op,
                                                                             val) || isCompatibleForOperator(mf





                                                                                                                                                                          .getType(), op, val))))) {


                    if (log.isWarningEnabled()) {
                        final Throwable t = new Throwable();
                        final StackTraceElement ste = getFirstClientLine(t);
                        log.warning("The type(s) for the query/update may be inconsistent; using an instance of type '"
                                            + val.getClass().getName() + "' for the field '" + mf.getDeclaringClass()
                                .getName() + "." + mf.getJavaFieldName()
                                            + "' which is declared as '" + mf.getType().getName() + (ste == null ? "'" : "'\r\n --@--" + ste));

                        if (log.isDebugEnabled()) {
                            log.debug("Location of warning:\r\n", t);
                        }
                    }
                }
            }
        }
        return mf;
    }

    /**
     * Return the first {@link StackTraceElement} not in our code (package).
     */
    private static StackTraceElement getFirstClientLine(final Throwable t) {
        for (final StackTraceElement ste : t.getStackTrace()) {
            if (!ste.getClassName().startsWith("com.google.code.morphia") &&
                    !ste.getClassName().startsWith("sun.reflect") &&
                    !ste.getClassName().startsWith("org.junit") &&
                    !ste.getClassName().startsWith("org.eclipse") &&
                    !ste.getClassName().startsWith("java.lang")) {
                return ste;
            }
        }

        return null;
    }

    /**
     * Returns if the MappedField is a Reference or Serilized
     */
    private static boolean canQueryPast(final MappedField mf) {
        return !(mf.hasAnnotation(Reference.class) || mf.hasAnnotation(Serialized.class));
    }

    public static boolean isCompatibleForOperator(final Class<?> type, final FilterOperator op, final Object value) {
        if (value == null || type == null) {
            return true;
        }
        else if (op.equals(FilterOperator.EXISTS) && (value instanceof Boolean)) {
            return true;
        }
        else if (op.equals(FilterOperator.IN) && (value.getClass().isArray() || Iterable.class.isAssignableFrom(value

                                                                                                                        .getClass()) || Map.class.isAssignableFrom(value.getClass()))) {
            return true;
        }
        else if (op.equals(FilterOperator.NOT_IN) && (value.getClass().isArray() || Iterable.class.isAssignableFrom(value.getClass()) || Map.class.isAssignableFrom(value.getClass()))) {
            return true;
        }
        else if (op.equals(FilterOperator.ALL) && (value.getClass().isArray() || Iterable.class.isAssignableFrom(value.getClass()) || Map.class.isAssignableFrom(value.getClass()))) {
            return true;
        }
        else if (value instanceof Integer && (int.class.equals(type) || long.class.equals(type) || Long.class.equals
                (type))) {
            return true;
        }
        else if ((value instanceof Integer || value instanceof Long) && (double.class.equals(type) || Double.class.equals(type))) {
            return true;
        }
        else if (value instanceof Pattern && String.class.equals(type)) {
            return true;
        }
        else if (value.getClass().getAnnotation(Entity.class) != null && Key.class.equals(type)) {
            return true;
        }
        else if (value instanceof List<?>) {
            return true;
        }
        else if (!value.getClass().isAssignableFrom(type) &&
                //hack to let Long match long, and so on
                !value.getClass().getSimpleName().toLowerCase().equals(type.getSimpleName().toLowerCase())) {
            return false;
        }
        return true;
    }

    public Class<?> getClassFromKind(final String kind) {
        final Set<MappedClass> mcs = mappedClassesByCollection.get(kind);
        if (mcs.isEmpty()) {
            throw new MappingException("The collection '" + kind + "' is not mapped to a java class.");
        }
        if (mcs.size() > 1) {
            if (log.isInfoEnabled()) {
                log.info("Found more than one class mapped to collection '" + kind + "'" + mcs);
            }
        }
        return mcs.iterator().next().getClazz();
    }
}
