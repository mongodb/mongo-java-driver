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

import com.google.code.morphia.EntityInterceptor;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.EntityListeners;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexes;
import com.google.code.morphia.annotations.Polymorphic;
import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.validation.MappingValidator;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.DBObject;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a mapped class between the MongoDB DBObject and the java POJO.
 * <p/>
 * This class will validate classes to make sure they meet the requirement for persistence.
 *
 * @author Scott Hernandez
 */
@SuppressWarnings("unchecked")
public class MappedClass {
    private static final Logr LOG = MorphiaLoggerFactory.get(MappedClass.class);

    private static class ClassMethodPair {
        private final Class<?> clazz;
        private final Method method;

        public ClassMethodPair(final Class<?> c, final Method m) {
            clazz = c;
            method = m;
        }
    }

    /**
     * special fields representing the Key of the object
     */
    private Field idField;

    /**
     * special annotations representing the type the object
     */
    private Entity entityAn;
    private Embedded embeddedAn;
    //    private Polymorphic polymorphicAn;

    /**
     * Annotations we are interested in looking for.
     */
    public static final List<Class<? extends Annotation>> interestingAnnotations = new ArrayList<Class<? extends
            Annotation>>(Arrays.asList(
            Embedded.class,
            Entity.class,
            Polymorphic.class,
            EntityListeners.class,
            Version.class,
            Converters.class,
            Indexes.class));
    /**
     * Annotations interesting for life-cycle events
     */
    private static final Class<? extends Annotation>[] lifecycleAnnotations = new Class[]{
            PrePersist.class,
            PreSave.class,
            PostPersist.class,
            PreLoad.class,
            PostLoad.class};

    /**
     * Annotations we were interested in, and found.
     */
    private final Map<Class<? extends Annotation>, ArrayList<Annotation>> foundAnnotations = new HashMap<Class<? extends
            Annotation>, ArrayList<Annotation>>();

    /**
     * Methods which are life-cycle events
     */
    private final Map<Class<? extends Annotation>, List<ClassMethodPair>> lifecycleMethods = new HashMap<Class<? extends
            Annotation>, List<ClassMethodPair>>();

    /**
     * a list of the fields to map
     */
    private final List<MappedField> persistenceFields = new ArrayList<MappedField>();

    /**
     * the type we are mapping to/from
     */
    private final Class<?> clazz;
    final Mapper mapr;

    /**
     * constructor
     */
    public MappedClass(final Class<?> clazz, final Mapper mapr) {
        this.mapr = mapr;
        this.clazz = clazz;

        if (LOG.isTraceEnabled()) {
            LOG.trace("Creating MappedClass for " + clazz);
        }

        basicValidate();
        discover();

        if (LOG.isDebugEnabled()) {
            LOG.debug("MappedClass done: " + toString());
        }
    }

    protected void basicValidate() {
        final boolean isstatic = Modifier.isStatic(clazz.getModifiers());
        if (!isstatic && clazz.isMemberClass()) {
            throw new MappingException("Cannot use non-static inner class: " + clazz + ". Please make static.");
        }
    }

    /*
     * Update mappings based on fields/annotations.
     */
    // TODO: Remove this and make these fields dynamic or auto-set some other way
    public void update() {
        embeddedAn = (Embedded) getAnnotation(Embedded.class);
        entityAn = (Entity) getAnnotation(Entity.class);
        // polymorphicAn = (Polymorphic) getAnnotation(Polymorphic.class);
        final List<MappedField> fields = getFieldsAnnotatedWith(Id.class);
        if (fields != null && fields.size() > 0) {
            idField = fields.get(0).field;
        }


    }

    /**
     * Discovers interesting (that we care about) things about the class.
     */
    protected void discover() {
        for (final Class<? extends Annotation> c : interestingAnnotations) {
            addAnnotation(c);
        }

        final List<Class<?>> lifecycleClasses = new ArrayList<Class<?>>();
        lifecycleClasses.add(clazz);

        final EntityListeners entityLisAnn = (EntityListeners) getAnnotation(EntityListeners.class);
        if (entityLisAnn != null && entityLisAnn.value() != null && entityLisAnn.value().length != 0) {
            Collections.addAll(lifecycleClasses, entityLisAnn.value());
        }

        for (final Class<?> cls : lifecycleClasses) {
            for (final Method m : ReflectionUtils.getDeclaredAndInheritedMethods(cls)) {
                for (final Class<? extends Annotation> c : lifecycleAnnotations) {
                    if (m.isAnnotationPresent(c)) {
                        addLifecycleEventMethod(c, m, cls.equals(clazz) ? null : cls);
                    }
                }
            }
        }

        update();

        for (final Field field : ReflectionUtils.getDeclaredAndInheritedFields(clazz, true)) {
            field.setAccessible(true);
            final int fieldMods = field.getModifiers();
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }
            else if (field.isSynthetic() && (fieldMods & Modifier.TRANSIENT) == Modifier.TRANSIENT) {
                continue;
            }
            else if (mapr.getOptions().actLikeSerializer && ((fieldMods & Modifier.TRANSIENT) == Modifier.TRANSIENT)) {
                continue;
            }
            else if (mapr.getOptions().ignoreFinals && ((fieldMods & Modifier.FINAL) == Modifier.FINAL)) {
                continue;
            }
            else if (field.isAnnotationPresent(Id.class)) {
                final MappedField mf = new MappedField(field, clazz);
                persistenceFields.add(mf);
                update();
            }
            else if (field.isAnnotationPresent(Property.class) ||
                    field.isAnnotationPresent(Reference.class) ||
                    field.isAnnotationPresent(Embedded.class) ||
                    field.isAnnotationPresent(Serialized.class) ||
                    isSupportedType(field.getType()) ||
                    ReflectionUtils.implementsInterface(field.getType(), Serializable.class)) {
                persistenceFields.add(new MappedField(field, clazz));
            }
            else {
                if (mapr.getOptions().defaultMapper != null) {
                    persistenceFields.add(new MappedField(field, clazz));
                }
                else if (LOG.isWarningEnabled()) {
                    LOG.warning("Ignoring (will not persist) field: " + clazz.getName() + "." + field.getName() + " " +
                                        "[type:" + field.getType().getName() + "]");
                }
            }
        }
    }

    private void addLifecycleEventMethod(final Class<? extends Annotation> lceClazz, final Method m,
                                         final Class<?> clazz) {
        final ClassMethodPair cm = new ClassMethodPair(clazz, m);
        if (lifecycleMethods.containsKey(lceClazz)) {
            lifecycleMethods.get(lceClazz).add(cm);
        }
        else {
            final ArrayList<ClassMethodPair> methods = new ArrayList<ClassMethodPair>();
            methods.add(cm);
            lifecycleMethods.put(lceClazz, methods);
        }
    }

    public void addAnnotation(final Class<? extends Annotation> clazz, final Annotation ann) {
        if (ann == null || clazz == null) {
            return;
        }

        if (!this.foundAnnotations.containsKey(clazz)) {
            final ArrayList<Annotation> list = new ArrayList<Annotation>();
            foundAnnotations.put(clazz, list);
        }

        foundAnnotations.get(clazz).add(ann);
    }

    public List<ClassMethodPair> getLifecycleMethods(final Class<Annotation> clazz) {
        return lifecycleMethods.get(clazz);
    }

    /**
     * Adds the annotation, if it exists on the field.
     *
     * @param clazz
     */
    private void addAnnotation(final Class<? extends Annotation> clazz) {
        final ArrayList<? extends Annotation> anns = ReflectionUtils.getAnnotations(getClazz(), clazz);
        for (final Annotation ann : anns) {
            addAnnotation(clazz, ann);
        }
    }

    @Override
    public String toString() {
        return "MappedClass - kind:" + this.getCollectionName() + " for " + this.getClazz().getName() + " fields:" +
                persistenceFields;
    }

    /**
     * Returns fields annotated with the clazz
     */
    public List<MappedField> getFieldsAnnotatedWith(final Class<? extends Annotation> clazz) {
        final List<MappedField> results = new ArrayList<MappedField>();
        for (final MappedField mf : persistenceFields) {
            if (mf.foundAnnotations.containsKey(clazz)) {
                results.add(mf);
            }
        }
        return results;
    }

    /**
     * Returns the MappedField by the name that it will stored in mongodb as
     */
    public MappedField getMappedField(final String storedName) {
        for (final MappedField mf : persistenceFields) {
            for (final String n : mf.getLoadNames()) {
                if (storedName.equals(n)) {
                    return mf;
                }
            }
        }

        return null;
    }

    /**
     * Check java field name that will stored in mongodb
     */
    public boolean containsJavaFieldName(final String name) {
        return getMappedField(name) != null;
    }

    /**
     * Returns MappedField for a given java field name on the this MappedClass
     */
    public MappedField getMappedFieldByJavaField(final String name) {
        for (final MappedField mf : persistenceFields) {
            if (name.equals(mf.getJavaFieldName())) {
                return mf;
            }
        }

        return null;
    }

    /**
     * Checks to see if it a Map/Set/List or a property supported by the MangoDB java driver
     */
    public static boolean isSupportedType(final Class<?> clazz) {
        if (ReflectionUtils.isPropertyType(clazz)) {
            return true;
        }
        if (clazz.isArray() || Map.class.isAssignableFrom(clazz) || Iterable.class.isAssignableFrom(clazz)) {
            Class<?> subType = null;
            if (clazz.isArray()) {
                subType = clazz.getComponentType();
            }
            else {
                subType = ReflectionUtils.getParameterizedClass(clazz);
            }

            //get component type, String.class from List<String>
            if (subType != null && subType != Object.class && !ReflectionUtils.isPropertyType(subType)) {
                return false;
            }

            //either no componentType or it is an allowed type
            return true;
        }
        return false;
    }

    @SuppressWarnings("deprecation")
    public void validate() {
        new MappingValidator().validate(this);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof Class<?>) {
            return equals((Class<?>) obj);
        }
        else return obj instanceof MappedClass && equals((MappedClass) obj);
    }

    public boolean equals(final MappedClass clazz) {
        return this.getClazz().equals(clazz.getClazz());
    }

    public boolean equals(final Class<?> clazz) {
        return this.getClazz().equals(clazz);
    }

    /**
     * Call the lifcycle methods
     */
    public DBObject callLifecycleMethods(final Class<? extends Annotation> event, final Object entity,
                                         final DBObject dbObj,
                                         final Mapper mapr) {
        final List<ClassMethodPair> methodPairs = getLifecycleMethods((Class<Annotation>) event);
        DBObject retDbObj = dbObj;
        try {
            Object tempObj = null;
            if (methodPairs != null) {
                final HashMap<Class<?>, Object> toCall =
                        new HashMap<Class<?>, Object>((int) (methodPairs.size() * 1.3));
                for (final ClassMethodPair cm : methodPairs) {
                    toCall.put(cm.clazz, null);
                }
                for (final Class<?> c : toCall.keySet()) {
                    if (c != null) {
                        toCall.put(c, getOrCreateInstance(c));
                    }
                }

                for (final ClassMethodPair cm : methodPairs) {
                    final Method method = cm.method;
                    final Class<?> type = cm.clazz;

                    final Object inst = toCall.get(type);
                    method.setAccessible(true);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Calling lifecycle method(@" + event.getSimpleName() + " " + method + ") on " +
                                          inst + "");
                    }

                    if (inst == null) {
                        if (method.getParameterTypes().length == 0) {
                            tempObj = method.invoke(entity);
                        }
                        else {
                            tempObj = method.invoke(entity, retDbObj);
                        }
                    }
                    else if (method.getParameterTypes().length == 0) {
                        tempObj = method.invoke(inst);
                    }
                    else if (method.getParameterTypes().length == 1) {
                        tempObj = method.invoke(inst, entity);
                    }
                    else {
                        tempObj = method.invoke(inst, entity, retDbObj);
                    }

                    if (tempObj != null) {
                        retDbObj = (DBObject) tempObj;
                    }
                }
            }

            callGlobalInterceptors(event, entity, dbObj, mapr, mapr.getInterceptors());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        return retDbObj;
    }

    private Object getOrCreateInstance(final Class<?> clazz) {
        if (mapr.instanceCache.containsKey(clazz)) {
            return mapr.instanceCache.get(clazz);
        }

        final Object o = mapr.getOptions().objectFactory.createInstance(clazz);
        final Object nullO = mapr.instanceCache.put(clazz, o);
        if (nullO != null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Race-condition, created duplicate class: " + clazz);
            }
        }

        return o;

    }

    private void callGlobalInterceptors(final Class<? extends Annotation> event, final Object entity,
                                        final DBObject dbObj, final Mapper mapr,
                                        final Collection<EntityInterceptor> interceptors) {
        for (final EntityInterceptor ei : interceptors) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling interceptor method " + event.getSimpleName() + " on " + ei);
            }

            if (event.equals(PreLoad.class)) {
                ei.preLoad(entity, dbObj, mapr);
            }
            else if (event.equals(PostLoad.class)) {
                ei.postLoad(entity, dbObj, mapr);
            }
            else if (event.equals(PrePersist.class)) {
                ei.prePersist(entity, dbObj, mapr);
            }
            else if (event.equals(PreSave.class)) {
                ei.preSave(entity, dbObj, mapr);
            }
            else if (event.equals(PostPersist.class)) {
                ei.postPersist(entity, dbObj, mapr);
            }
        }
    }

    /**
     * @return the idField
     */
    public Field getIdField() {
        return idField;
    }

    /**
     * @return the entityAn
     */
    public Entity getEntityAnnotation() {
        return entityAn;
    }

    /**
     * @return the embeddedAn
     */
    public Embedded getEmbeddedAnnotation() {
        return embeddedAn;
    }

    /**
     * @return the releventAnnotations
     */
    public Map<Class<? extends Annotation>, ArrayList<Annotation>> getReleventAnnotations() {
        return foundAnnotations;
    }

    /**
     * @return the instance if it was found, if more than onw was found, the last one added
     */
    public Annotation getAnnotation(final Class<? extends Annotation> clazz) {
        final ArrayList<Annotation> found = foundAnnotations.get(clazz);
        return (found != null && found.size() > 0) ? found.get(found.size() - 1) : null;
    }

    /**
     * @return the instance if it was found, if more than onw was found, the last one added
     */
    public ArrayList<Annotation> getAnnotations(final Class<? extends Annotation> clazz) {
        return foundAnnotations.get(clazz);
    }

    /**
     * @return the persistenceFields
     */
    public List<MappedField> getPersistenceFields() {
        return persistenceFields;
    }

    /**
     * @return the collName
     */
    public String getCollectionName() {
        return (entityAn == null || entityAn.value().equals(Mapper.IGNORED_FIELDNAME)) ? clazz.getSimpleName() :
                entityAn.value();
    }

    /**
     * @return the clazz
     */
    public Class<?> getClazz() {
        return clazz;
    }

    /**
     * @return the Mapper this class is bound to
     */
    public Mapper getMapper() {
        return mapr;
    }

    public MappedField getMappedIdField() {
        return getFieldsAnnotatedWith(Id.class).get(0);
    }

}