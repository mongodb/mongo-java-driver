/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
package com.google.code.morphia.converters;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.MapperOptions;
import com.google.code.morphia.mapping.MappingException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default encoders
 *
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DefaultConverters {
    private static final Logr LOG = MorphiaLoggerFactory.get(DefaultConverters.class);

    private final List<TypeConverter> untypedTypeEncoders = new LinkedList<TypeConverter>();
    private final Map<Class, List<TypeConverter>> tcMap = new ConcurrentHashMap<Class, List<TypeConverter>>();
    private final List<Class<? extends TypeConverter>> registeredConverterClasses
        = new LinkedList<Class<? extends TypeConverter>>();

    private Mapper mapr;

    public DefaultConverters() {
        // some converters are commented out since the pass-through converter is enabled, at the end of the list.
        // Re-enable them if that changes.
        // addConverter(new PassthroughConverter(DBRef.class));

        //Pass-through DBObject or else the MapOfValuesConverter will process it.
        addConverter(new PassthroughConverter(DBObject.class, BasicDBObject.class));
        //Pass-through byte[] for the driver to handle
        addConverter(new ByteArrayConverter());
        addConverter(new EnumSetConverter());
        addConverter(new EnumConverter());
        addConverter(new StringConverter());
        addConverter(new CharacterConverter());
        addConverter(new ByteConverter());
        addConverter(new BooleanConverter());
        addConverter(new DoubleConverter());
        addConverter(new FloatConverter());
        addConverter(new LongConverter());
        addConverter(new LocaleConverter());
        addConverter(new ShortConverter());
        addConverter(new IntegerConverter());
        addConverter(new SerializedObjectConverter());
        addConverter(new CharArrayConverter());
        addConverter(new DateConverter());
        addConverter(new URIConverter());
        addConverter(new KeyConverter());
        addConverter(new MapOfValuesConverter(this));
        addConverter(new IterableConverter(this));
        addConverter(new ClassConverter());
        addConverter(new ObjectIdConverter());
        addConverter(new TimestampConverter());

        //generic converter that will just pass things through.
        addConverter(new PassthroughConverter());
    }

    /**
     * Add a type converter. If it is a duplicate for an existing type, it will override that type.
     *
     * @param tc
     */
    public TypeConverter addConverter(final TypeConverter tc) {
        if (tc.getSupportedTypes() != null) {
            for (final Class c : tc.getSupportedTypes()) {
                addTypedConverter(c, tc);
            }
        }
        else {
            untypedTypeEncoders.add(tc);
        }

        tc.setMapper(mapr);

        registeredConverterClasses.add(tc.getClass());
        return tc;
    }

    public TypeConverter addConverter(final Class<? extends TypeConverter> clazz) {
        return addConverter((TypeConverter) this.mapr.getOptions().objectFactory.createInstance(clazz));
    }

    /**
     * Removes the type converter.
     *
     * @param tc
     */
    public void removeConverter(final TypeConverter tc) {
        if (tc.getSupportedTypes() == null) {
            untypedTypeEncoders.remove(tc);
        }
        else {
            for (final List<TypeConverter> tcList : tcMap.values()) {
                if (tcList.contains(tc)) {
                    tcList.remove(tc);
                }
            }
        }

        registeredConverterClasses.remove(tc.getClass());
    }

    public boolean isRegistered(final Class<? extends TypeConverter> tcClass) {
        return registeredConverterClasses.contains(tcClass);
    }

    private void addTypedConverter(final Class type, final TypeConverter tc) {
        if (tcMap.containsKey(type)) {
            tcMap.get(type).add(0, tc);
            LOG.warning("Added duplicate converter for " + type + " ; " + tcMap.get(type));
        }
        else {
            final ArrayList<TypeConverter> vals = new ArrayList<TypeConverter>();
            vals.add(tc);
            tcMap.put(type, vals);
        }
    }

    public void fromDBObject(final DBObject dbObj, final MappedField mf, final Object targetEntity) {
        final Object object = mf.getDbObjectValue(dbObj);
        if (object == null) {
            processMissingField(mf);
        }
        else {
            final TypeConverter enc = getEncoder(mf);
            final Object decodedValue = enc.decode(mf.getType(), object, mf);
            try {
                mf.setFieldValue(targetEntity, decodedValue);
            } catch (IllegalArgumentException e) {
                throw new MappingException("Error setting value from converter (" + enc.getClass().getSimpleName()
                                           + ") for " + mf.getFullName() + " to " + decodedValue);
            }
        }
    }

    protected void processMissingField(final MappedField mf) {
        //we ignore missing values.
    }

    private TypeConverter getEncoder(final MappedField mf) {
        return getEncoder(null, mf);
    }

    private TypeConverter getEncoder(final Object val, final MappedField mf) {

        List<TypeConverter> tcs = null;

        if (val != null) {
            tcs = tcMap.get(val.getClass());
        }

        if (tcs == null || (tcs.size() > 0 && tcs.get(0) instanceof PassthroughConverter)) {
            tcs = tcMap.get(mf.getType());
        }

        if (tcs != null) {
            if (tcs.size() > 1) {
                LOG.warning("Duplicate converter for " + mf.getType() + ", returning first one from " + tcs);
            }
            return tcs.get(0);
        }

        for (final TypeConverter tc : untypedTypeEncoders) {
            if (tc.canHandle(mf) || (val != null && tc.isSupported(val.getClass(), mf))) {
                return tc;
            }
        }

        throw new ConverterNotFoundException("Cannot find encoder for " + mf.getType() + " as need for "
                                             + mf.getFullName());
    }

    private TypeConverter getEncoder(final Class c) {
        final List<TypeConverter> tcs = tcMap.get(c);
        if (tcs != null) {
            if (tcs.size() > 1) {
                LOG.warning("Duplicate converter for " + c + ", returning first one from " + tcs);
            }
            return tcs.get(0);
        }

        for (final TypeConverter tc : untypedTypeEncoders) {
            if (tc.canHandle(c)) {
                return tc;
            }
        }

        throw new ConverterNotFoundException("Cannot find encoder for " + c.getName());
    }

    public void toDBObject(final Object containingObject, final MappedField mf, final DBObject dbObj,
                           final MapperOptions opts) {
        final Object fieldValue = mf.getFieldValue(containingObject);
        final TypeConverter enc = getEncoder(fieldValue, mf);

        final Object encoded = enc.encode(fieldValue, mf);
        if (encoded != null || opts.storeNulls) {
            dbObj.put(mf.getNameToStore(), encoded);
        }
    }

    public Object decode(Class c, final Object fromDBObject, final MappedField mf) {
        if (c == null) {
            c = fromDBObject.getClass();
        }
        return getEncoder(c).decode(c, fromDBObject, mf);
    }

    public Object decode(final Class c, final Object fromDBObject) {
        return decode(c, fromDBObject, null);
    }

    public Object encode(final Object o) {
        if (o == null) {
            return null;
        }
        return encode(o.getClass(), o);
    }

    public Object encode(final Class c, final Object o) {
        return getEncoder(c).encode(o);
    }

    public void setMapper(final Mapper mapr) {
        this.mapr = mapr;
        for (final List<TypeConverter> tcs : tcMap.values()) {
            for (final TypeConverter tc : tcs) {
                tc.setMapper(mapr);
            }
        }
        for (final TypeConverter tc : untypedTypeEncoders) {
            tc.setMapper(mapr);
        }
    }

    public boolean hasSimpleValueConverter(final MappedField c) {
        final TypeConverter conv = getEncoder(c);
        return (conv instanceof SimpleValueConverter);
    }

    public boolean hasSimpleValueConverter(final Class c) {
        final TypeConverter conv = getEncoder(c);
        return (conv instanceof SimpleValueConverter);
    }

    public boolean hasSimpleValueConverter(final Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Class) {
            return hasSimpleValueConverter((Class) o);
        }
        else if (o instanceof MappedField) {
            return hasSimpleValueConverter((MappedField) o);
        }
        else {
            return hasSimpleValueConverter(o.getClass());
        }
    }


    public boolean hasDbObjectConverter(final MappedField c) {
        final TypeConverter conv = getEncoder(c);
        return conv != null && !(conv instanceof PassthroughConverter) && !(conv instanceof SimpleValueConverter);
    }

    public boolean hasDbObjectConverter(final Class c) {
        final TypeConverter conv = getEncoder(c);
        return conv != null && !(conv instanceof PassthroughConverter) && !(conv instanceof SimpleValueConverter);
    }
}
