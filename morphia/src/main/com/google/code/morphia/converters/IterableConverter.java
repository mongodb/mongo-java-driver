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
package com.google.code.morphia.converters;

import com.google.code.morphia.ObjectFactory;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.utils.ReflectionUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */

@SuppressWarnings({"unchecked", "rawtypes"})
public class IterableConverter extends TypeConverter {
    private final DefaultConverters chain;

    public IterableConverter(final DefaultConverters chain) {
        this.chain = chain;
    }

    @Override
    protected boolean isSupported(final Class c, final MappedField mf) {
        if (mf != null) {
            return mf.isMultipleValues() && !mf.isMap(); //&& !mf.isTypeMongoCompatible();
        }
        else {
            return c.isArray() || ReflectionUtils.implementsInterface(c, Iterable.class);
        }
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject,
                         final MappedField mf) throws MappingException {
        if (mf == null || fromDBObject == null) {
            return fromDBObject;
        }

        final Class subtypeDest = mf.getSubClass();
        final Collection vals = createNewCollection(mf);

        if (fromDBObject.getClass().isArray()) {
            //This should never happen. The driver always returns list/arrays as a List
            for (final Object o : (Object[]) fromDBObject) {
                vals.add(chain.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o));
            }
        }
        else if (fromDBObject instanceof Iterable) {
            // map back to the java datatype
            // (List/Set/Array[])
            for (final Object o : (Iterable) fromDBObject) {
                vals.add(chain.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o));
            }
        }
        else {
            //Single value case.
            vals.add(chain.decode((subtypeDest != null) ? subtypeDest : fromDBObject.getClass(), fromDBObject));
        }

        //convert to and array if that is the destination type (not a list/set)
        if (mf.getType().isArray()) {
            return ReflectionUtils.convertToArray(subtypeDest, (ArrayList) vals);
        }
        else {
            return vals;
        }
    }

    private Collection<?> createNewCollection(final MappedField mf) {
        final ObjectFactory of = mapr.getOptions().objectFactory;
        return mf.isSet() ? of.createSet(mf) : of.createList(mf);
    }

    @Override
    public Object encode(final Object value, final MappedField mf) {

        if (value == null) {
            return null;
        }

        Iterable<?> iterableValues = null;

        if (value.getClass().isArray()) {

            if (Array.getLength(value) == 0) {
                return value;
            }

            if (value.getClass().getComponentType().isPrimitive()) {
                return value;
            }

            iterableValues = Arrays.asList((Object[]) value);
        }
        else {
            if (!(value instanceof Iterable)) {
                throw new ConverterException("Cannot cast " + value.getClass() + " to Iterable for MappedField: " + mf);
            }

            // cast value to a common interface
            iterableValues = (Iterable<?>) value;
        }

        final List values = new ArrayList();
        if (mf != null && mf.getSubClass() != null) {
            for (final Object o : iterableValues) {
                values.add(chain.encode(mf.getSubClass(), o));
            }
        }
        else {
            for (final Object o : iterableValues) {
                values.add(chain.encode(o));
            }
        }
        if (values.size() > 0 || mapr.getOptions().storeEmpties) {
            return values;
        }
        else {
            return null;
        }
    }
}
