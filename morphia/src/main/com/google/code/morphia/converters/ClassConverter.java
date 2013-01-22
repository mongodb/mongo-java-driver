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

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ClassConverter extends TypeConverter implements SimpleValueConverter {

    public ClassConverter() {
        super(Class.class);
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo) {
        if (fromDBObject == null) {
            return null;
        }

        final String l = fromDBObject.toString();
        try {
            return Class.forName(l);
        } catch (ClassNotFoundException e) {
            throw new MappingException("Cannot create class from Name '" + l + "'", e);
        }
    }

    @Override
    public Object encode(final Object value, final MappedField optionalExtraInfo) {
        if (value == null) {
            return null;
        }
        else {
            return ((Class) value).getName();
        }
    }
}
