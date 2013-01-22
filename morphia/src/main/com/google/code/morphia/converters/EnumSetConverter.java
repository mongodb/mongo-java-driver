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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EnumSetConverter extends TypeConverter implements SimpleValueConverter {

    private final EnumConverter ec = new EnumConverter();

    public EnumSetConverter() {
        super(EnumSet.class);
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo) {
        if (fromDBObject == null) {
            return null;
        }

        final Class enumType = optionalExtraInfo.getSubClass();

        final List l = (List) fromDBObject;
        if (l.isEmpty()) {
            return EnumSet.noneOf(enumType);
        }

        final ArrayList enums = new ArrayList();
        for (final Object object : l) {
            enums.add(ec.decode(enumType, object));
        }
        final EnumSet copyOf = EnumSet.copyOf(enums);
        return copyOf;
    }

    @Override
    public Object encode(final Object value, final MappedField optionalExtraInfo) {
        if (value == null) {
            return null;
        }

        final ArrayList values = new ArrayList();

        final EnumSet s = (EnumSet) value;
        final Object[] array = s.toArray();
        for (int i = 0; i < array.length; i++) {
            values.add(ec.encode(array[i]));
        }

        return values;
    }
}
