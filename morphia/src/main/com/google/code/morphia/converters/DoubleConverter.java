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
import com.google.code.morphia.utils.ReflectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings("rawtypes")
public class DoubleConverter extends TypeConverter implements SimpleValueConverter {

    public DoubleConverter() {
        super(double.class, Double.class);
    }

    @Override
    public Object decode(final Class targetClass, final Object val, final MappedField optionalExtraInfo) {
        if (val == null) {
            return null;
        }

        if (val instanceof Double) {
            return (Double) val;
        }

        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }

        //FixMe: super-hacky
        if (// val instanceof LazyBSONList ||  // TODO: May have to replace this with something else
                val instanceof ArrayList) {
            return ReflectionUtils.convertToArray(double.class, (List<?>) val);
        }

        final String sVal = val.toString();
        return Double.parseDouble(sVal);
    }
}
