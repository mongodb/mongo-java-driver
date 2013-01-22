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

import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.MappedField;
import com.mongodb.DBRef;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class KeyConverter extends TypeConverter {

    public KeyConverter() {
        super(Key.class);
    }

    @Override
    public Object decode(final Class targetClass, final Object o, final MappedField optionalExtraInfo) {
        if (o == null) {
            return null;
        }
        if (!(o instanceof DBRef)) {
            throw new ConverterException(String.format("cannot convert %s to Key because it isn't a DBRef",
                                                      o.toString()));
        }

        return mapr.refToKey((DBRef) o);
    }

    @Override
    public Object encode(final Object t, final MappedField optionalExtraInfo) {
        if (t == null) {
            return null;
        }
        return mapr.keyToRef((Key) t);
    }

}
