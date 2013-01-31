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

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.utils.IterHelper;
import com.google.code.morphia.utils.IterHelper.MapIterCallback;
import com.google.code.morphia.utils.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapOfValuesConverter extends TypeConverter {
    private final DefaultConverters converters;

    public MapOfValuesConverter(final DefaultConverters converters) {
        this.converters = converters;
    }

    @Override
    protected boolean isSupported(final Class<?> c, final MappedField optionalExtraInfo) {
        if (optionalExtraInfo != null) {
            return optionalExtraInfo.isMap();
        }
        else {
            return ReflectionUtils.implementsInterface(c, Map.class);
        }
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject,
                         final MappedField mf) {
        if (fromDBObject == null) {
            return null;
        }


        final Map values = mapr.getOptions().objectFactory.createMap(mf);
        new IterHelper<Object, Object>().loopMap(fromDBObject, new MapIterCallback<Object, Object>() {
            @Override
            public void eval(final Object key, final Object val) {
                final Object objKey = converters.decode(mf.getMapKeyClass(), key);
                values.put(objKey, converters.decode(mf.getSubClass(), val));
            }
        });

        return values;
    }

    @Override
    public Object encode(final Object value, final MappedField mf) {
        if (value == null) {
            return null;
        }

        final Map<Object, Object> map = (Map<Object, Object>) value;
        if ((map != null) && (map.size() > 0)) {
            final Map mapForDb = new HashMap();
            for (final Map.Entry<Object, Object> entry : map.entrySet()) {
                final String strKey = converters.encode(entry.getKey()).toString();
                mapForDb.put(strKey, converters.encode(entry.getValue()));
            }
            return mapForDb;
        }
        return null;
    }
}