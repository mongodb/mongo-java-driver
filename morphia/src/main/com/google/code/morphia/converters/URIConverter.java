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

import java.net.URI;

/**
 * @author scotthernandez
 */
@SuppressWarnings("rawtypes")
public class URIConverter extends TypeConverter implements SimpleValueConverter {

    public URIConverter() {
        this(URI.class);
    }

    protected URIConverter(final Class clazz) {
        super(clazz);
    }

    @Override
    public String encode(final Object uri, final MappedField optionalExtraInfo) {
        if (uri == null) {
            return null;
        }

        return ((URI) uri).toString().replace(".", "%46");
    }

    @Override
    public Object decode(final Class targetClass, final Object val, final MappedField optionalExtraInfo) {
        if (val == null) {
            return null;
        }

        return URI.create(val.toString().replace("%46", "."));
    }
}
