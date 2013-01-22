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

import java.util.Locale;
import java.util.StringTokenizer;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class LocaleConverter extends TypeConverter implements SimpleValueConverter {

    public LocaleConverter() {
        super(Locale.class);
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo)
    {
        return parseLocale(fromDBObject.toString());
    }

    @Override
    public Object encode(final Object val, final MappedField optionalExtraInfo) {
        if (val == null) {
            return null;
        }

        return val.toString();
    }

    public static Locale parseLocale(final String localeString) {
        if ((localeString != null) && (localeString.length() > 0)) {
            final StringTokenizer st = new StringTokenizer(localeString, "_");
            final String language = st.hasMoreElements() ? st.nextToken() : Locale.getDefault().getLanguage();
            final String country = st.hasMoreElements() ? st.nextToken() : "";
            final String variant = st.hasMoreElements() ? st.nextToken() : "";
            return new Locale(language, country, variant);
        }
        return null;
    }
}
