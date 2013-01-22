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

package com.google.code.morphia.issue173;

import com.google.code.morphia.converters.SimpleValueConverter;
import com.google.code.morphia.converters.TypeConverter;
import com.google.code.morphia.mapping.MappedField;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

@SuppressWarnings("rawtypes")
public class CalendarConverter extends TypeConverter implements SimpleValueConverter {
    public CalendarConverter() {
        super(Calendar.class);
    }

    @Override
    public Object encode(final Object val, final MappedField optionalExtraInfo) {
        if (val == null) {
            return null;
        }
        final Calendar calendar = (Calendar) val;
        final long millis = calendar.getTimeInMillis();
        // . a date so that we can see it clearly in MongoVue
        // . the date is UTC because
        //   . timeZone.getOffset(millis) - timeZone.getOffset(newMillis)  may not be 0 (if we're close to DST limits)
        //   . and it's like that inside GregorianCalendar => more natural
        final Date utcDate = new Date(millis);
        final List<Object> vals = new ArrayList<Object>();
        vals.add(utcDate);
        vals.add(calendar.getTimeZone().getID());
        return vals;
    }

    @Override
    public Object decode(final Class type, final Object o, final MappedField mf) {
        if (o == null) {
            return null;
        }
        final List vals = (List) o;
        if (vals.size() < 2) {
            return null;
        }
        //-- date --//
        final Date utcDate = (Date) vals.get(0);
        final long millis = utcDate.getTime();

        //-- TimeZone --//
        final String timeZoneId = (String) vals.get(1);
        final TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);
        //-- GregorianCalendar construction --//
        final GregorianCalendar calendar = new GregorianCalendar(timeZone);
        calendar.setTimeInMillis(millis);
        return calendar;
    }
}