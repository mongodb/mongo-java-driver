/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.json;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.lang.String.format;


class ShellDateTimeConverter implements Converter<Long> {
    @Override
    public void convert(final Long value, final StrictJsonWriter writer) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (value >= -59014396800000L && value <= 253399536000000L) {
            writer.writeRaw(format("ISODate(\"%s\")", dateFormat.format(new Date(value))));
        } else {
            writer.writeRaw(format("new Date(%d)", value));
        }
    }
}
