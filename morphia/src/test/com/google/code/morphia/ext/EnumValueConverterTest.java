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

package com.google.code.morphia.ext;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.converters.SimpleValueConverter;
import com.google.code.morphia.converters.TypeConverter;
import com.google.code.morphia.mapping.MappedField;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Example converter which stores the enum value instead of string (name)
 *
 * @author scotthernandez
 */
@SuppressWarnings("rawtypes")
public class EnumValueConverterTest extends TestBase {

    private static class AEnumConverter extends TypeConverter implements SimpleValueConverter {

        public AEnumConverter() {
            super(AEnum.class);
        }

        @Override
        public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo)
        {
            if (fromDBObject == null) {
                return null;
            }
            return AEnum.values()[(Integer) fromDBObject];
        }

        @Override
        public Object encode(final Object value, final MappedField optionalExtraInfo) {
            if (value == null) {
                return null;
            }

            return ((Enum) value).ordinal();
        }
    }

    private static enum AEnum {
        One,
        Two

    }

    @SuppressWarnings("unused")
    @Converters(AEnumConverter.class)
    private static class EnumEntity {
        @Id
        private final ObjectId id = new ObjectId();
        private final AEnum val = AEnum.Two;
    }

    @Test
    public void testEnum() {
        final EnumEntity ee = new EnumEntity();
        ds.save(ee);
        final DBObject dbObj = ds.getCollection(EnumEntity.class).findOne();
        Assert.assertEquals(1, dbObj.get("val"));
    }
}
