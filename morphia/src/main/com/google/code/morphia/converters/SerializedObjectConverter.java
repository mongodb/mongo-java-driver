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

import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.mapping.Serializer;
import org.bson.types.Binary;

import java.io.IOException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings("unchecked")
public class SerializedObjectConverter extends TypeConverter {
    @Override
    protected boolean isSupported(final Class c, final MappedField optionalExtraInfo) {
        if (optionalExtraInfo != null) {
            return (optionalExtraInfo.hasAnnotation(Serialized.class));
        }
        else {
            return false;
        }
    }

    @Override
    public Object decode(final Class targetClass, final Object fromDBObject,
                         final MappedField f) throws MappingException {
        if (fromDBObject == null) {
            return null;
        }

        if (!((fromDBObject instanceof Binary) || (fromDBObject instanceof byte[]))) {
            throw new MappingException("The stored data is not a DBBinary or byte[] instance for " + f.getFullName()
                                               + " ; it is a " + fromDBObject.getClass().getName());
        }

        try {
            final boolean useCompression = !f.getAnnotation(Serialized.class).disableCompression();
            return Serializer.deserialize(fromDBObject, useCompression);
        } catch (IOException e) {
            throw new MappingException("While deserializing to " + f.getFullName(), e);
        } catch (ClassNotFoundException e) {
            throw new MappingException("While deserializing to " + f.getFullName(), e);
        }
    }

    @Override
    public Object encode(final Object value, final MappedField f) {
        if (value == null) {
            return null;
        }
        try {
            final boolean useCompression = !f.getAnnotation(Serialized.class).disableCompression();
            return Serializer.serialize(value, useCompression);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
