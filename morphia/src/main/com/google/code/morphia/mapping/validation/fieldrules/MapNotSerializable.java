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
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;
import com.google.code.morphia.utils.ReflectionUtils;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MapNotSerializable extends FieldConstraint {

    @Override
    protected void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.isMap()) {
            if (mf.hasAnnotation(Serialized.class)) {
                final Class<?> keyClass = ReflectionUtils.getParameterizedClass(mf.getField(), 0);
                final Class<?> valueClass = ReflectionUtils.getParameterizedClass(mf.getField(), 1);
                if (keyClass != null) {
                    if (!Serializable.class.isAssignableFrom(keyClass)) {
                        ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                                       "Key class (" + keyClass.getName()
                                                               + ") is not Serializable"));
                    }
                }
                if (valueClass != null) {
                    if (!Serializable.class.isAssignableFrom(keyClass)) {
                        ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                                       "Value class (" + valueClass.getName()
                                + ") is not Serializable"));
                    }
                }
            }
        }
    }
}
