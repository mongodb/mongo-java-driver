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
import org.bson.types.ObjectId;

import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MapKeyDifferentFromString extends FieldConstraint {
    private static final String SUPPORTED_EXAMPLE = "(Map<String/Enum/Long/ObjectId/..., ?>)";

    @Override
    protected void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.isMap() && (!mf.hasAnnotation(Serialized.class))) {
            final Class<?> parameterizedClass = ReflectionUtils.getParameterizedClass(mf.getField(), 0);
            if (parameterizedClass == null) {
                ve.add(new ConstraintViolation(Level.WARNING, mc, mf, this.getClass(),
                                              "Maps cannot be keyed by Object (Map<Object,"
                                              + "?>); Use a parametrized type that is supported "
                                              + SUPPORTED_EXAMPLE));
            }
            else if (!parameterizedClass.equals(String.class) && !parameterizedClass.equals(ObjectId.class)
                     && !ReflectionUtils.isPrimitiveLike(parameterizedClass)) {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                              "Maps must be keyed by a simple type "
                                              + SUPPORTED_EXAMPLE + "; "
                                              + parameterizedClass + " is not supported as a map key "
                                              + "type."));
            }
        }
    }
}
