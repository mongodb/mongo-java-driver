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
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class EmbeddedAndValue implements ClassConstraint {

    public void check(final MappedClass mc, final Set<ConstraintViolation> ve) {

        if (mc.getEmbeddedAnnotation() != null && !mc.getEmbeddedAnnotation().value().equals(Mapper

                                                                                             .IGNORED_FIELDNAME)) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, this.getClass(), "@" + Embedded.class.getSimpleName()
                                                                             + " classes cannot specify a fieldName " +
                                                                             "value(); this is on applicable on fields"));
        }
    }

}
