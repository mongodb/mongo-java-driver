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
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ContradictingFieldAnnotation extends FieldConstraint {

    private final Class<? extends Annotation> a1;
    private final Class<? extends Annotation> a2;

    public ContradictingFieldAnnotation(final Class<? extends Annotation> a1, final Class<? extends Annotation> a2) {
        this.a1 = a1;
        this.a2 = a2;
    }

    @Override
    protected final void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.hasAnnotation(a1) && mf.hasAnnotation(a2)) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                          "A field can be either annotated with @" + a1.getSimpleName()
                                          + " OR @" + a2.getSimpleName()
                                          + ", but not both."));
        }
    }
}
