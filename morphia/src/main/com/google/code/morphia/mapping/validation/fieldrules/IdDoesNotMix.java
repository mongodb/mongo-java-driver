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

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * @author ScottHenandez
 */
public class IdDoesNotMix extends FieldConstraint {

    @Override
    protected void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        // an @Id field can not be a Value, Reference, or Embedded
        if (mf.hasAnnotation(Id.class)) {
            if (mf.hasAnnotation(Reference.class) || mf.hasAnnotation(Embedded.class) || mf.hasAnnotation(Property




                                                                                                                  .class)) {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(), mf.getFullName() + " is annotated as @"
                        + Id.class.getSimpleName() + " and cannot be mixed with other annotations (like @Reference)"));
            }
        }
    }
}
