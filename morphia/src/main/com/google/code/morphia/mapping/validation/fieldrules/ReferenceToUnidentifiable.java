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

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ReferenceToUnidentifiable extends FieldConstraint {

    @Override
    protected void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.hasAnnotation(Reference.class)) {
            final Class realType = (mf.isSingleValue()) ? mf.getType() : mf.getSubClass();

            if (realType == null) {
                throw new MappingException("Type is null for this MappedField: " + mf);
            }

            if ((!realType.isInterface() && mc.getMapper().getMappedClass(realType).getIdField() == null)) {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                              mf.getFullName() + " is annotated as a @"
                                              + Reference.class.getSimpleName() + " but the " + mf.getType()
                                                                                                  .getName()
                                              + " class is missing the @" + Id.class.getSimpleName() + " annotation"));
            }
        }
    }

}
