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

import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.mapping.DefaultCreator;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class VersionMisuse extends FieldConstraint {

    @Override
    protected void check(final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.hasAnnotation(Version.class)) {
            final Class<?> type = mf.getType();
            if (Long.class.equals(type) || long.class.equals(type)) {

                //TODO: Replace this will a read ObjectFactory call -- requires Mapper instance.
                final Object testInstance = DefaultCreator.createInst(mc.getClazz());

                // check initial value
                if (Long.class.equals(type)) {
                    if (mf.getFieldValue(testInstance) != null) {
                        ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                                      "When using @" + Version.class.getSimpleName()
                                                      + " on a Long field, it must be initialized to null."));
                    }
                }
                else if (long.class.equals(type)) {
                    if ((Long) mf.getFieldValue(testInstance) != 0L) {
                        ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                                      "When using @" + Version.class.getSimpleName()
                                                      + " on a long field, it must be initialized to 0."));
                    }
                }
            }
            else {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mf, this.getClass(),
                                              "@" + Version.class.getSimpleName()
                                              + " can only be used on a Long/long field."));
            }
        }
    }

}
