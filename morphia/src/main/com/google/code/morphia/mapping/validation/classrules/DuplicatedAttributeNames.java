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

package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.HashSet;
import java.util.Set;

/**
 * @author josephpachod
 */
public class DuplicatedAttributeNames implements ClassConstraint {

    public void check(final MappedClass mc, final Set<ConstraintViolation> ve) {
        final Set<String> foundNames = new HashSet<String>();
        final Set<String> duplicates = new HashSet<String>();
        for (final MappedField mappedField : mc.getPersistenceFields()) {
            for (final String name : mappedField.getLoadNames()) {
                // if (duplicates.contains(name)) {
                // continue;
                // }
                if (!foundNames.add(name)) {
                    ve.add(new ConstraintViolation(Level.FATAL, mc, mappedField, this.getClass(),
                                                  "Mapping to MongoDB field name '" + name
                                                  + "' is duplicated; you cannot map different " +
                                                  "Java fields to the same MongoDB field."));
                    duplicates.add(name);
                }
            }
        }
    }
}
