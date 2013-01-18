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

package com.google.code.morphia.mapping.validation;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConstraintViolation {
    public enum Level {
        MINOR, INFO, WARNING, SEVERE, FATAL
    }

    private final MappedClass clazz;
    private MappedField field = null;
    private final Class<? extends ClassConstraint> validator;
    private final String message;
    private final Level level;

    public ConstraintViolation(final Level level, final MappedClass clazz, final MappedField field,
                               final Class<? extends ClassConstraint> validator, final String message) {
        this(level, clazz, validator, message);
        this.field = field;
    }

    public ConstraintViolation(final Level level, final MappedClass clazz, final Class<? extends ClassConstraint>
            validator, final String message) {
        this.level = level;
        this.clazz = clazz;
        this.message = message;
        this.validator = validator;
    }

    public String render() {
        return String.format("%s complained about %s : %s",
                             validator.getSimpleName(),
                             getPrefix(),
                             message);
    }

    public Level getLevel() {
        return level;
    }

    public String getPrefix() {
        final String fn = (field != null) ? field.getJavaFieldName() : "";
        return clazz.getClazz().getName() + "." + fn;
    }
}