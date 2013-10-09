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

package org.mongodb.codecs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.lang.reflect.Modifier.isTransient;

public class ClassModel<T> {
    private static final Pattern FIELD_NAME_REGEX_PATTERN = Pattern.compile("([a-zA-Z_][\\w$]*)");

    private final Class<T> theClass;

    //Two collections to track the fields for performance reasons
    private final List<Field> validatedFields = new ArrayList<Field>();
    private final Map<String, Field> validatedFieldsByName = new HashMap<String, Field>();

    public ClassModel(final Class<T> theClass) {
        this.theClass = theClass;

        for (final Field field : theClass.getDeclaredFields()) {
            String fieldName = field.getName();
            if (isValidFieldName(fieldName) && !isTransient(field.getModifiers())) {
                this.validatedFields.add(field);
                this.validatedFieldsByName.put(fieldName, field);
            }
        }
    }

    public T createInstanceOfClass() throws IllegalAccessException {
        try {
            return theClass.newInstance();
        } catch (InstantiationException e) {
            throw new DecodingException(String.format("Can't create an instance of %s", theClass), e);
        }
    }

    public Collection<Field> getFields() {
        //returning validatedFieldsByName.values is half as fast as simply returning this list
        return validatedFields;
    }

    public Field getDeclaredField(final String fieldName) throws NoSuchFieldException {
        Field field = validatedFieldsByName.get(fieldName);
        if (field == null) {
            throw new NoSuchFieldException(String.format("Field %s not found on class %s", fieldName, theClass));
        }
        return field;
    }

    private boolean isValidFieldName(final String fieldName) {
        //We need to document that fields starting with a $ will be ignored
        //and we probably need to be able to either disable this validation or make it pluggable
        return FIELD_NAME_REGEX_PATTERN.matcher(fieldName).matches();
    }

    @Override
    public String toString() {
        return "ClassModel{"
               + "theClass=" + theClass
               + ", validatedFields=" + validatedFields
               + '}';
    }
}
