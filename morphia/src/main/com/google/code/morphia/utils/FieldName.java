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

package com.google.code.morphia.utils;

import java.lang.reflect.Field;

/**
 * Handy class to test if a certain Fieldname is available in a class. Usage: If you add <code> public static final
 * String _foo = FieldName.of("foo"); </code>
 * <p/>
 * you´ll see an Exception on loading the class. A nice side-effect: if you use this in Queries like <code>
 * q.field(MyEntity._foo).equal("bar") </code>
 * <p/>
 * your IDE is able to track this usage. Using FieldName does not at all replace query validation.
 *
 * @author us@thomas-daily.de
 */
public final class FieldName {
    private FieldName() {
    }

    public static String of(final String name) {
        return of(callingClass(), name);
    }

    public static String of(final Class<?> clazz, final String name) {
        Assert.parameterNotNull(clazz, "clazz");
        Assert.parameterNotNull(name, "name");
        if (hasField(clazz, name)) {
            return name;
        }
        throw new FieldNameNotFoundException("Field called '" + name + "' on class '" + clazz
                                             + "' was not found.");
    }

    private static boolean hasField(final Class<?> clazz, final String name) {
        final Field[] fa = ReflectionUtils.getDeclaredAndInheritedFields(clazz, true);
        for (final Field field : fa) {
            if (name.equals(field.getName())) {
                return true;
            }
        }
        return false;
    }

    private static Class<?> callingClass() {
        return callingClass(FieldName.class);
    }

    private static Class<?> callingClass(final Class<?>... classesToExclude) {
        final StackTraceElement[] stackTrace = new Exception().getStackTrace();
        for (final StackTraceElement e : stackTrace) {
            final String c = e.getClassName();

            boolean exclude = false;
            for (final Class<?> ec : classesToExclude) {
                exclude |= c.equals(ec.getName());
            }
            if (!exclude) {
                return forName(c);
            }
        }
        throw new java.lang.IllegalStateException();

    }

    private static Class<?> forName(final String c) {
        try {
            return Class.forName(c);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error when getting class for name '" + c + "'");
        }
    }

    public static class FieldNameNotFoundException extends RuntimeException {
        private static final long serialVersionUID = 52124742120586573L;

        public FieldNameNotFoundException(final String msg) {
            super(msg);
        }
    }

}
