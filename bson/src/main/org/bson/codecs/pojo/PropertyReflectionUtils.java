/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs.pojo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.reflect.Modifier.isPublic;

final class PropertyReflectionUtils {
    private PropertyReflectionUtils() {}

    private static final String IS_PREFIX = "is";
    private static final String GET_PREFIX = "get";
    private static final String SET_PREFIX = "set";

    static boolean isGetter(final Method method) {
        if (method.getParameterTypes().length > 0) {
            return false;
        } else if (method.getName().startsWith(GET_PREFIX) && method.getName().length() > GET_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(GET_PREFIX.length()));
        } else if (method.getName().startsWith(IS_PREFIX) && method.getName().length() > IS_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(IS_PREFIX.length()));
        }
        return false;
    }

    static boolean isSetter(final Method method) {
        if (method.getName().startsWith(SET_PREFIX) && method.getName().length() > SET_PREFIX.length()
                && method.getParameterTypes().length == 1) {
            return Character.isUpperCase(method.getName().charAt(SET_PREFIX.length()));
        }
        return false;
    }

    static String toPropertyName(final Method method) {
        String name = method.getName();
        String propertyName = name.substring(name.startsWith(IS_PREFIX) ? 2 : 3, name.length());
        char[] chars = propertyName.toCharArray();
        chars[0] = Character.toLowerCase(chars[0]);
        return new String(chars);
    }

    static PropertyMethods getPropertyMethods(final Class<?> clazz) {
        List<Method> setters = new ArrayList<Method>();
        List<Method> getters = new ArrayList<Method>();

        // get all the default method from interface
        for (Class<?> i : clazz.getInterfaces()) {
            for (Method method : i.getDeclaredMethods()) {
                if (method.isDefault()) {
                    verifyAddMethodToList(method, getters, setters);
                }
            }
        }

        for (Method method : clazz.getDeclaredMethods()) {
            verifyAddMethodToList(method, getters, setters);
        }

        return new PropertyMethods(getters, setters);
    }

    private static void verifyAddMethodToList(final Method method, final List<Method> getters, final List<Method> setters) {
        // Note that if you override a getter to provide a more specific return type, getting the declared methods
        // on the subclass will return the overridden method as well as the method that was overridden from
        // the super class. This original method is copied over into the subclass as a bridge method, so we're
        // excluding them here to avoid multiple getters of the same property with different return types
        if (isPublic(method.getModifiers()) && !method.isBridge()) {
            if (isGetter(method)) {
                getters.add(method);
            } else if (isSetter(method)) {
                // Setters are a bit more tricky - don't do anything fancy here
                setters.add(method);
            }
        }
    }

    static class PropertyMethods {
        private final Collection<Method> getterMethods;
        private final Collection<Method> setterMethods;

        PropertyMethods(final Collection<Method> getterMethods, final Collection<Method> setterMethods) {
            this.getterMethods = getterMethods;
            this.setterMethods = setterMethods;
        }

        Collection<Method> getGetterMethods() {
            return getterMethods;
        }

        Collection<Method> getSetterMethods() {
            return setterMethods;
        }
    }
}
