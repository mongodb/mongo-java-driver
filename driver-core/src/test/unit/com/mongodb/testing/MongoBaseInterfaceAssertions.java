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

package com.mongodb.testing;

import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class MongoBaseInterfaceAssertions {

    private MongoBaseInterfaceAssertions() {
        //NOP
    }

    public static <T> void assertSubtypeReturn(final Class<T> baseClass) {
        Reflections reflections = new Reflections("com.mongodb");
        Set<Class<? extends T>> subtypes = reflections.getSubTypesOf(baseClass).stream()
                .filter(aClass -> Modifier.isPublic(aClass.getModifiers()))
                .filter(aClass -> !aClass.getPackage().getName().contains(".internal"))
                .collect(Collectors.toSet());

        Method[] baseMethods = baseClass.getDeclaredMethods();

        for (Class<? extends T> subtype : subtypes) {
            for (Method baseMethod : baseMethods) {
                Method method = assertDoesNotThrow(
                        () -> subtype.getDeclaredMethod(baseMethod.getName(), baseMethod.getParameterTypes()),
                        String.format(
                                "`%s` does not override `%s`. The methods must be copied into the implementing class/interface.",
                                subtype,
                                baseMethod
                        )
                );

                assertEquals(
                        subtype,
                        method.getReturnType(),
                        String.format(
                                "Method `%s` in `%s` does not return `%s`. "
                                        + "The return type must match the defining class/interface.",
                                method,
                                subtype,
                                subtype
                        )
                );
            }
        }
    }
}
