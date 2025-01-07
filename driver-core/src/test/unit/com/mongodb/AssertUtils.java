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

package com.mongodb;

import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertUtils {

    public static <T> void assertSubInterfaceReturnTypes(final String packageName,
                                                         final Class<T> baseClass) {
        Reflections reflections = new Reflections(packageName);
        Set<Class<? extends T>> subInterfaces = reflections.getSubTypesOf(baseClass).stream()
                .filter(Class::isInterface)
                .collect(Collectors.toSet());

        Method[] baseMethods = baseClass.getDeclaredMethods();

        for (Class<? extends T> subInterface : subInterfaces) {
            for (Method baseMethod : baseMethods) {
                Method method = assertDoesNotThrow(
                        () -> subInterface.getDeclaredMethod(baseMethod.getName(), baseMethod.getParameterTypes()),
                        String.format(
                                "%s does not override %s. The methods must be copied into the implementing interface.",
                                subInterface.getName(),
                                baseMethod.getName()
                        )
                );

                assertEquals(
                        subInterface,
                        method.getReturnType(),
                        String.format(
                                "Method %s in %s does not return %s. "
                                        + "The return type must match the defining class.",
                                method.getName(),
                                subInterface.getName(),
                                subInterface.getName()
                        )
                );
            }
        }
    }
}
