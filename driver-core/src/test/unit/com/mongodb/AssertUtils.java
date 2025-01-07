package com.mongodb;

import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertUtils {

    public static <T> void assertSubInterfaceReturnTypes(String packageName, Class<T> baseClass) {
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