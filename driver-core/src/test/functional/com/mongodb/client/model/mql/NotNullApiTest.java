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

package com.mongodb.client.model.mql;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.client.model.mql.MqlValues.of;
import static com.mongodb.client.model.mql.MqlValues.ofArray;
import static com.mongodb.client.model.mql.MqlValues.ofMap;
import static com.mongodb.client.model.mql.MqlValues.ofNull;

class NotNullApiTest {

    @Test
    public void notNullApiTest() {
        Map<Class<?>, Object> mapping = new HashMap<>();
        Map<Class<?>, Object> paramMapping = new HashMap<>();

        // to test:
        mapping.put(MqlValues.class, null);
        mapping.put(MqlBoolean.class, of(true));
        mapping.put(MqlInteger.class, of(1));
        mapping.put(MqlNumber.class, of(1.0));
        mapping.put(MqlString.class, of(""));
        mapping.put(MqlDate.class, of(Instant.now()));
        mapping.put(MqlDocument.class, of(BsonDocument.parse("{}")));
        mapping.put(MqlMap.class, ofMap(BsonDocument.parse("{}")));
        mapping.put(MqlArray.class, ofArray());
        mapping.put(MqlValue.class, ofNull());
        mapping.put(Branches.class, new Branches<>());
        mapping.put(BranchesIntermediary.class, new BranchesIntermediary<>(Collections.emptyList()));
        mapping.put(BranchesTerminal.class, new BranchesTerminal<>(Collections.emptyList(), null));

        // additional params from classes not tested:
        paramMapping.put(String.class, "");
        paramMapping.put(Instant.class, Instant.now());
        paramMapping.put(Bson.class, BsonDocument.parse("{}"));
        paramMapping.put(Function.class, Function.identity());
        paramMapping.put(Number.class, 1);
        paramMapping.put(int.class, 1);
        paramMapping.put(boolean.class, true);
        paramMapping.put(long.class, 1L);
        paramMapping.put(Object.class, new Object());
        paramMapping.put(Decimal128.class, new Decimal128(1));
        putArray(paramMapping, MqlValue.class);
        putArray(paramMapping, boolean.class);
        putArray(paramMapping, long.class);
        putArray(paramMapping, int.class);
        putArray(paramMapping, double.class);
        putArray(paramMapping, Decimal128.class);
        putArray(paramMapping, Instant.class);
        putArray(paramMapping, String.class);

        checkNotNullApi(mapping, paramMapping);
    }

    private void putArray(final Map<Class<?>, Object> paramMapping, final Class<?> componentType) {
        final Object o = Array.newInstance(componentType, 0);
        paramMapping.put(o.getClass(), o);
    }

    private void checkNotNullApi(
            final Map<Class<?>, Object> mapping,
            final Map<Class<?>, Object> paramMapping) {
        Map<Class<?>, Object> allParams = new HashMap<>();
        allParams.putAll(mapping);
        allParams.putAll(paramMapping);
        List<String> uncheckedMethods = new ArrayList<>();
        for (Map.Entry<Class<?>, Object> entry : mapping.entrySet()) {
            Object instance = entry.getValue();
            Class<?> clazz = entry.getKey();
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                if (!Modifier.isPublic(method.getModifiers())) {
                    continue;
                }
                boolean failed = false;
                for (int i = 0; i < method.getParameterCount(); i++) {
                    if (method.getParameterTypes()[i].isPrimitive()) {
                        continue;
                    }
                    if (method.toString().endsWith(".equals(java.lang.Object)")) {
                        continue;
                    }
                    Object[] args = createArgs(allParams, method);
                    args[i] = null; // set one parameter to null
                    try {
                        // the method needs to throw due to Assertions.notNull:
                        method.invoke(instance, args);
                        failed = true;
                    } catch (Exception e) {
                        Throwable cause = e.getCause();
                        if (!(cause instanceof IllegalArgumentException)) {
                            failed = true;
                            continue;
                        }
                        StackTraceElement[] trace = cause.getStackTrace();
                        if (!method.getName().equals(trace[1].getMethodName())) {
                            failed = true;
                        }
                        if (!"notNull".equals(trace[0].getMethodName())) {
                            failed = true;
                        }
                    }
                }
                if (failed) {
                    uncheckedMethods.add("> " + method);
                }
            }
        }
        if (uncheckedMethods.size() > 0) {
            fail("Assertions.notNull must be called on parameter from "
                    + uncheckedMethods.size() + " methods:\n"
                    + String.join("\n", uncheckedMethods));
        }
    }

    private Object[] createArgs(final Map<Class<?>, ?> mapping, final Method method) {
        Object[] args = new Object[method.getParameterCount()];
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (int j = 0; j < parameterTypes.length; j++) {
            Class<?> p = parameterTypes[j];
            Object arg = mapping.get(p);
            if (arg == null) {
                throw new IllegalArgumentException("mappings did not contain parameter of type: "
                        + p + " for method " + method);
            }
            args[j] = arg;
        }
        return args;
    }

}
