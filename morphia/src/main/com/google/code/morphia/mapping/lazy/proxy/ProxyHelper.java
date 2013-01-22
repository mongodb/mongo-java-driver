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

/**
 *
 */
package com.google.code.morphia.mapping.lazy.proxy;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ProxyHelper {

    public static <T> T unwrap(final T entity) {
        if (isProxy(entity)) {
            return (T) asProxy(entity).__unwrap();
        }
        return entity;
    }

    private static <T> ProxiedReference asProxy(final T entity) {
        return ((ProxiedReference) entity);
    }

    public static boolean isProxy(final Object entity) {
        return (entity != null && isProxied(entity.getClass()));
    }

    public static boolean isProxied(final Class<?> clazz) {
        return ProxiedReference.class.isAssignableFrom(clazz);
    }

    public static Class getReferentClass(final Object entity) {
        if (isProxy(entity)) {
            return asProxy(entity).__getReferenceObjClass();
        }
        else {
            return entity != null ? entity.getClass() : null;
        }
    }

    public static boolean isFetched(final Object entity) {
        return entity == null || !isProxy(entity) || asProxy(entity).__isFetched();
    }

    public static boolean isUnFetched(final Object entity) {
        return !isFetched(entity);
    }
}
