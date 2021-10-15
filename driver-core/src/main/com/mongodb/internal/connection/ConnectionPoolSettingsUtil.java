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
package com.mongodb.internal.connection;

import com.mongodb.MongoInterruptedException;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ConnectionPoolSettings.Builder;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * This class contains methods for accessing non-{@code public} methods of {@link ConnectionPoolSettings},
 * {@link ConnectionPoolSettings.Builder}, and related utility methods.
 */
public final class ConnectionPoolSettingsUtil {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    @Nullable
    private static final Method PRESTART_ASYNC_WORK_MANAGER = getDeclaredMethod(Builder.class,
            "prestartAsyncWorkManager", boolean.class);
    @Nullable
    private static final Method IS_PRESTART_ASYNC_WORK_MANAGER = getDeclaredMethod(ConnectionPoolSettings.class,
            "isPrestartAsyncWorkManager");

    /**
     * See {@code ConnectionPoolSettings.Builder.prestartAsyncWorkManager(boolean)}.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void prestartAsyncWorkManager(final Builder builder, final boolean prestart) {
        try {
            assertNotNull(PRESTART_ASYNC_WORK_MANAGER).invoke(builder, prestart);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public static void sleepWhilePrestartingAsyncWorkManager() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException(null, e);
        }
    }

    /**
     * See {@code ConnectionPoolSettings.isPrestartAsyncWorkManager()}.
     */
    public static boolean isPrestartAsyncWorkManager(final ConnectionPoolSettings settings) {
        try {
            return IS_PRESTART_ASYNC_WORK_MANAGER != null && (boolean) IS_PRESTART_ASYNC_WORK_MANAGER.invoke(settings);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.debug(null, e);
            return false;
        }
    }

    @Nullable
    private static Method getDeclaredMethod(final Class<?> klass, final String name, final Class<?>... parameterTypes) {
        try {
            Method result = klass.getDeclaredMethod(name, parameterTypes);
            result.setAccessible(true);
            return result;
        } catch (NoSuchMethodException e) {
            LOGGER.debug(null, e);
            return null;
        }
    }

    private ConnectionPoolSettingsUtil() {
        fail();
    }
}
