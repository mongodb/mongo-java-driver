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
package com.mongodb.internal.time;

/**
 * Avoid using this class directly and prefer using other program elements from {@link com.mongodb.internal.time}, if possible.
 * <p>
 * We do not use {@link System#nanoTime()} directly in the rest of the {@link com.mongodb.internal.time} package,
 * and use {@link SystemNanoTime#get()} instead because we need to tamper with it via {@code Mockito.mockStatic},
 * and mocking methods of {@link System} class is both impossible and unwise.
 */
public final class SystemNanoTime {
    private SystemNanoTime() {
    }

    public static long get() {
        return System.nanoTime();
    }
}
