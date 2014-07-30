/*
 * Copyright (c) 2008-2014 Atlassian Pty Ltd
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

package com.mongodb.assertions;

/**
 * Design by contract assertions.
 */
public final class Assertions {
    public static <T> T notNull(final String name, final T notNull) {
        if (notNull == null) {
            throw new IllegalArgumentException(name + " can not be null");
        }
        return notNull;
    }

    public static void isTrue(final String name, final boolean check) {
        if (!check) {
            throw new IllegalStateException("state should be: " + name);
        }
    }

    public static void isTrueArgument(final String name, final boolean check) {
        if (!check) {
            throw new IllegalArgumentException("state should be: " + name);
        }
    }

    private Assertions() {
    }
}
