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

package org.mongodb.connection;

public final class AsyncDetector {

    private static final boolean ASYNC_ENABLED;

    private AsyncDetector() {
    }

    static {
        if (System.getProperty("org.mongodb.disableAsync", "false").equals("true")) {
            ASYNC_ENABLED = false;
        }
        else {
            String javaSpecificationVersion = System.getProperty("java.specification.version");
            String minorVersionString = javaSpecificationVersion.substring(javaSpecificationVersion.lastIndexOf(".") + 1);
            boolean javaSevenOrGreater = false;
            try {
                javaSevenOrGreater = Integer.parseInt(minorVersionString) >= 7;
            } catch (NumberFormatException e) { // NOPMD
                // TODO: log this
            }
            ASYNC_ENABLED = javaSevenOrGreater;
        }
    }

    public static boolean isAsyncEnabled() {
        return ASYNC_ENABLED;
    }
}
