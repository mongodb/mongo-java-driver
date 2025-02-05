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

package com.mongodb.client;

import com.mongodb.internal.connection.FaasEnvironmentAccessor;
import com.mongodb.lang.Nullable;

import java.util.Map;

@FunctionalInterface
public interface WithWrapper {

    void run(Runnable r);

    static WithWrapper withWrapper() {
        return r -> r.run();
    }

    default WithWrapper withEnvironmentVariable(final String name, @Nullable final String value) {
        return runnable -> {
            Map<String, String> innerMap = getEnvInnerMap();
            String original = innerMap.get(name);
            if (value == null) {
                innerMap.remove(name);
            } else {
                innerMap.put(name, value);
            }
            try {
                this.run(runnable);
            } finally {
                if (original == null) {
                    innerMap.remove(name);
                } else {
                    innerMap.put(name, original);
                }
            }
        };
    }

    default WithWrapper withSystemProperty(final String name, final String value) {
        return runnable -> {
            String original = System.getProperty(name);
            System.setProperty(name, value);
            try {
                this.run(runnable);
            } finally {
                System.setProperty(name, original);
            }
        };
    }

    static Map<String, String> getEnvInnerMap() {
        return FaasEnvironmentAccessor.getFaasEnvMap();
    }
}
