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

package com.mongodb.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EnvironmentProviderTest {

    @Test
    void shouldDelegateGetEnvToSystemByDefault() {
        String path = System.getenv("PATH");
        assertEquals(path, EnvironmentProvider.getEnv("PATH"));
    }

    @Test
    void shouldInterceptGetEnvWhenOverridden() {
        try (EnvironmentProvider.EnvironmentOverride env = EnvironmentProvider.envOverride()) {
            env.set("MY_TEST_VAR", "hello");
            assertEquals("hello", EnvironmentProvider.getEnv("MY_TEST_VAR"));
        }

        assertNull(EnvironmentProvider.getEnv("MY_TEST_VAR"));
    }

    @Test
    void shouldAllowSettingNullOverride() {
        try (EnvironmentProvider.EnvironmentOverride env = EnvironmentProvider.envOverride()) {
            env.set("PATH", null);
            assertNull(EnvironmentProvider.getEnv("PATH"));
        }

        assertEquals(System.getenv("PATH"), EnvironmentProvider.getEnv("PATH"));
    }

    @Test
    void shouldChainOverridesCorrectly() {
        try (EnvironmentProvider.EnvironmentOverride outer = EnvironmentProvider.envOverride()) {
            outer.set("VAR_A", "outer");
            assertEquals("outer", EnvironmentProvider.getEnv("VAR_A"));

            try (EnvironmentProvider.EnvironmentOverride inner = EnvironmentProvider.envOverride()) {
                inner.set("VAR_B", "inner");
                assertEquals("outer", EnvironmentProvider.getEnv("VAR_A"));
                assertEquals("inner", EnvironmentProvider.getEnv("VAR_B"));
            }

            assertEquals("outer", EnvironmentProvider.getEnv("VAR_A"));
            assertNull(EnvironmentProvider.getEnv("VAR_B"));
        }
    }

    @Test
    void shouldReturnSelfForChaining() {
        try (EnvironmentProvider.EnvironmentOverride env = EnvironmentProvider.envOverride()) {
            env.set("A", "1").set("B", "2").set("C", "3");
            assertEquals("1", EnvironmentProvider.getEnv("A"));
            assertEquals("2", EnvironmentProvider.getEnv("B"));
            assertEquals("3", EnvironmentProvider.getEnv("C"));
        }
    }
}
