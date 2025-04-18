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

package org.bson.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class PlatformUtilTest {

    @ParameterizedTest
    @ValueSource(strings = {"arm", "ppc", "ppc64", "sparc", "mips"})
    @DisplayName("Should not allow unaligned access for unsupported architectures")
    void shouldNotAllowUnalignedAccessForUnsupportedArchitecture(final String architecture) {
        withSystemProperty("os.arch", architecture, () -> {
            boolean result = PlatformUtil.isUnalignedAccessAllowed();
            assertFalse(result);
        });
    }

    @Test
    @DisplayName("Should not allow unaligned access when system property is undefined")
    void shouldNotAllowUnalignedAccessWhenSystemPropertyIsUndefined() {
        withSystemProperty("os.arch", null, () -> {
            boolean result = PlatformUtil.isUnalignedAccessAllowed();
            assertFalse(result);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"x86", "amd64", "i386", "x86_64", "arm64", "aarch64"})
    @DisplayName("Should allow unaligned access for supported architectures")
    void shouldAllowUnalignedAccess(final String architecture) {
        withSystemProperty("os.arch", architecture, () -> {
            boolean result = PlatformUtil.isUnalignedAccessAllowed();
            assertTrue(result);
        });
    }

    public static void withSystemProperty(final String name, final String value, final Runnable testCode) {
        String original = System.getProperty(name);
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
        try {
            testCode.run();
        } finally {
            System.setProperty(name, original);
        }
    }
}
