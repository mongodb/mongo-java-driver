package org.bson.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class PlatformUtilTest {

    @ParameterizedTest
    @ValueSource(strings = {"arm", "arm64", "aarch64", "ppc", "ppc64", "sparc", "mips"})
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
    @ValueSource(strings = {"x86", "amd64", "i386", "x86_64"})
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