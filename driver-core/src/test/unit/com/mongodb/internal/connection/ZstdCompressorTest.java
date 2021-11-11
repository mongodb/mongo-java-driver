package com.mongodb.internal.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.util.Native;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;

final class ZstdCompressorTest {
    @Test
    void zstdClassInitializer() throws NoSuchFieldException, IllegalAccessException {
        Field loaded = Native.class.getDeclaredField("loaded");
        loaded.setAccessible(true);
        assertFalse((Boolean) loaded.get(null));

        @SuppressWarnings("unused") ZstdCompressor zstdCompressor = new ZstdCompressor();
        assertTrue((Boolean) loaded.get(null));
    }
}
