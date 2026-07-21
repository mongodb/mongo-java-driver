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
 *
 */

package com.mongodb.internal.crypt.capi;

import com.mongodb.internal.crypt.capi.CAPI.cstring;
import com.sun.jna.Pointer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CstringTest {

    // A native function declared to return a cstring (e.g. mongocrypt_status_message) may return a
    // NULL pointer. By default JNA would marshal that to a null cstring reference, causing callers
    // that immediately call toString() to throw a NullPointerException. The fromNative override must
    // return a non-null cstring instead.
    @Test
    void fromNativeReturnsEmptyStringBackedInstanceForNullPointer() {
        cstring result = (cstring) new cstring().fromNative(null, null);

        assertNotNull(result);
        assertEquals("", result.toString());
    }

    @Test
    void fromNativeWrapsNonNullPointer() {
        cstring source = new cstring("hello world");

        cstring result = (cstring) new cstring().fromNative(source.getPointer(), null);

        assertNotNull(result);
        assertEquals("hello world", result.toString());
    }

    @Test
    void toStringReturnsEmptyStringForNullPointer() {
        assertEquals("", new cstring().toString());
    }

    @Test
    void toStringReturnsEmptyStringForExplicitNullPointer() {
        cstring value = new cstring();
        value.setPointer(Pointer.NULL);

        assertEquals("", value.toString());
    }

    @Test
    void toStringReadsBackConstructedValue() {
        assertEquals("a value", new cstring("a value").toString());
    }
}
