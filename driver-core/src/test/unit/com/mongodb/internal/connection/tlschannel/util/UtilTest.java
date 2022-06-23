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
package com.mongodb.internal.connection.tlschannel.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class UtilTest {
    @Test
    void getJavaMajorVersion() {
        assertAll(
                () -> assertEquals(8, Util.getJavaMajorVersion("1.8.0_72-ea")),
                () -> assertEquals(9, Util.getJavaMajorVersion("9-ea")),
                () -> assertEquals(9, Util.getJavaMajorVersion("9")),
                () -> assertEquals(9, Util.getJavaMajorVersion("9.0.1")),
                () -> assertEquals(17, Util.getJavaMajorVersion("17")),
                () -> assertEquals(19, Util.getJavaMajorVersion("19-ea")),
                () -> assertEquals(42, Util.getJavaMajorVersion("42.1.0-ea"))
        );
    }

    private UtilTest() {
    }
}
