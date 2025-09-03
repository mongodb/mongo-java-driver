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

package org.bson.json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonStringBufferTest {

    @Test
    public void testRead() {
        JsonBuffer buffer = new JsonStringBuffer("ABC");
        assertEquals('A', buffer.read());
        assertEquals('B', buffer.read());
        assertEquals('C', buffer.read());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testUnRead() {
        JsonStringBuffer buffer = new JsonStringBuffer("A");
        buffer.unread(buffer.read());
        assertEquals('A', buffer.read());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testPosition() {
        JsonStringBuffer buffer = new JsonStringBuffer("ABC");

        buffer.read();
        buffer.read();
        assertEquals(2, buffer.getPosition());
    }

    @Test
    public void testEOFCheck() {
        JsonStringBuffer buffer = new JsonStringBuffer("");

        buffer.read();
        assertThrows(JsonParseException.class, () -> buffer.read());
    }
}
