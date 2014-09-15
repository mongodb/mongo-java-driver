/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;

public class CursorFlagTest {
    @Test
    public void testToSet() {
        assertEquals(EnumSet.noneOf(CursorFlag.class), CursorFlag.toSet(0));
        assertEquals(EnumSet.of(CursorFlag.TAILABLE), CursorFlag.toSet(2));
        assertEquals(EnumSet.of(CursorFlag.TAILABLE, CursorFlag.SLAVE_OK), CursorFlag.toSet(6));
        assertEquals(EnumSet.allOf(CursorFlag.class), CursorFlag.toSet(0xFE));
    }

    @Test
    public void testFromSet() {
        assertEquals(0, CursorFlag.fromSet(EnumSet.noneOf(CursorFlag.class)));
        assertEquals(2, CursorFlag.fromSet(EnumSet.of(CursorFlag.TAILABLE)));
        assertEquals(6, CursorFlag.fromSet(EnumSet.of(CursorFlag.TAILABLE, CursorFlag.SLAVE_OK)));
        assertEquals(0xFE, CursorFlag.fromSet(EnumSet.allOf(CursorFlag.class)));
    }
}
