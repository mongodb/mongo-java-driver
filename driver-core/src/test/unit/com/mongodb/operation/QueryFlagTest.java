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

package com.mongodb.operation;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;

public class QueryFlagTest {
    @Test
    public void testToSet() {
        assertEquals(EnumSet.noneOf(QueryFlag.class), QueryFlag.toSet(0));
        assertEquals(EnumSet.of(QueryFlag.Tailable), QueryFlag.toSet(2));
        assertEquals(EnumSet.of(QueryFlag.Tailable, QueryFlag.SlaveOk), QueryFlag.toSet(6));
        assertEquals(EnumSet.allOf(QueryFlag.class), QueryFlag.toSet(0xFE));
    }

    @Test
    public void testFromSet() {
        assertEquals(0, QueryFlag.fromSet(EnumSet.noneOf(QueryFlag.class)));
        assertEquals(2, QueryFlag.fromSet(EnumSet.of(QueryFlag.Tailable)));
        assertEquals(6, QueryFlag.fromSet(EnumSet.of(QueryFlag.Tailable, QueryFlag.SlaveOk)));
        assertEquals(0xFE, QueryFlag.fromSet(EnumSet.allOf(QueryFlag.class)));
    }
}
