/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;

public class QueryOptionTest {
    @Test
    public void testToSet() {
        assertEquals(EnumSet.noneOf(QueryOption.class), QueryOption.toSet(0));
        assertEquals(EnumSet.of(QueryOption.Tailable), QueryOption.toSet(2));
        assertEquals(EnumSet.of(QueryOption.Tailable, QueryOption.SlaveOk), QueryOption.toSet(6));
        assertEquals(EnumSet.allOf(QueryOption.class), QueryOption.toSet(0xFE));
    }

    @Test
    public void testFromSet() {
        assertEquals(0, QueryOption.fromSet(EnumSet.noneOf(QueryOption.class)));
        assertEquals(2, QueryOption.fromSet(EnumSet.of(QueryOption.Tailable)));
        assertEquals(6, QueryOption.fromSet(EnumSet.of(QueryOption.Tailable, QueryOption.SlaveOk)));
        assertEquals(0xFE, QueryOption.fromSet(EnumSet.allOf(QueryOption.class)));
    }
}
