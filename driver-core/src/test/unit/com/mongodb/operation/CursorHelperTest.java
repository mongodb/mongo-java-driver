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

package com.mongodb.operation;

import org.junit.Test;

import java.net.UnknownHostException;

import static com.mongodb.operation.CursorHelper.getNumberToReturn;
import static org.junit.Assert.assertEquals;

public class CursorHelperTest {

    @Test
    public void testNumberToReturn() throws UnknownHostException {
        assertEquals(0, getNumberToReturn(0, 0, 5));
        assertEquals(40, getNumberToReturn(0, 40, 5));
        assertEquals(-40, getNumberToReturn(0, -40, 5));
        assertEquals(15, getNumberToReturn(20, 0, 5));
        assertEquals(10, getNumberToReturn(20, 10, 5));
        assertEquals(15, getNumberToReturn(20, -40, 5));
    }
}
