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

package com.mongodb.internal.connection;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ChangeEventTest {
    @Test
    public void testAll() {
        ChangeEvent<Integer> event = new ChangeEvent<Integer>(1, 2);
        assertEquals(Integer.valueOf(1), event.getPreviousValue());
        assertEquals(Integer.valueOf(2), event.getNewValue());

        assertTrue(event.toString().startsWith("ChangeEvent"));

        assertEquals(event, event);
        assertEquals(new ChangeEvent<Integer>(1, 2), event);
        assertThat(new ChangeEvent<Integer>(2, 3), not(event));
        assertEquals(33, event.hashCode());
    }
}
