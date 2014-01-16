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

package org.bson.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringRangeSetTest extends com.mongodb.util.TestCase {

    @Test
    public void rangeOfSizeProducesCorrectIteration() {
        int len = 250;
        StringRangeSet set = new StringRangeSet(len);
        int i = 0;
        for (String num : set) {
            assertEquals(num, String.valueOf(i++));
        }
        assertEquals(i, 250);
    }

    @Test
    public void testToArray() {
        int len = 1000;
        StringRangeSet set = new StringRangeSet(len);
        String[] array = (String[]) set.toArray();
        for (int i = 0; i < len; ++i)
            assertEquals(array[i], String.valueOf(i));
    }
}
