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

package org.bson;

import com.mongodb.BasicDBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BSONTest {
    @Test
    public void testEncodingDecode() {
        BasicDBObject inputDoc = new BasicDBObject("_id", 1);
        byte[] encoded = BSON.encode(inputDoc);
        assertEquals(inputDoc, BSON.decode(encoded));
    }

    @Test
    public void testToInt() {
        assertEquals(1, BSON.toInt(Boolean.TRUE));
        assertEquals(0, BSON.toInt(Boolean.FALSE));
        assertEquals(12, BSON.toInt(12.23f));
        assertEquals(21, BSON.toInt(21.32d));
        assertEquals(13, BSON.toInt(13));
    }
}
