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

import com.mongodb.connection.TopologyVersion;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TopologyVersionHelperTest {
    @Test
    void compare() {
        int objectIdCounterExclusiveUpperBound = 0xff_ff_ff;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        ObjectId processIdA = new ObjectId(rnd.nextInt(), rnd.nextInt(objectIdCounterExclusiveUpperBound));
        ObjectId processIdB = new ObjectId(rnd.nextInt(), rnd.nextInt(objectIdCounterExclusiveUpperBound));
        assertNotEquals(processIdA, processIdB);
        TopologyVersion a1 = new TopologyVersion(processIdA, 1);
        TopologyVersion a2 = new TopologyVersion(processIdA, 2);
        TopologyVersion b1 = new TopologyVersion(processIdB, 1);
        assertAll(
                () -> assertFalse(TopologyVersionHelper.newer(null, null)),
                () -> assertFalse(TopologyVersionHelper.newer(null, a1)),
                () -> assertFalse(TopologyVersionHelper.newer(a1, null)),
                () -> assertFalse(TopologyVersionHelper.newer(a1, b1)),
                () -> assertFalse(TopologyVersionHelper.newer(b1, a1)),
                () -> assertFalse(TopologyVersionHelper.newer(a1, a2)),
                () -> assertFalse(TopologyVersionHelper.newer(a1, a1)),
                () -> assertTrue(TopologyVersionHelper.newer(a2, a1)));
        assertAll(
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(null, null)),
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(null, a1)),
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(a1, null)),
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(a1, b1)),
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(b1, a1)),
                () -> assertFalse(TopologyVersionHelper.newerOrEqual(a1, a2)),
                () -> assertTrue(TopologyVersionHelper.newerOrEqual(a1, a1)),
                () -> assertTrue(TopologyVersionHelper.newerOrEqual(a2, a1)));
    }

    private TopologyVersionHelperTest() {
    }
}
