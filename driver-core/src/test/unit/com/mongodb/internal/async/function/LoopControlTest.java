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
package com.mongodb.internal.async.function;

import com.mongodb.client.syncadapter.SupplyingCallback;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LoopControlTest {
    @Test
    void iterationsAndAdvance() {
        LoopControl loopControl = new LoopControl();
        assertAll(
                () -> assertTrue(loopControl.isFirstIteration()),
                () -> assertEquals(0, loopControl.iteration()),
                () -> assertFalse(loopControl.isLastIteration()),
                () -> assertTrue(loopControl.advance()),
                () -> assertFalse(loopControl.isFirstIteration()),
                () -> assertEquals(1, loopControl.iteration()),
                () -> assertFalse(loopControl.isLastIteration())
        );
        loopControl.markAsLastIteration();
        assertAll(
                () -> assertFalse(loopControl.isFirstIteration()),
                () -> assertEquals(1, loopControl.iteration()),
                () -> assertTrue(loopControl.isLastIteration()),
                () -> assertFalse(loopControl.advance())
        );
    }

    @Test
    void markAsLastIteration() {
        LoopControl loopControl = new LoopControl();
        loopControl.markAsLastIteration();
        assertTrue(loopControl.isLastIteration());
        assertFalse(loopControl.advance());
    }

    @Test
    void breakAndCompleteIfFalse() {
        LoopControl loopControl = new LoopControl();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(loopControl.breakAndCompleteIf(() -> false, callback));
        assertFalse(callback.completed());
    }

    @Test
    void breakAndCompleteIfTrue() {
        LoopControl loopControl = new LoopControl();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(loopControl.breakAndCompleteIf(() -> true, callback));
        assertTrue(callback.completed());
    }

    @Test
    void breakAndCompleteIfPredicateThrows() {
        LoopControl loopControl = new LoopControl();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        RuntimeException e = new RuntimeException();
        assertTrue(loopControl.breakAndCompleteIf(() -> {
            throw e;
        }, callback));
        assertSame(e, assertThrows(e.getClass(), callback::get));
    }
}
