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
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class LoopStateTest {
    @Test
    void iterationsAndAdvance() {
        LoopState loopState = new LoopState();
        assertAll(
                () -> assertTrue(loopState.firstIteration()),
                () -> assertEquals(0, loopState.iteration()),
                () -> assertFalse(loopState.lastIteration()),
                () -> assertTrue(loopState.advance()),
                () -> assertFalse(loopState.firstIteration()),
                () -> assertEquals(1, loopState.iteration()),
                () -> assertFalse(loopState.lastIteration())
        );
        loopState.markAsLastIteration();
        assertAll(
                () -> assertFalse(loopState.firstIteration()),
                () -> assertEquals(1, loopState.iteration()),
                () -> assertTrue(loopState.lastIteration()),
                () -> assertFalse(loopState.advance())
        );
    }

    @Test
    void maskAsLastIteration() {
        LoopState loopState = new LoopState();
        loopState.markAsLastIteration();
        assertTrue(loopState.lastIteration());
        assertFalse(loopState.advance());
    }

    @Test
    void breakAndCompleteIfFalse() {
        LoopState loopState = new LoopState();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(loopState.breakAndCompleteIf(() -> false, callback));
        assertFalse(callback.completed());
    }

    @Test
    void breakAndCompleteIfTrue() {
        LoopState loopState = new LoopState();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(loopState.breakAndCompleteIf(() -> true, callback));
        assertTrue(callback.completed());
    }

    @Test
    void breakAndCompleteIfPredicateThrows() {
        LoopState loopState = new LoopState();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        RuntimeException e = new RuntimeException() {
        };
        assertTrue(loopState.breakAndCompleteIf(() -> {
            throw e;
        }, callback));
        assertThrows(e.getClass(), callback::get);
    }

    @Test
    void attachAndAttachment() {
        LoopState loopState = new LoopState();
        AttachmentKey<Integer> attachmentKey = AttachmentKeys.maxWireVersion();
        int attachmentValue = 1;
        assertFalse(loopState.attachment(attachmentKey).isPresent());
        loopState.attach(attachmentKey, attachmentValue, false);
        assertEquals(attachmentValue, loopState.attachment(attachmentKey).get());
        loopState.advance();
        assertEquals(attachmentValue, loopState.attachment(attachmentKey).get());
        loopState.attach(attachmentKey, attachmentValue, true);
        assertEquals(attachmentValue, loopState.attachment(attachmentKey).get());
        loopState.advance();
        assertFalse(loopState.attachment(attachmentKey).isPresent());
    }
}
