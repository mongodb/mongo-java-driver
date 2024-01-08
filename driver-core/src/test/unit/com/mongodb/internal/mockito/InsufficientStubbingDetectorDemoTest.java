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
package com.mongodb.internal.mockito;

import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.operation.ListCollectionsOperation;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

final class InsufficientStubbingDetectorDemoTest {
    private static final Logger LOGGER = Loggers.getLogger(InsufficientStubbingDetectorDemoTest.class.getSimpleName());

    private ListCollectionsOperation<BsonDocument> operation;

    @BeforeEach
    void beforeEach() {
        operation = new ListCollectionsOperation<>(TimeoutSettings.DEFAULT, "db", new BsonDocumentCodec());
    }

    @Test
    void mockObjectWithDefaultAnswer() {
        ReadBinding binding = Mockito.mock(ReadBinding.class);
        LOGGER.info("", assertThrows(NullPointerException.class, () -> operation.execute(binding)));
    }

    @Test
    void mockObjectWithThrowsException() {
        ReadBinding binding = Mockito.mock(ReadBinding.class,
                new ThrowsException(new AssertionError("Insufficient stubbing for " + ReadBinding.class)));
        LOGGER.info("", assertThrows(AssertionError.class, () -> operation.execute(binding)));
    }

    @Test
    void mockObjectWithInsufficientStubbingDetector() {
        ReadBinding binding = MongoMockito.mock(ReadBinding.class);
        LOGGER.info("", assertThrows(AssertionError.class, () -> operation.execute(binding)));
    }

    @Test
    void stubbingWithThrowsException() {
        ReadBinding binding = Mockito.mock(ReadBinding.class,
                new ThrowsException(new AssertionError("Unfortunately, you cannot do stubbing")));
        assertThrows(AssertionError.class, () -> when(binding.getOperationContext()).thenReturn(OPERATION_CONTEXT));
    }

    @Test
    void stubbingWithInsufficientStubbingDetector() {
        MongoMockito.mock(ReadBinding.class, bindingMock ->
                when(bindingMock.getOperationContext()).thenReturn(OPERATION_CONTEXT)
        );
    }
}
