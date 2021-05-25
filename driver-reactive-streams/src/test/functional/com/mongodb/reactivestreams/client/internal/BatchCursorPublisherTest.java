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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.operation.AsyncReadOperation;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.mongodb.reactivestreams.client.internal.TestHelper.OPERATION_EXECUTOR;
import static com.mongodb.reactivestreams.client.internal.TestHelper.OPERATION_PUBLISHER;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class BatchCursorPublisherTest {

    private static final String ERROR_CREATING_CURSOR = "Error creating cursor";
    private static final String ERROR_RETURNING_RESULTS = "Error returning results";

    @Mock
    private AsyncReadOperation<AsyncBatchCursor<Document>> readOperation;
    @Mock
    private AsyncBatchCursor<Document> batchCursor;

    @Test
    public void testBatchCursor() {
        List<Document> documents = IntStream.range(1, 20).boxed().map(i -> Document.parse(format("{_id: %s}", i))).collect(toList());

        StepVerifier.create(createVerifiableBatchCursor(documents))
                .expectNext(documents.toArray(new Document[0]))
                .expectComplete()
                .verify(Duration.ofMillis(1000));
    }

    @Test
    public void testBatchCursorRespectsBatchSize() {
        List<Document> documents = IntStream.range(1, 11).boxed().map(i -> Document.parse(format("{_id: %s}", i))).collect(toList());

        StepVerifier.Step<Document> verifier = StepVerifier.create(createVerifiableBatchCursor(documents, 2));
        createBatches(documents, 2).forEach(b -> verifier.expectNext(b.toArray(new Document[0])));
        verifier.expectComplete()
                .verify();

        StepVerifier.create(createVerifiableBatchCursor(documents, 2), 1)
                .expectNext(documents.get(0))
                .thenRequest(8)
                .expectNextCount(8)
                .thenRequest(1)
                .expectNext(documents.get(documents.size() - 1))
                .expectComplete()
                .verify();
    }

    @Test
    public void testBatchCursorFirst() {
        List<Document> documents = IntStream.range(1, 11).boxed().map(i -> Document.parse(format("{_id: %s}", i))).collect(toList());
        StepVerifier.create(createVerifiableBatchCursor(documents).first())
                .expectNext(documents.get(0))
                .expectComplete()
                .verify();
    }

    @Test
    public void testBatchCursorFirstEmpty() {
        StepVerifier.create(createVerifiableBatchCursor(emptyList()).first())
                .expectComplete()
                .verify();
    }

    @Test
    public void testBatchCursorError() {
        StepVerifier.create(createVerifiableBatchCursorError())
                .expectErrorMessage(ERROR_CREATING_CURSOR)
                .verify();
    }

    @Test
    public void testBatchCursorOnNextError() {
        List<Document> documents = IntStream.range(1, 11).boxed().map(i -> Document.parse(format("{_id: %s}", i))).collect(toList());

        StepVerifier.create(createVerifiableBatchCursorError(documents))
                .expectNext(documents.toArray(new Document[0]))
                .expectErrorMessage(ERROR_RETURNING_RESULTS)
                .verify();

    }

    @Test
    public void testCancellingSubscriptionBatchCursor() {
        List<Document> documents = IntStream.range(1, 11).boxed().map(i -> Document.parse(format("{_id: %s}", i))).collect(toList());

        StepVerifier.create(createVerifiableBatchCursor(documents, 2), 1)
                .expectNext(documents.get(0))
                .thenRequest(2)
                .expectNext(documents.get(1), documents.get(2))
                .thenCancel()
                .verifyThenAssertThat()
                .hasDiscarded(documents.get(3));
    }

    BatchCursorPublisher<Document> createVerifiableBatchCursor(final List<Document> expected) {
        return createVerifiableBatchCursor(expected, 0);
    }

    BatchCursorPublisher<Document> createVerifiableBatchCursor(final List<Document> expected, final int batchSize) {
        return createVerifiableBatchCursor(expected, batchSize, false, false);
    }

    BatchCursorPublisher<Document> createVerifiableBatchCursorError() {
        return createVerifiableBatchCursor(emptyList(), 0, true, false);
    }

    BatchCursorPublisher<Document> createVerifiableBatchCursorError(final List<Document> expected) {
        return createVerifiableBatchCursor(expected, 0, false, true);
    }

    List<List<Document>> createBatches(final List<Document> expected, final int batchSize) {
        if (batchSize == 0) {
            return singletonList(expected);
        }
        final AtomicInteger counter = new AtomicInteger();
        return new ArrayList<>(expected.stream().collect(groupingBy(it -> counter.getAndIncrement() / batchSize)).values());
    }

    BatchCursorPublisher<Document> createVerifiableBatchCursor(final List<Document> expected, final int batchSize,
                                                                    final boolean errorCreatingCursor, final boolean errorOnEmpty) {

        BatchCursorPublisher<Document> publisher = new BatchCursorPublisher<Document>(
                null, OPERATION_PUBLISHER) {
            @Override
            AsyncReadOperation<AsyncBatchCursor<Document>> asAsyncReadOperation(final int initialBatchSize) {
                return readOperation;
            }
        };

        OperationExecutor executor = OPERATION_EXECUTOR;

        if (batchSize > 0) {
            publisher.batchSize(batchSize);
        }

        ArgumentMatcher<Supplier<AsyncReadOperation<AsyncBatchCursor<Document>>>> supplierMatcher =
                (arg) -> arg.get().equals(readOperation);

        if (errorCreatingCursor) {
            Mockito.doAnswer(invocation -> Mono.fromCallable(() -> {
                throw new Exception(ERROR_CREATING_CURSOR);
            }))
                    .when(executor)
                    .execute(argThat(supplierMatcher),
                             eq(ReadPreference.primary()),
                             eq(ReadConcern.DEFAULT),
                             eq(null));
        } else {
            Mockito.doAnswer(invocation ->  Mono.fromCallable(() -> batchCursor))
                    .when(executor)
                    .execute(argThat(supplierMatcher),
                             eq(ReadPreference.primary()),
                             eq(ReadConcern.DEFAULT),
                             eq(null));

            Queue<List<Document>> queuedResults = new LinkedList<>(createBatches(expected, batchSize));
            AtomicBoolean isClosed = new AtomicBoolean(false);
            Mockito.lenient().doAnswer(i -> isClosed.get()).when(batchCursor).isClosed();
            Mockito.doAnswer(invocation -> {
                List<Document> next = queuedResults.poll();
                if (queuedResults.isEmpty()) {
                    if (!errorOnEmpty) {
                        isClosed.set(true);
                    } else if (next == null) {
                        invocation.getArgument(0, SingleResultCallback.class)
                                .onResult(null, new Exception(ERROR_RETURNING_RESULTS));
                        return null;
                    }
                }
                invocation.getArgument(0, SingleResultCallback.class).onResult(next, null);
                return null;
            }).when(batchCursor).next(any(SingleResultCallback.class));
        }

        return publisher;
    }

}
