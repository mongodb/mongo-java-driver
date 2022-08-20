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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.internal.operation.Operations;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class TestHelper {

    @Mock
    private AsyncBatchCursor<Document> batchCursor;

    TestHelper() {
    }


    static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");
    static final Collation COLLATION = Collation.builder().locale("de").build();

    static final MongoOperationPublisher<Document> OPERATION_PUBLISHER;
    static final OperationExecutor OPERATION_EXECUTOR;

    static {
        OperationExecutor executor = mock(OperationExecutor.class);
        Mockito.lenient().doAnswer(invocation -> Mono.empty())
                .when(executor)
                .execute(any(), any(), any());
        Mockito.lenient().doAnswer(invocation -> Mono.empty())
                .when(executor)
                .execute(any(), any(), any(), any());
        OPERATION_EXECUTOR = executor;
        OPERATION_PUBLISHER = createMongoOperationPublisher(OPERATION_EXECUTOR);
    }

    static final CodecRegistry BSON_CODEC_REGISTRY = fromProviders(new BsonValueCodecProvider());

    static MongoOperationPublisher<Document> createMongoOperationPublisher(final OperationExecutor executor) {
        return new MongoOperationPublisher<>(NAMESPACE, Document.class,
                                             getDefaultCodecRegistry(), ReadPreference.primary(), ReadConcern.DEFAULT,
                                             WriteConcern.ACKNOWLEDGED, true, true,
                                             UuidRepresentation.STANDARD, null, executor);
    }


    public static void assertOperationIsTheSameAs(@Nullable final Object expectedOperation, @Nullable final Object actualOperation) {

        if (expectedOperation instanceof AsyncReadOperation) {
            assertTrue(actualOperation instanceof AsyncReadOperation, "Both async read operations");
        } else {
            assertTrue(actualOperation instanceof AsyncWriteOperation, "Both async write operations");
        }

        Map<String, Object> expectedMap = getClassGetterValues(unwrapOperation(expectedOperation));
        Map<String, Object> actualMap = getClassGetterValues(unwrapOperation(actualOperation));
        assertEquals(expectedMap, actualMap);
    }

    public static void assertPublisherIsTheSameAs(final Publisher<?> expectedPublisher, final Publisher<?> actualPublisher) {
        assertPublisherIsTheSameAs(expectedPublisher, actualPublisher, null);
    }

    public static void assertPublisherIsTheSameAs(final Publisher<?> expectedPublisher, final Publisher<?> actualPublisher,
            @Nullable final String message) {
        Map<String, Optional<Object>> expectedMap = getClassPrivateFieldValues(getRootSource(expectedPublisher));
        Map<String, Optional<Object>> actualMap = getClassPrivateFieldValues(getRootSource(actualPublisher));
        assertEquals(expectedMap, actualMap, message);
    }

    private static Object unwrapOperation(@Nullable final Object operation) {
        assertTrue(operation instanceof AsyncReadOperation || operation instanceof AsyncWriteOperation,
                   "Must be a read or write operation");
        if (operation instanceof MapReducePublisherImpl.WrappedMapReduceReadOperation) {
            return ((MapReducePublisherImpl.WrappedMapReduceReadOperation<?>) operation).getOperation();
        } else if (operation instanceof MapReducePublisherImpl.WrappedMapReduceWriteOperation) {
            return ((MapReducePublisherImpl.WrappedMapReduceWriteOperation) operation).getOperation();
        }
        return operation;
    }

    @NonNull
    private static Map<String, Object> getClassGetterValues(final Object instance) {
        return Arrays.stream(instance.getClass().getMethods())
                .filter(n -> n.getParameterCount() == 0 && (n.getName().startsWith("get") || n.getName().startsWith("is")))
                .collect(toMap(Method::getName, n -> {
                    Object value = null;
                    try {
                        value = checkValueTypes(n.invoke(instance));
                    } catch (Exception e) {
                        // Ignore value
                    }
                    return value != null ? value : "null";
                }));
    }


    private static Map<String, Optional<Object>> getClassPrivateFieldValues(final Object instance) {
        return Arrays.stream(instance.getClass().getDeclaredFields())
                .filter(field -> Modifier.isPrivate(field.getModifiers()))
                .collect(toMap(Field::getName, field -> {
                    Optional<Object> value = Optional.empty();
                    field.setAccessible(true);
                    try {
                        value = Optional.ofNullable(field.get(instance));
                    } catch (IllegalAccessException e) {
                        // ignore
                    }
                    return value.map(TestHelper::checkValueTypes);
                }));
    }

    private static Object checkValueTypes(final Object instance) {
        Object actual = instance instanceof Optional ? ((Optional<Object>) instance).orElse(instance) : instance;
        if (actual instanceof AsyncReadOperation || actual instanceof AsyncWriteOperation) {
            return getClassPrivateFieldValues(actual);
        } else if (actual instanceof Operations) {
            return getClassPrivateFieldValues(actual);
        } else if (actual.getClass().getSimpleName().equals("ChangeStreamDocumentCodec")) {
            return getClassGetterValues(actual);
        } else if (actual instanceof FindOptions) {
            return getClassGetterValues(actual);
        } else if (actual instanceof WriteRequest) {
            return getClassGetterValues(actual);
        } else if (actual instanceof IndexRequest) {
            return getClassGetterValues(actual);
        } else if (actual instanceof List && !((List<?>) actual).isEmpty()) {
            return ((List<?>) actual).stream()
                                       .map(TestHelper::checkValueTypes)
                                       .collect(toList());
        }
        return actual;
    }

    private static Publisher<?> getRootSource(final Publisher<?> publisher) {
        Optional<Publisher<?>> sourcePublisher = Optional.of(publisher);
        // Uses reflection to find the root / source publisher
        if (publisher instanceof Scannable) {
            Scannable scannable = (Scannable) publisher;
            List<? extends Scannable> parents = scannable.parents().collect(toList());
            if (parents.isEmpty()) {
                sourcePublisher = getSource(scannable);
            } else {
                sourcePublisher = parents.stream().map(TestHelper::getSource)
                        .filter(Optional::isPresent)
                        .reduce((first, second) -> second)
                        .orElse(Optional.empty());
            }
        }
        return sourcePublisher.orElse(publisher);
    }

    private static Optional<Publisher<?>> getSource(final Scannable scannable) {
        Optional<Publisher<?>> optionalSource = getScannableSource(scannable);
        if (optionalSource.isPresent()) {
            return optionalSource;
        } else {
            return getScannableArray(scannable);
        }
    }

    private static Optional<Publisher<?>> getScannableSource(final Scannable scannable) {
        return (Optional<Publisher<?>>) getScannableFieldValue(scannable, "source");
    }

    private static Optional<Publisher<?>> getScannableArray(final Scannable scannable) {
        return getScannableFieldValue(scannable, "array")
                .flatMap((Function<Object, Optional<? extends Publisher<?>>>) o ->
                        Arrays.stream((Publisher<?>[]) o).map(TestHelper::getRootSource)
                        .reduce((first, second) -> first));
    }

    private static Optional<?> getScannableFieldValue(final Scannable scannable, final String fieldName) {
        try {
            Optional<Field> sourceField = Arrays.stream(scannable.getClass().getDeclaredFields())
                    .filter(field -> field.getName().equals(fieldName))
                    .findFirst();
            if (sourceField.isPresent()) {
                sourceField.get().setAccessible(true);
                return Optional.of(sourceField.get().get(scannable));
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    TestOperationExecutor createOperationExecutor(final List<Object> responses) {
        configureBatchCursor();
        return new TestOperationExecutor(responses);
    }


    void configureBatchCursor() {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        Mockito.lenient().doAnswer(i -> isClosed.get()).when(getBatchCursor()).isClosed();
        Mockito.lenient().doAnswer(invocation -> {
            isClosed.set(true);
            invocation.getArgument(0, SingleResultCallback.class).onResult(null, null);
            return null;
        }).when(getBatchCursor()).next(any(SingleResultCallback.class));
    }

    public AsyncBatchCursor<Document> getBatchCursor() {
        return batchCursor;
    }
}
