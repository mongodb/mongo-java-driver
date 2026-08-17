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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Stream;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.time.Timeout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that a socket read timeout in the reactive KeyManagementService is classified as a
 * retryable network error when the CSOT deadline has not yet expired, and as a
 * {@link MongoOperationTimeoutException} only when the deadline has actually passed.
 */
class KeyManagementServiceKmsRetryTest {

    private MongoKeyDecryptor keyDecryptor;
    private KeyManagementService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        keyDecryptor = mock(MongoKeyDecryptor.class);
        when(keyDecryptor.getKmsProvider()).thenReturn("aws");
        when(keyDecryptor.getHostName()).thenReturn("kms.example.com");
        when(keyDecryptor.getMessage()).thenReturn(ByteBuffer.allocate(0));
        when(keyDecryptor.sleepMicroseconds()).thenReturn(0L);
        when(keyDecryptor.bytesNeeded()).thenReturn(1);

        Stream stream = mock(Stream.class);
        StreamFactory streamFactory = mock(StreamFactory.class);
        TlsChannelStreamFactoryFactory factoryFactory = mock(TlsChannelStreamFactoryFactory.class);

        when(streamFactory.create(any(ServerAddress.class))).thenReturn(stream);
        when(factoryFactory.create(any(SocketSettings.class), any(SslSettings.class))).thenReturn(streamFactory);

        doAnswer(inv -> {
            AsyncCompletionHandler<Void> handler = inv.getArgument(1);
            handler.failed(new MongoSocketReadTimeoutException(
                    "read timed out", new ServerAddress("kms.example.com"), new Exception()));
            return null;
        }).when(stream).openAsync(any(OperationContext.class), any());

        service = new KeyManagementService(emptyMap(), 1000, factoryFactory);
    }

    @Test
    void shouldTreatSocketReadTimeoutAsRetryableNetworkErrorWhenBudgetRemains() {
        // retries exhausted, so the network error itself surfaces
        when(keyDecryptor.fail()).thenReturn(false);
        Timeout operationTimeout = Timeout.expiresIn(5, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED);

        StepVerifier.create(service.decryptKey(keyDecryptor, operationTimeout))
                .expectErrorSatisfies(error -> {
                    // the connect/handshake socket timeout is a fixed KMS timeout, not the operation deadline,
                    // so with budget remaining it must consume retry budget rather than report a CSOT timeout
                    assertFalse(error instanceof MongoOperationTimeoutException,
                            "a socket timeout with operation budget remaining must not be classified as an operation timeout");
                    // the meaningful MongoSocketReadTimeoutException must be preserved, not stripped to its raw cause
                    assertTrue(error instanceof MongoSocketReadTimeoutException,
                            "the socket read timeout type must be preserved");
                })
                .verify();

        verify(keyDecryptor).fail();
    }

    @Test
    void shouldThrowOperationTimeoutWhenDeadlineHasExpired() {
        Timeout operationTimeout = Timeout.expiresIn(0, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED);

        StepVerifier.create(service.decryptKey(keyDecryptor, operationTimeout))
                .expectError(MongoOperationTimeoutException.class)
                .verify();

        verify(keyDecryptor, never()).fail();
    }

    @Test
    void shouldThrowOperationTimeoutWhenRetryBackoffExceedsRemainingBudget() {
        // libmongocrypt asks for a retry backoff that cannot fit within the remaining CSOT budget, so
        // checkKmsRetryBackoff must fail fast with an operation timeout before any further KMS attempt
        when(keyDecryptor.sleepMicroseconds()).thenReturn(TimeUnit.SECONDS.toMicros(10));
        Timeout operationTimeout = Timeout.expiresIn(1, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED);

        StepVerifier.create(service.decryptKey(keyDecryptor, operationTimeout))
                .expectError(MongoOperationTimeoutException.class)
                .verify();

        verify(keyDecryptor, never()).fail();
    }
}
