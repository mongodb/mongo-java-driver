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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.crypt.capi.MongoCryptContext;
import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.time.Timeout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies how KMS network failures are classified under CSOT: a {@code SocketTimeoutException}
 * from the fixed-timeout connect/handshake phase with operation budget remaining is a transient,
 * retryable network error, whereas an expired operation deadline must surface as
 * {@link MongoOperationTimeoutException}.
 */
class CryptKmsRetryTest {

    private MongoCryptContext cryptContext;
    private MongoKeyDecryptor keyDecryptor;
    private KeyManagementService keyManagementService;
    private Crypt crypt;

    @BeforeEach
    void setUp() throws Exception {
        MongoCrypt mongoCrypt = mock(MongoCrypt.class);
        cryptContext = mock(MongoCryptContext.class);
        keyDecryptor = mock(MongoKeyDecryptor.class);
        keyManagementService = mock(KeyManagementService.class);

        when(mongoCrypt.createDataKeyContext(anyString(), any())).thenReturn(cryptContext);
        when(cryptContext.getState()).thenReturn(MongoCryptContext.State.NEED_KMS);
        when(cryptContext.nextKeyDecryptor()).thenReturn(keyDecryptor);
        when(keyDecryptor.getKmsProvider()).thenReturn("aws");
        when(keyDecryptor.getHostName()).thenReturn("kms.example.com");
        when(keyDecryptor.getMessage()).thenReturn(ByteBuffer.allocate(0));
        when(keyDecryptor.sleepMicroseconds()).thenReturn(0L);
        when(keyManagementService.stream(anyString(), anyString(), any(), any()))
                .thenThrow(new SocketTimeoutException("connect timed out"));

        crypt = new Crypt(mongoCrypt, mock(KeyRetriever.class), keyManagementService, emptyMap(), emptyMap());
    }

    @Test
    void shouldTreatSocketTimeoutAsRetryableNetworkErrorWhenBudgetRemains() {
        // retries exhausted, so the network error itself surfaces
        when(keyDecryptor.fail()).thenReturn(false);
        Timeout operationTimeout = Timeout.expiresIn(30, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED);

        MongoClientException e = assertThrows(MongoClientException.class,
                () -> crypt.createDataKey("aws", new DataKeyOptions(), operationTimeout));

        // the connect/handshake socket timeout is a fixed KMS timeout, not the operation deadline,
        // so with budget remaining it must consume retry budget rather than report a CSOT timeout
        assertFalse(e instanceof MongoOperationTimeoutException,
                "a socket timeout with operation budget remaining must not be classified as an operation timeout");
        verify(keyDecryptor).fail();
    }

    @Test
    void shouldThrowOperationTimeoutWhenDeadlineHasExpired() {
        Timeout operationTimeout = Timeout.expiresIn(0, TimeUnit.SECONDS, Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED);

        assertThrows(MongoOperationTimeoutException.class,
                () -> crypt.createDataKey("aws", new DataKeyOptions(), operationTimeout));
        verify(keyDecryptor, never()).fail();
    }
}
