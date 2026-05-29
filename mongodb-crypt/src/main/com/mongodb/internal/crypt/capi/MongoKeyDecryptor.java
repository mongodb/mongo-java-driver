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
 *
 */

package com.mongodb.internal.crypt.capi;

import java.nio.ByteBuffer;

/**
 * An interface representing a key decryption operation using a key management service.
 */
public interface MongoKeyDecryptor {

    /**
     * Initial read size after re-sending a KMS request. Matches libmongocrypt's DEFAULT_MAX_KMS_BYTE_REQUEST
     * and is used when {@link #bytesNeeded()} still returns 0 because libmongocrypt's should_retry flag has
     * not yet been cleared by {@link #feedAndRetry}.
     *
     * @since 5.8
     */
    int DEFAULT_KMS_READ_SIZE = 1024;

    /**
     * Gets the name of the KMS provider, e.g. "aws" or "kmip"
     *
     * @return the KMS provider name
     */
    String getKmsProvider();

    /**
     * Gets the host name of the key management service.
     *
     * @return the host name
     */
    String getHostName();

    /**
     * Gets the message to send to the key management service.
     *
     * <p>
     * Clients should call this method first, and send the message on a TLS connection to a configured KMS server.
     * </p>
     *
     * @return the message to send
     */
    ByteBuffer getMessage();

    /**
     * Gets the number of bytes that should be received from the KMS server.
     *
     * <p>
     * After sending the message to the KMS server, clients should call this method in a loop, receiving {@code bytesNeeded} from
     * the KMS server and feeding those bytes to this decryptor, until {@code bytesNeeded} is 0.
     * </p>
     *
     * @return the actual number of bytes that clients should be prepared receive
     */
    int bytesNeeded();

    /**
     * Feed the received bytes to the decryptor.
     *
     * <p>
     * After sending the message to the KMS server, clients should call this method in a loop, receiving the number of bytes indicated by
     * a call to {@link #bytesNeeded()} from the KMS server and feeding those bytes to this decryptor, until {@link #bytesNeeded()}
     * returns 0.
     * </p>
     *
     * @param bytes the received bytes
     */
    void feed(ByteBuffer bytes);

    /**
     * Gets the number of microseconds to sleep before sending the next KMS request.
     *
     * @return the number of microseconds to sleep, or 0 if no delay is needed
     * @since 5.8
     */
    long sleepMicroseconds();

    /**
     * Feed the received bytes to the decryptor, with retry support.
     *
     * @param bytes the received bytes
     * @return true if the KMS request should be retried
     * @since 5.8
     */
    boolean feedAndRetry(ByteBuffer bytes);

    /**
     * Signal to libmongocrypt that a network error occurred on this KMS request.
     *
     * @return true if the request should be retried, false if retries are exhausted
     * @since 5.8
     */
    boolean fail();
}
