/*
 * Copyright 2019-present MongoDB, Inc.
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

package com.mongodb.crypt.capi;

import java.nio.ByteBuffer;

/**
 * An interface representing a key decryption operation using a key management service.
 */
public interface MongoKeyDecryptor {

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
}
