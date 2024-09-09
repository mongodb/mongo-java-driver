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

package com.mongodb.crypt.capi;

import com.mongodb.crypt.capi.CAPI.mongocrypt_binary_t;
import com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_t;
import com.mongodb.crypt.capi.CAPI.mongocrypt_status_t;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.nio.ByteBuffer;

import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_destroy;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_new;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_bytes_needed;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_endpoint;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_feed;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_get_kms_provider;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_message;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_status;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_destroy;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_new;
import static com.mongodb.crypt.capi.CAPIHelper.toBinary;
import static com.mongodb.crypt.capi.CAPIHelper.toByteBuffer;
import static org.bson.assertions.Assertions.notNull;

class MongoKeyDecryptorImpl implements MongoKeyDecryptor {
    private final mongocrypt_kms_ctx_t wrapped;

    MongoKeyDecryptorImpl(final mongocrypt_kms_ctx_t wrapped) {
        notNull("wrapped", wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public String getKmsProvider() {
        return mongocrypt_kms_ctx_get_kms_provider(wrapped, null).toString();
    }

    @Override
    public String getHostName() {
        PointerByReference hostNamePointerByReference = new PointerByReference();
        boolean success = mongocrypt_kms_ctx_endpoint(wrapped, hostNamePointerByReference);
        if (!success) {
            throwExceptionFromStatus();
        }
        Pointer hostNamePointer = hostNamePointerByReference.getValue();
        return hostNamePointer.getString(0);
    }

    @Override
    public ByteBuffer getMessage() {
        mongocrypt_binary_t binary = mongocrypt_binary_new();

        try {
            boolean success = mongocrypt_kms_ctx_message(wrapped, binary);
            if (!success) {
                throwExceptionFromStatus();
            }
            return toByteBuffer(binary);
        } finally {
            mongocrypt_binary_destroy(binary);
        }
    }

    @Override
    public int bytesNeeded() {
        return mongocrypt_kms_ctx_bytes_needed(wrapped);
    }

    @Override
    public void feed(final ByteBuffer bytes) {
        try (BinaryHolder binaryHolder = toBinary(bytes)) {
            boolean success = mongocrypt_kms_ctx_feed(wrapped, binaryHolder.getBinary());
            if (!success) {
                throwExceptionFromStatus();
            }
        }
    }

    private void throwExceptionFromStatus() {
        mongocrypt_status_t status = mongocrypt_status_new();
        mongocrypt_kms_ctx_status(wrapped, status);
        MongoCryptException e = new MongoCryptException(status);
        mongocrypt_status_destroy(status);
        throw e;
    }

}
