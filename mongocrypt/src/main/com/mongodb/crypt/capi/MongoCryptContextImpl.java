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

import com.mongodb.crypt.capi.CAPI.mongocrypt_binary_t;
import com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_t;
import com.mongodb.crypt.capi.CAPI.mongocrypt_kms_ctx_t;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_destroy;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_new;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_destroy;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_finalize;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_kms_done;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_mongo_done;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_mongo_feed;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_mongo_op;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_next_kms_ctx;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_provide_kms_providers;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_state;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_ctx_status;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_destroy;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_new;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_t;
import static com.mongodb.crypt.capi.CAPIHelper.toBinary;
import static com.mongodb.crypt.capi.CAPIHelper.toDocument;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.notNull;

class MongoCryptContextImpl implements MongoCryptContext {
    private final mongocrypt_ctx_t wrapped;
    private volatile boolean closed;

    MongoCryptContextImpl(final mongocrypt_ctx_t wrapped) {
        notNull("wrapped", wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public State getState() {
        isTrue("open", !closed);
        return State.fromIndex(mongocrypt_ctx_state(wrapped));
    }

    @Override
    public RawBsonDocument getMongoOperation() {
        isTrue("open", !closed);
        mongocrypt_binary_t binary = mongocrypt_binary_new();

        try {
            boolean success = mongocrypt_ctx_mongo_op(wrapped, binary);
            if (!success) {
                throwExceptionFromStatus();
            }
            return toDocument(binary);
        } finally {
            mongocrypt_binary_destroy(binary);
        }
    }

    @Override
    public void addMongoOperationResult(final BsonDocument document) {
        isTrue("open", !closed);

        try (BinaryHolder binaryHolder = toBinary(document)) {
            boolean success = mongocrypt_ctx_mongo_feed(wrapped, binaryHolder.getBinary());
            if (!success) {
                throwExceptionFromStatus();
            }
        }
    }

    @Override
    public void completeMongoOperation() {
        isTrue("open", !closed);
        boolean success = mongocrypt_ctx_mongo_done(wrapped);
        if (!success) {
            throwExceptionFromStatus();
        }
    }

    @Override
    public void provideKmsProviderCredentials(final BsonDocument credentialsDocument) {
        try (BinaryHolder binaryHolder = toBinary(credentialsDocument)) {
            boolean success = mongocrypt_ctx_provide_kms_providers(wrapped, binaryHolder.getBinary());
            if (!success) {
                throwExceptionFromStatus();
            }
        }
    }

    @Override
    public MongoKeyDecryptor nextKeyDecryptor() {
        isTrue("open", !closed);

        mongocrypt_kms_ctx_t kmsContext = mongocrypt_ctx_next_kms_ctx(wrapped);
        if (kmsContext == null) {
            return null;
        }
        return new MongoKeyDecryptorImpl(kmsContext);
    }

    @Override
    public void completeKeyDecryptors() {
        isTrue("open", !closed);

        boolean success = mongocrypt_ctx_kms_done(wrapped);
        if (!success) {
            throwExceptionFromStatus();
        }

    }

    @Override
    public RawBsonDocument finish() {
        isTrue("open", !closed);

        mongocrypt_binary_t binary = mongocrypt_binary_new();

        try {
            boolean success = mongocrypt_ctx_finalize(wrapped, binary);
            if (!success) {
                throwExceptionFromStatus();
            }
            return toDocument(binary);
        } finally {
            mongocrypt_binary_destroy(binary);
        }
    }

    @Override
    public void close() {
        mongocrypt_ctx_destroy(wrapped);
        closed = true;
    }

    static void throwExceptionFromStatus(final mongocrypt_ctx_t wrapped) {
        mongocrypt_status_t status = mongocrypt_status_new();
        mongocrypt_ctx_status(wrapped, status);
        MongoCryptException e = new MongoCryptException(status);
        mongocrypt_status_destroy(status);
        throw e;
    }

    private void throwExceptionFromStatus() {
        throwExceptionFromStatus(wrapped);
    }
}
