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

package com.mongodb.client;

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import java.util.function.Supplier;

/**
 * An implementation of {@link  ClientEncryption} used for testing, that retries any operation that interacts with the Key Management
 * Service (KMS), in order to handle transient errors from the KMS (e.g. socket timeouts) that are encountered occasionally in CI.
 *
 * <p>
 * Each operation will be retried three times, and sleep for one second before each retry.  If the operation fails after three attempts,
 * the last exception will be thrown.
 * </p>
 *
 * <p>
 * Note that this class is not appropriate for testing operations that the test expects to throw exceptions, as there is no way to
 * configure it to not retry when an exception is expected.
 * </p>
 *
 * <p>
 * Flaky tests like this can wrap their {@code ClientEncryption} in a {@code RetryingClientEncryption}.
 * </p>
 */
public class RetryingClientEncryption implements ClientEncryption {
    private static final Logger LOGGER = Loggers.getLogger("test");
    private final ClientEncryption wrapped;

    RetryingClientEncryption(final ClientEncryption wrapped) {
        this.wrapped = wrapped;
    }

    <T> T retryOperation(final Supplier<T> supplier) {
        RuntimeException lastException = null;
        for (int i = 0; i < 3; i++) {
            try {
                return supplier.get();
            } catch (RuntimeException e) {
                lastException = e;
                LOGGER.info("Exception in ClientEncryption.  Retrying in 1 second...", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
        }
        throw lastException;
    }


    @Override
    public BsonBinary createDataKey(final String kmsProvider) {
        return retryOperation(() -> wrapped.createDataKey(kmsProvider));
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        return retryOperation(() -> wrapped.createDataKey(kmsProvider, dataKeyOptions));
    }

    @Override
    public BsonBinary encrypt(final BsonValue value, final EncryptOptions options) {
        return retryOperation(() -> wrapped.encrypt(value, options));
    }

    @Override
    public BsonDocument encryptExpression(final Bson expression, final EncryptOptions options) {
        return retryOperation(() -> wrapped.encryptExpression(expression, options));
    }

    @Override
    public BsonValue decrypt(final BsonBinary value) {
        return retryOperation(() -> wrapped.decrypt(value));
    }

    @Override
    public DeleteResult deleteKey(final BsonBinary id) {
        return wrapped.deleteKey(id);
    }

    @Nullable
    @Override
    public BsonDocument getKey(final BsonBinary id) {
        return wrapped.getKey(id);
    }

    @Override
    public FindIterable<BsonDocument> getKeys() {
        return wrapped.getKeys();
    }

    @Nullable
    @Override
    public BsonDocument addKeyAltName(final BsonBinary id, final String keyAltName) {
        return wrapped.addKeyAltName(id, keyAltName);
    }

    @Nullable
    @Override
    public BsonDocument removeKeyAltName(final BsonBinary id, final String keyAltName) {
        return wrapped.removeKeyAltName(id, keyAltName);
    }

    @Nullable
    @Override
    public BsonDocument getKeyByAltName(final String keyAltName) {
        return wrapped.getKeyByAltName(keyAltName);
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter) {
        return retryOperation(() -> wrapped.rewrapManyDataKey(filter));
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter, final RewrapManyDataKeyOptions options) {
        return retryOperation(() -> wrapped.rewrapManyDataKey(filter, options));
    }

    @Override
    public BsonDocument createEncryptedCollection(final MongoDatabase database, final String collectionName, final CreateCollectionOptions createCollectionOptions, final CreateEncryptedCollectionParams createEncryptedCollectionParams) {
        return wrapped.createEncryptedCollection(database, collectionName, createCollectionOptions, createEncryptedCollectionParams);
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
