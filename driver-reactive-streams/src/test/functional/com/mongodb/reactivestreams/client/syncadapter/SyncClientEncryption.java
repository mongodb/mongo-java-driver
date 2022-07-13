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

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static java.util.Objects.requireNonNull;

public class SyncClientEncryption implements ClientEncryption {

    private final com.mongodb.reactivestreams.client.vault.ClientEncryption wrapped;

    public SyncClientEncryption(final com.mongodb.reactivestreams.client.vault.ClientEncryption wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider) {
        return requireNonNull(Mono.from(wrapped.createDataKey(kmsProvider, new DataKeyOptions())).block(TIMEOUT_DURATION));
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        return requireNonNull(Mono.from(wrapped.createDataKey(kmsProvider, dataKeyOptions)).block(TIMEOUT_DURATION));
    }

    @Override
    public BsonBinary encrypt(final BsonValue value, final EncryptOptions options) {
        return requireNonNull(Mono.from(wrapped.encrypt(value, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public BsonValue decrypt(final BsonBinary value) {
        return requireNonNull(Mono.from(wrapped.decrypt(value)).block(TIMEOUT_DURATION));
    }

    @Override
    public DeleteResult deleteKey(final BsonBinary id) {
        return requireNonNull(Mono.from(wrapped.deleteKey(id)).block(TIMEOUT_DURATION));
    }

    @Override
    public BsonDocument getKey(final BsonBinary id) {
        return Mono.from(wrapped.getKey(id)).block(TIMEOUT_DURATION);
    }

    @Override
    public FindIterable<BsonDocument> getKeys() {
        return new SyncFindIterable<>(wrapped.getKeys());
    }

    @Override
    public BsonDocument addKeyAltName(final BsonBinary id, final String keyAltName) {
        return Mono.from(wrapped.addKeyAltName(id, keyAltName)).block(TIMEOUT_DURATION);
    }

    @Override
    public BsonDocument removeKeyAltName(final BsonBinary id, final String keyAltName) {
        return Mono.from(wrapped.removeKeyAltName(id, keyAltName)).block(TIMEOUT_DURATION);
    }

    @Override
    public BsonDocument getKeyByAltName(final String keyAltName) {
        return Mono.from(wrapped.getKeyByAltName(keyAltName)).block(TIMEOUT_DURATION);
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter) {
        return requireNonNull(Mono.from(wrapped.rewrapManyDataKey(filter)).block(TIMEOUT_DURATION));
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter, final RewrapManyDataKeyOptions options) {
        return requireNonNull(Mono.from(wrapped.rewrapManyDataKey(filter, options)).block(TIMEOUT_DURATION));
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
