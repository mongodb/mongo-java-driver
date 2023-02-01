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
package com.mongodb.internal.client.vault;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RangeOptions;
import com.mongodb.crypt.capi.MongoExplicitEncryptOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public final class EncryptOptionsHelper {

    public static MongoExplicitEncryptOptions asMongoExplicitEncryptOptions(final EncryptOptions options) {
        MongoExplicitEncryptOptions.Builder encryptOptionsBuilder = MongoExplicitEncryptOptions.builder()
                .algorithm(options.getAlgorithm());

        if (options.getKeyId() != null) {
            encryptOptionsBuilder.keyId(options.getKeyId());
        }

        if (options.getKeyAltName() != null) {
            encryptOptionsBuilder.keyAltName(options.getKeyAltName());
        }

        if (options.getContentionFactor() != null) {
            encryptOptionsBuilder.contentionFactor(options.getContentionFactor());
        }

        if (options.getQueryType() != null) {
            encryptOptionsBuilder.queryType(options.getQueryType());
        }

        RangeOptions rangeOptions = options.getRangeOptions();
        if (rangeOptions != null) {
            BsonDocument rangeOptionsBsonDocument = new BsonDocument();
            BsonValue min = rangeOptions.getMin();
            if (min != null) {
                rangeOptionsBsonDocument.put("min", min);
            }
            BsonValue max = rangeOptions.getMax();
            if (max != null) {
                rangeOptionsBsonDocument.put("max", max);
            }
            Long sparsity = rangeOptions.getSparsity();
            if (sparsity != null) {
                rangeOptionsBsonDocument.put("sparsity", new BsonInt64(sparsity));
            }
            Integer precision = rangeOptions.getPrecision();
            if (precision != null) {
                rangeOptionsBsonDocument.put("precision", new BsonInt32(precision));
            }
            encryptOptionsBuilder.rangeOptions(rangeOptionsBsonDocument);
        }
        return encryptOptionsBuilder.build();
    }
    private EncryptOptionsHelper() {
    }
}
