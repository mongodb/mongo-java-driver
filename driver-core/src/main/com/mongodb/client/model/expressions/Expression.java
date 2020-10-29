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

package com.mongodb.client.model.expressions;

import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * An expression in the MongoDB aggregation language.
 *
 * @mongodb.driver.manual reference/operator/aggregation/
 * @see Expressions
 * @since 4.?
 */
public interface Expression {
    /**
     * Convert an expression to a BSON value
     *
     * @param codecRegistry the registry to use to convert literal expressions
     * @return the BSON value represented by this expression
     */
    BsonValue toBsonValue(CodecRegistry codecRegistry);

    /**
     * Convert an expression to a BSON value using a codec registry appropriate for the implementation.
     * <p>
     * The default implementation of this method calls {@link #toBsonValue(CodecRegistry)} with the
     * {@link Bson#DEFAULT_CODEC_REGISTRY} as the second argument.
     * </p>
     *
     * @return the BSON value represented by this expression
     */
    default BsonValue toBsonValue() {
        return toBsonValue(Bson.DEFAULT_CODEC_REGISTRY);
    }
}
