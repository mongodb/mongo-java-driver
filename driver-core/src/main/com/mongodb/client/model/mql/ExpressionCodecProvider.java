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

package com.mongodb.client.model.mql;

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * Provides Codec instances for the {@link MqlValue MQL API}.
 *
 * <p>Responsible for converting values and computations expressed using the
 * driver's implementation of the {@link MqlValue MQL API} into the corresponding
 * values and computations expressed in MQL BSON. Booleans are converted to BSON
 * booleans, documents to BSON documents, and so on. The specific structure
 * representing numbers is preserved where possible (that is, number literals
 * specified as Java longs are converted into BSON int64, and so on).
 *
 * @since 4.9.0
 */
@Beta(Reason.CLIENT)
@Immutable
public final class ExpressionCodecProvider implements CodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (MqlExpression.class.equals(clazz)) {
            return (Codec<T>) new MqlExpressionCodec(registry);
        }
        return null;
    }
}
