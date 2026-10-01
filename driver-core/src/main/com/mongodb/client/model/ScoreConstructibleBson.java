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

package com.mongodb.client.model;

import com.mongodb.annotations.Immutable;
import com.mongodb.internal.client.model.AbstractConstructibleBson;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

final class ScoreConstructibleBson extends AbstractConstructibleBson<ScoreConstructibleBson> implements ScoreOptions {
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    static final ScoreOptions EMPTY_IMMUTABLE = new ScoreConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    private ScoreConstructibleBson(final Bson base) {
        super(base);
    }

    private ScoreConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    public ScoreOptions normalization(final ScoreNormalization normalization) {
        notNull("normalization", normalization);
        return newAppended("normalization", normalization.getValue());
    }

    @Override
    public ScoreOptions weight(final double weight) {
        isTrueArgument("weight must be in the range [0, 1]", weight >= 0 && weight <= 1);
        return newAppended("weight", weight);
    }

    @Override
    public ScoreOptions scoreDetails(final boolean scoreDetails) {
        return newAppended("scoreDetails", scoreDetails);
    }

    @Override
    protected ScoreConstructibleBson newSelf(final Bson base, final Document appended) {
        return new ScoreConstructibleBson(base, appended);
    }
}
