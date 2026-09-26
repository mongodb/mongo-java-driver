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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class ScoreFusionConstructibleBson extends AbstractConstructibleBson<ScoreFusionConstructibleBson>
        implements ScoreFusionOptions, WeightedScoreFusionCombination {
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    static final ScoreFusionConstructibleBson EMPTY_IMMUTABLE =
            new ScoreFusionConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    ScoreFusionConstructibleBson(final Bson base) {
        super(base);
    }

    private ScoreFusionConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    @Override
    protected ScoreFusionConstructibleBson newSelf(final Bson base, final Document appended) {
        return new ScoreFusionConstructibleBson(base, appended);
    }

    @Override
    public ScoreFusionOptions combination(final ScoreFusionCombination combination) {
        return newAppended("combination", notNull("combination", combination));
    }

    @Override
    public ScoreFusionOptions scoreDetails(final boolean scoreDetails) {
        return newAppended("scoreDetails", BsonBoolean.valueOf(scoreDetails));
    }

    @Override
    public ScoreFusionOptions option(final String name, final Object value) {
        return newAppended(notNull("name", name), notNull("value", value));
    }

    @Override
    public WeightedScoreFusionCombination avg() {
        return newAppended("method", new BsonString("avg"));
    }
}
