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

package com.mongodb.client.model.search;

import com.mongodb.annotations.Immutable;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;

@Immutable
public final class BoostSearchScore extends SearchScore {
    private final double value;

    BoostSearchScore(final double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    @Override
    public BsonValue toBsonValue() {
        return new BsonDocument("boost", new BsonDocument("value", new BsonDouble(value)));
    }
}
