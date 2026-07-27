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

import org.bson.BsonValue;

import java.util.Objects;

final class ScoreNormalizationBson implements ScoreNormalization {
    private final BsonValue normalization;

    ScoreNormalizationBson(final BsonValue normalization) {
        this.normalization = normalization;
    }

    @Override
    public BsonValue toBsonValue() {
        return normalization;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScoreNormalizationBson that = (ScoreNormalizationBson) o;
        return Objects.equals(normalization, that.normalization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalization);
    }

    @Override
    public String toString() {
        return "ScoreNormalization{normalization=" + normalization + '}';
    }
}
