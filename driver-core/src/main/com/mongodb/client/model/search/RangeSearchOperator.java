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
import com.mongodb.lang.Nullable;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

@Immutable
public final class RangeSearchOperator extends SearchOperator {

    private final List<SearchPath> path;
    @Nullable private final BsonValue gt;
    @Nullable private final BsonValue gte;
    @Nullable private final BsonValue lt;
    @Nullable private final BsonValue lte;

    RangeSearchOperator(final List<SearchPath> path, @Nullable final String index,
            @Nullable final BsonValue gt, @Nullable final BsonValue gte, @Nullable final BsonValue lt, @Nullable final BsonValue lte) {
        super(index);
        this.path = notNull("path", path);
        this.gt = gt;
        this.gte = gte;
        this.lt = lt;
        this.lte = lte;
    }

    public List<SearchPath> getPath() {
        return path;
    }

    public RangeSearchOperator gt(Number value) {
        return new RangeSearchOperator(path, getIndex(), numberToBsonValue(value), gte, lt, lte);
    }

    public RangeSearchOperator gte(Number value) {
        return new RangeSearchOperator(path, getIndex(), gt, numberToBsonValue(value), lt, lte);
    }

    public RangeSearchOperator lt(Number value) {
        return new RangeSearchOperator(path, getIndex(), gt, gte, numberToBsonValue(value), lte);
    }

    public RangeSearchOperator lte(Number value) {
        return new RangeSearchOperator(path, getIndex(), gt, gte, lt, numberToBsonValue(value));
    }

    @Override
    public RangeSearchOperator index(final String index) {
        return new RangeSearchOperator(path, index, gt, gte, lt, lte);
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocument searchStageDocument = new BsonDocument();
        appendCommonFields(searchStageDocument);
        BsonDocument rangeSearchOperator = new BsonDocument();

        appendPath(rangeSearchOperator, path);

        if (gt != null) {
            rangeSearchOperator.append("gt", gt);
        }

        if (gte != null) {
            rangeSearchOperator.append("gte", gte);
        }

        if (lt != null) {
            rangeSearchOperator.append("lt", lt);
        }

        if (lte != null) {
            rangeSearchOperator.append("lte", lte);
        }

        searchStageDocument.append("range", rangeSearchOperator);

        return searchStageDocument;
    }

    private static BsonValue numberToBsonValue(final Number value) {
        if (value instanceof Integer) {
            return new BsonInt32((Integer) value);
        } else if (value instanceof Long) {
            return new BsonInt64((Long) value);
        } else if (value instanceof Double) {
            return new BsonDouble((Double) value);
        } else if (value instanceof Decimal128) {
            return new BsonDecimal128((Decimal128) value);
        } else {
            throw new IllegalArgumentException("Unsupported number subclass: " + value.getClass());
        }
    }
}
