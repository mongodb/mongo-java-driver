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

final class GeoNearConstructibleBson extends AbstractConstructibleBson<GeoNearConstructibleBson> implements GeoNearOptions {
    /**
     * An {@linkplain Immutable immutable} {@link BsonDocument#isEmpty() empty} instance.
     */
    static final GeoNearOptions EMPTY_IMMUTABLE = new GeoNearConstructibleBson(AbstractConstructibleBson.EMPTY_IMMUTABLE);

    private GeoNearConstructibleBson(final Bson base) {
        super(base);
    }

    private GeoNearConstructibleBson(final Bson base, final Document appended) {
        super(base, appended);
    }

    private GeoNearOptions setOption(final String key, final Object value) {
        return newAppended(key, value);
    }

    @Override
    public GeoNearOptions distanceMultiplier(final Number distanceMultiplier) {
        return setOption("distanceMultiplier", distanceMultiplier);
    }

    @Override
    public GeoNearOptions includeLocs(final String includeLocs) {
        return setOption("includeLocs", includeLocs);
    }

    @Override
    public GeoNearOptions key(final String key) {
        return setOption("key", key);
    }

    @Override
    public GeoNearOptions minDistance(final Number minDistance) {
        return setOption("minDistance", minDistance);
    }

    @Override
    public GeoNearOptions maxDistance(final Number maxDistance) {
        return setOption("maxDistance", maxDistance);
    }

    @Override
    public GeoNearOptions query(final Document query) {
        return setOption("query", query);
    }

    @Override
    public GeoNearOptions spherical() {
        return setOption("spherical", true);
    }

    @Override
    protected GeoNearConstructibleBson newSelf(final Bson base, final Document appended) {
        return new GeoNearConstructibleBson(base, appended);
    }
}
