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

package com.mongodb.client.model.vault;

import com.mongodb.lang.Nullable;
import org.bson.BsonValue;

/**
 * Range options specifies index options for a Queryable Encryption field supporting "range" queries.
 *
 * <p>{@code min}, {@code max}, {@code sparsity}, and {@code precision} must match the values set in the {@code encryptedFields}
 * of the destination collection.
 *
 * <p>For {@code double} and {@code decimal128}, {@code min}/{@code max}/{@code precision} must all be set, or all be unset.
 *
 * @since 4.9
 * @mongodb.server.release 6.2
 * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
 */
public class RangeOptions {

    private BsonValue min;
    private BsonValue max;
    private Integer trimFactor;
    private Long sparsity;
    private Integer precision;

    /**
     * Construct a new instance
     */
    public RangeOptions() {
    }

    /**
     * Set the minimum value set in the encryptedFields of the destination collection.
     * @param min the minimum value
     * @return this
     */
    public RangeOptions min(@Nullable final BsonValue min) {
        this.min = min;
        return this;
    }

    /**
     * @return the minimum value if set
     */
    @Nullable
    public BsonValue getMin() {
        return min;
    }

    /**
     * Set the maximum value set in the encryptedFields of the destination collection.
     * @param max the maximum value
     * @return this
     */
    public RangeOptions max(@Nullable final BsonValue max) {
        this.max = max;
        return this;
    }

    /**
     * @return the trim factor value if set
     * @since 5.2
     */
    @Nullable
    public Integer getTrimFactor() {
        return trimFactor;
    }

    /**
     * Set the number of top-level edges stored per record.
     * <p>
     * The trim factor may be used to tune performance.
     *
     * @param trimFactor the trim factor
     * @return this
     * @since 5.2
     */
    public RangeOptions trimFactor(@Nullable final Integer trimFactor) {
        this.trimFactor = trimFactor;
        return this;
    }

    /**
     * @return the maximum value if set
     */
    @Nullable
    public BsonValue getMax() {
        return max;
    }

    /**
     * Set the Queryable Encryption range hypergraph sparsity factor.
     * <p>
     * Sparsity may be used to tune performance.
     *
     * @param sparsity the sparsity
     * @return this
     */
    public RangeOptions sparsity(@Nullable final Long sparsity) {
        this.sparsity = sparsity;
        return this;
    }

    /**
     * @return the sparsity value if set
     */
    @Nullable
    public Long getSparsity() {
        return sparsity;
    }

    /**
     * Set the precision of double or decimal128 values in the encryptedFields of the destination collection.
     * @param precision the precision
     * @return this
     */
    public RangeOptions precision(@Nullable final Integer precision) {
        this.precision = precision;
        return this;
    }

    /**
     * @return the precision value if set
     */
    @Nullable
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public String toString() {
        return "RangeOptions{"
                + "min=" + min
                + ", max=" + max
                + ", trimFactor=" + trimFactor
                + ", sparsity=" + sparsity
                + ", precision=" + precision
                + '}';
    }
}
