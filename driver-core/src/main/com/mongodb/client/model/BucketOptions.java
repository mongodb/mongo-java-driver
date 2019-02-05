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

import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * The options for a $bucket aggregation pipeline stage
 *
 * @mongodb.driver.manual reference/operator/aggregation/bucketAuto/ $bucket
 * @mongodb.server.release 3.4
 * @since 3.4
 */
public class BucketOptions {
    private Object defaultBucket;
    private List<BsonField> output;

    /**
     * The name of the default bucket for values outside the defined buckets
     *
     * @param name the bucket value
     * @return this
     */
    public BucketOptions defaultBucket(@Nullable final Object name) {
        defaultBucket = name;
        return this;
    }

    /**
     * @return the default bucket value
     */
    @Nullable
    public Object getDefaultBucket() {
        return defaultBucket;
    }

    /**
     * @return the output document definition
     */
    @Nullable
    public List<BsonField> getOutput() {
        return output == null ? null : new ArrayList<BsonField>(output);
    }

    /**
     * The definition of the output document in each bucket
     *
     * @param output the output document definition
     * @return this
     */
    public BucketOptions output(final BsonField... output) {
        this.output = asList(output);
        return this;
    }

    /**
     * The definition of the output document in each bucket
     *
     * @param output the output document definition
     * @return this
     */
    public BucketOptions output(final List<BsonField> output) {
        this.output = output;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BucketOptions that = (BucketOptions) o;

        if (defaultBucket != null ? !defaultBucket.equals(that.defaultBucket) : that.defaultBucket != null) {
            return false;
        }
        return output != null ? output.equals(that.output) : that.output == null;
    }

    @Override
    public int hashCode() {
        int result = defaultBucket != null ? defaultBucket.hashCode() : 0;
        result = 31 * result + (output != null ? output.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BucketOptions{"
                + "defaultBucket=" + defaultBucket
                + ", output=" + output
                + '}';
    }
}
