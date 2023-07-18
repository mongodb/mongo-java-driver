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

package com.mongodb.client.model.changestream;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Objects;

/**
 * The current fragment, out of the total number of fragments.
 * When the change stream's backing aggregation pipeline contains the
 * <code>$changeStreamSplitLargeEvent</code> stage, events larger than 16MB
 * will be split into multiple events.
 *
 * @since 4.11
 * @mongodb.server.release 7.0
 * @mongodb.driver.manual reference/operator/aggregation/changeStreamSplitLargeEvent/  $changeStreamSplitLargeEvent
 */
public class SplitEvent {
    private final int fragment;
    private final int of;

    @BsonCreator
    public SplitEvent(
            @BsonProperty("fragment") final int fragment,
            @BsonProperty("of") final int of) {
        this.fragment = fragment;
        this.of = of;
    }

    /**
     * Which 1-based fragment this is, out of the total number of fragments.
     * @return the fragment number
     */
    public int getFragment() {
        return fragment;
    }

    /**
     * The total number of fragments.
     * @return the total number of fragments.
     */
    public int getOf() {
        return of;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitEvent that = (SplitEvent) o;
        return fragment == that.fragment && of == that.of;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, of);
    }

    @Override
    public String toString() {
        return "SplitEvent{"
                + "fragment=" + fragment
                + ", of=" + of
                + '}';
    }
}
