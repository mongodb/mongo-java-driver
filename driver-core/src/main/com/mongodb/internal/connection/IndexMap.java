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

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * <p>Efficiently maps each integer in a set to another integer in a set, useful for merging bulk write errors when a bulk write must be
 * split into multiple batches. Has the ability to switch from a range-based to a hash-based map depending on the mappings that have
 * been added.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class IndexMap {

    /**
     * Create an empty index map.
     *
     * @return a new index map
     */
    public static IndexMap create() {
        return new RangeBased();
    }

    /**
     * Create an index map that maps the integers 0..count to startIndex..startIndex + count.
     *
     * @param startIndex the start index
     * @param count      the count
     * @return an index map
     */
    public static IndexMap create(final int startIndex, final int count) {
        return new RangeBased(startIndex, count);
    }

    /**
     * Add a new index to the map
     *
     * @param index         the index
     * @param originalIndex the original index
     * @return an index map with this index added to it
     */
    public abstract IndexMap add(int index, int originalIndex);

    /**
     * Return the index that the specified index is mapped to.
     *
     * @param index the index
     * @return the index it's mapped to
     */
    public abstract int map(int index);

    private static class HashBased extends IndexMap {
        private final Map<Integer, Integer> indexMap = new HashMap<Integer, Integer>();

        HashBased(final int startIndex, final int count) {
            for (int i = startIndex; i < startIndex + count; i++) {
                indexMap.put(i - startIndex, i);
            }
        }

        @Override
        public IndexMap add(final int index, final int originalIndex) {
            indexMap.put(index, originalIndex);
            return this;
        }

        @Override
        public int map(final int index) {
            Integer originalIndex = indexMap.get(index);
            if (originalIndex == null) {
                throw new MongoInternalException("no mapping found for index " + index);
            }
            return originalIndex;
        }
    }

    private static class RangeBased extends IndexMap {
        private int startIndex;
        private int count;

        RangeBased() {
        }

        RangeBased(final int startIndex, final int count) {
            isTrueArgument("startIndex", startIndex >= 0);
            isTrueArgument("count", count > 0);
            this.startIndex = startIndex;
            this.count = count;
        }

        @Override
        public IndexMap add(final int index, final int originalIndex) {
            if (count == 0) {
                startIndex = originalIndex;
                count = 1;
                return this;
            } else if (originalIndex == startIndex + count) {
                count += 1;
                return this;
            } else {
                IndexMap hashBasedMap = new HashBased(startIndex, count);
                hashBasedMap.add(index, originalIndex);
                return hashBasedMap;
            }
        }

        @Override
        public int map(final int index) {
            if (index < 0) {
                throw new MongoInternalException("no mapping found for index " + index);
            } else if (index >= count) {
                throw new MongoInternalException("index should not be greater than or equal to count");
            }
            return startIndex + index;
        }
    }
}
