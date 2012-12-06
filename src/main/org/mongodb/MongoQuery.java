/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb;

public interface MongoQuery<T> {

    /**
     * <p>Sorts based on a property (defines return order).  Examples:</p>
     * <p/>
     * <ul>
     * <li>{@code order("age")}</li>
     * <li>{@code order("-age")} (descending order)</li>
     * <li>{@code order("age, date")}</li>
     * <li>{@code order("age,-date")} (age ascending, date descending)</li>
     * </ul>
     */
    MongoQuery order(String condition);

    /**
     * Limit the fetched result set to a certain number of values.
     *
     * @param value must be >= 0.  A value of 0 indicates no limit.
     */
    MongoQuery limit(int value);

    /**
     * Batch-size of the fetched result (cursor).
     *
     * @param value must be >= 0.  A value of 0 indicates the server default.
     */
    MongoQuery batchSize(int value);

    /**
     * Starts the query results at a particular zero-based offset.
     *
     * @param value must be >= 0
     */
    MongoQuery offset(int value);

    /**
     * Hints as to which index should be used.
     */
    MongoQuery hintIndex(String idxName);

    /**
     * Limits the fields retrieved
     */
    MongoQuery retrievedFields(boolean include, String... fields);

    /**
     * Enabled snapshot mode where duplicate results
     * (which may be updated during the lifetime of the cursor)
     * will not be returned. Not compatible with order/sort and hint.
     */
    MongoQuery enableSnapshotMode();

    /**
     * Disable snapshot mode (default mode). This will be faster
     * but changes made during the cursor may cause duplicates.
     */
    MongoQuery disableSnapshotMode();

    /**
     * Set the read preference for this query
     */
    MongoQuery readPreference(ReadPreference readPreference);

    /**
     * Disables cursor timeout on server.
     */
    MongoQuery disableTimeout();

    /**
     * Enables cursor timeout on server.
     */
    MongoQuery enableTimeout();

    MongoCursor<T> entries();
}
