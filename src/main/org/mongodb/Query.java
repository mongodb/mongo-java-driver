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

import com.mongodb.ReadPreference;
import org.bson.types.CodeWScope;

public interface Query {

    /**
     * <p>Create a filter based on the specified condition and value.
     * </p><p>
     * <b>Note</b>: Property is in the form of "name op" ("age >").
     * </p><p>
     * Valid operators are ["=", "==","!=", "<>", ">", "<", ">=", "<=", "in", "nin", "all", "size", "exists"]
     * </p>
     * <p>Examples:</p>
     *
     * <ul>
     * <li>{@code filter("yearsOfOperation >", 5)}</li>
     * <li>{@code filter("rooms.maxBeds >=", 2)}</li>
     * <li>{@code filter("rooms.bathrooms exists", 1)}</li>
     * <li>{@code filter("stars in", new Long[]{3,4}) //3 and 4 stars (midrange?)}</li>
     * <li>{@code filter("age >=", age)}</li>
     * <li>{@code filter("age =", age)}</li>
     * <li>{@code filter("age", age)} (if no operator, = is assumed)</li>
     * <li>{@code filter("age !=", age)}</li>
     * <li>{@code filter("age in", ageList)}</li>
     * <li>{@code filter("customers.loyaltyYears in", yearsList)}</li>
     * </ul>
     */
    Query filter(String condition, Object value);


    /**
     * Limit the query using this javascript block; only one per query
     */
    Query where(String js);

    /**
     * Limit the query using this javascript block; only one per query
     */
    Query where(CodeWScope js);

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
    Query order(String condition);

    /**
     * Limit the fetched result set to a certain number of values.
     *
     * @param value must be >= 0.  A value of 0 indicates no limit.
     */
    Query limit(int value);

    /**
     * Batch-size of the fetched result (cursor).
     *
     * @param value must be >= 0.  A value of 0 indicates the server default.
     */
    Query batchSize(int value);

    /**
     * Starts the query results at a particular zero-based offset.
     *
     * @param value must be >= 0
     */
    Query offset(int value);

    @Deprecated
    Query skip(int value);

    /**
     * Turns on validation (for all calls made after); by default validation is on
     */
    Query enableValidation();

    /**
     * Turns off validation (for all calls made after)
     */
    Query disableValidation();

    /**
     * Hints as to which index should be used.
     */
    Query hintIndex(String idxName);

    /**
     * Limits the fields retrieved
     */
    Query retrievedFields(boolean include, String... fields);

    /**
     * Enabled snapshot mode where duplicate results
     * (which may be updated during the lifetime of the cursor)
     * will not be returned. Not compatible with order/sort and hint.
     */
    Query enableSnapshotMode();

    /**
     * Disable snapshot mode (default mode). This will be faster
     * but changes made during the cursor may cause duplicates.
     */
    Query disableSnapshotMode();

    /**
     * Set the read preference for this query
     */
    Query readPreference(ReadPreference readPreference);

    /**
     * Disables cursor timeout on server.
     */
    Query disableTimeout();

    /**
     * Enables cursor timeout on server.
     */
    Query enableTimeout();

}
