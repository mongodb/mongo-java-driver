/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb;

import java.util.List;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course of normal use of a
 * collection.
 */
public interface CollectionAdministration {
    /**
     * @param index all the details of the index to add
     * @see Index
     * @see <a href="http://docs.mongodb.org/manual/reference/javascript/#db.collection.ensureIndex">ensureIndex</a>
     */
    void ensureIndex(Index index);

    /**
     * @return a MongoCollection containing all the indexes on this collection
     */
    //TODO: shouldn't this be a list of Index objects?
    List<Document> getIndexes();

    /**
     * @return true is this is a capped collection
     */
    boolean isCapped();

    /**
     * Return statistics document for this collection, from collstats command
     *
     * @return statistics document
     */
    Document getStatistics();

    void drop();

    void dropIndex(Index index);

    void dropIndexes();

}
