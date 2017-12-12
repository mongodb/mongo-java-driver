/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.bson.BSONObject;

/**
 * This interface adds some specific behaviour to {@link org.bson.BSONObject} for MongoDB documents.
 *
 * @mongodb.driver.manual core/document/ Documents
 */
public interface DBObject extends BSONObject {
    /**
     * If this object was retrieved with only some fields (using a field filter) this method will be called to mark it as such.
     */
    void markAsPartialObject();

    /**
     * Whether {@link #markAsPartialObject} was ever called only matters if you are going to upsert and do not want to risk losing fields.
     *
     * @return true if this has been marked as a partial object
     */
    boolean isPartialObject();
}
