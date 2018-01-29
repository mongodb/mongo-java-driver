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

package com.mongodb;

/**
 * Factory for creating concrete implementations of DBCallback.
 */
public interface DBCallbackFactory {

    /**
     * Creates a DBCallback for the given collection.
     *
     * @param collection a DBCollection for the DBCallback
     * @return a new DBCallback that operates on the collection.
     */
    DBCallback create(DBCollection collection);

}
