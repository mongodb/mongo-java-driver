/*
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

import org.mongodb.operation.MongoUpdateOperations;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;

public interface MongoWritableStream<T> {
    MongoWritableStream<T> writeConcern(WriteConcern writeConcern);

    InsertResult insert(T document);

    InsertResult insert(Iterable<T> document);

    UpdateResult save(T document);

    RemoveResult remove();

    UpdateResult modify(MongoUpdateOperations updateOperations);

    UpdateResult modifyOrInsert(MongoUpdateOperations updateOperations);                // TODO: name

    UpdateResult replace(T replacement);

    UpdateResult replaceOrInsert(T replacement);                                        // TODO: name

    T modifyAndGet(MongoUpdateOperations updateOperations, Get beforeOrAfter);

    T modifyOrInsertAndGet(MongoUpdateOperations updateOperations, Get beforeOrAfter);  // TODO: name

    T replaceAndGet(T replacement, Get beforeOrAfter);

    T replaceOrInsertAndGet(T replacement, Get beforeOrAfter);                          // TODO: name

    T removeAndGet();
}
