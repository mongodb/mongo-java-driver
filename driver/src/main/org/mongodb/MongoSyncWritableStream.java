/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.operation.MongoUpdateOperations;
import org.mongodb.result.WriteResult;

public interface MongoSyncWritableStream<T> {
    WriteResult insert(T document);

    WriteResult insert(Iterable<T> document);

    WriteResult save(T document);

    WriteResult remove();

    WriteResult modify(MongoUpdateOperations updateOperations);

    WriteResult modifyOrInsert(MongoUpdateOperations updateOperations);                // TODO: name

    WriteResult replace(T replacement);

    WriteResult replaceOrInsert(T replacement);                                        // TODO: name

    T modifyAndGet(MongoUpdateOperations updateOperations, Get beforeOrAfter);

    T modifyOrInsertAndGet(MongoUpdateOperations updateOperations, Get beforeOrAfter);  // TODO: name

    T replaceAndGet(T replacement, Get beforeOrAfter);

    T replaceOrInsertAndGet(T replacement, Get beforeOrAfter);                          // TODO: name

    T removeAndGet();
}
