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

import java.util.List;

public interface MongoWritableView<T> {
    WriteResult insert(T document);

    WriteResult insert(List<T> document);

    WriteResult save(T document);

    WriteResult remove();

    WriteResult update(Document updateOperations);

    WriteResult update(ConvertibleToDocument updateOperations);

    WriteResult replace(T replacement);

    T updateOneAndGet(Document updateOperations);

    T updateOneAndGet(ConvertibleToDocument updateOperations);

    T replaceOneAndGet(T replacement);

    T updateOneAndGetOriginal(Document updateOperations);

    T updateOneAndGetOriginal(ConvertibleToDocument updateOperations);

    T replaceOneAndGetOriginal(T replacement);

    T removeOneAndGet();
}
