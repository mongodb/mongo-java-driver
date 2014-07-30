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

package com.mongodb.client;

import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.List;

public interface MongoWritableView<T> {
    WriteResult insert(T document);

    WriteResult insert(List<T> document);

    WriteResult save(T document);

    WriteResult remove();

    WriteResult removeOne();

    WriteResult update(Document updateOperations);

    WriteResult update(ConvertibleToDocument updateOperations);

    WriteResult updateOne(Document updateOperations);

    WriteResult updateOne(ConvertibleToDocument updateOperations);

    WriteResult replace(T replacement);

    T updateOneAndGet(Document updateOperations);

    T updateOneAndGet(ConvertibleToDocument updateOperations);

    T replaceOneAndGet(T replacement);

    T getOneAndUpdate(Document updateOperations);

    T getOneAndUpdate(ConvertibleToDocument updateOperations);

    T getOneAndReplace(T replacement);

    T getOneAndRemove();
}
