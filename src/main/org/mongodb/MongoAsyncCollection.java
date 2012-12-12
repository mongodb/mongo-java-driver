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

import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;

import java.util.concurrent.Future;

public interface MongoAsyncCollection<T> extends MongoCollectionBase<T> {
    MongoAsyncCursor<T> find(MongoFind find);

    Future<T> findOne(MongoFind find);  // TODO: MongoFind has too many options for findOne

    Future<Long> count();

    Future<Long> count(MongoFind find);  // TODO: MongoFind has too many options for count

    Future<T> findAndUpdate(MongoFindAndUpdate findAndUpdate);

    Future<T> findAndReplace(MongoFindAndReplace<T> findAndReplace);

    Future<T> findAndRemove(MongoFindAndRemove findAndRemove);

    Future<InsertResult> insert(MongoInsert<T> insert);

    Future<RemoveResult> remove(MongoRemove remove);
}
