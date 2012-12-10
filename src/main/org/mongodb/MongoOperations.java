/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;

import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;

public interface MongoOperations {

    // TODO: how to deal with custom serialization for commands like findAndModify
    // TODO: needs to take a MongoQuery or equivalent
    // TODO: should this really be a separate call from query?
    // TODO: how to custom serialize a piece of the command?
    MongoDocument executeCommand(String database, MongoCommandOperation commandOperation);

    <T> QueryResult<T> query(final MongoNamespace namespace, MongoFind find, Class<T> clazz);

    // TODO: needs a ServerAddress or doesn't make sense for some MongoClient implementations
    <T> GetMoreResult<T> getMore(final MongoNamespace namespace, GetMore getMore, Class<T> clazz);

    // TODO: needs a ServerAddress or doesn't make sense for some MongoClient implementations
    void killCursors(MongoKillCursor killCursor);

    <T> InsertResult insert(MongoNamespace namespace, MongoInsert<T> insert, Class<T> clazz);

    // TODO: Need to handle update where you have to custom serialize the update document
    UpdateResult update(final MongoNamespace namespace, MongoUpdate update);

    RemoveResult delete(final MongoNamespace namespace, MongoRemove remove);
}
