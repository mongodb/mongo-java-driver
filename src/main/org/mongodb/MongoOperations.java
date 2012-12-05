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

public interface MongoOperations {
    CommandResult executeCommand(String database, MongoDocument command);

    <T> QueryResult<T> query(final MongoCollectionName namespace, final MongoDocument query, Class<T> clazz);

    <T> GetMoreResult<T> getMore(final MongoCollectionName namespace, long cursorId, Class<T> clazz);

    void killCursors(long cursorId, long... cursorIds);

    <T> InsertResult insert(final MongoCollectionName namespace, T doc, WriteConcern writeConcern);

    UpdateResult update(final MongoCollectionName namespace, MongoDocument query,
                        MongoDocument updateOperations, WriteConcern writeConcern);

    DeleteResult delete(final MongoCollectionName namespace, MongoDocument query,
                        WriteConcern writeConcern);
}
