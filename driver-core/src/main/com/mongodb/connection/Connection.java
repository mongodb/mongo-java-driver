/*
 * Copyright 2013-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcernResult;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.binding.ReferenceCounted;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;


/**
 * A synchronous connection to a MongoDB server with blocking operations.
 *
 * <p> Implementations of this class are thread safe.  </p>
 *
 * <p> This interface is not stable. While methods will not be removed, new ones may be added. </p>
 *
 * @since 3.0
 */
@ThreadSafe
public interface Connection extends ReferenceCounted {

    @Override
    Connection retain();

    /**
     * Gets the description of the connection.
     *
     * @return the connection description
     */
    ConnectionDescription getDescription();

    /**
     * Insert the documents using the insert wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param insertRequest the insert request
     * @return the write concern result
     */
    WriteConcernResult insert(MongoNamespace namespace, boolean ordered, InsertRequest insertRequest);

    /**
     * Update the documents using the update wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param updateRequest the update request
     * @return the write concern result
     */
    WriteConcernResult update(MongoNamespace namespace, boolean ordered, UpdateRequest updateRequest);

    /**
     * Delete the documents using the delete wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param deleteRequest the delete request
     * @return the write concern result
     */
    WriteConcernResult delete(MongoNamespace namespace, boolean ordered, DeleteRequest deleteRequest);

    /**
     * Execute the command.
     *
     * @param database             the database to execute the command in
     * @param command              the command document
     * @param slaveOk              whether the command can run on a secondary
     * @param fieldNameValidator   the field name validator for the command document
     * @param commandResultDecoder the decoder for the result
     * @param <T>                  the type of the result
     * @return the command result
     * @deprecated Prefer {@link #command(String, BsonDocument, FieldNameValidator, ReadPreference, Decoder, SessionContext)}
     */
    @Deprecated
    <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator,
                  Decoder<T> commandResultDecoder);

    /**
     * Execute the command.
     *
     * @param <T>                  the type of the result
     * @param database             the database to execute the command in
     * @param command              the command document
     * @param fieldNameValidator   the field name validator for the command document
     * @param readPreference       the read preference that was applied to get this connection, or null if this is a write operation
     * @param commandResultDecoder the decoder for the result
     * @param sessionContext       the session context
     * @return the command result
     * @since 3.6
     */
    <T> T command(String database, BsonDocument command, FieldNameValidator fieldNameValidator, ReadPreference readPreference,
                  Decoder<T> commandResultDecoder, SessionContext sessionContext);

    /**
     * Executes the command, consuming as much of the {@code SplittablePayload} as possible.
     *
     * @param <T>                       the type of the result
     * @param database                  the database to execute the command in
     * @param command                   the command document
     * @param commandFieldNameValidator the field name validator for the command document
     * @param readPreference            the read preference that was applied to get this connection, or null if this is a write operation
     * @param commandResultDecoder      the decoder for the result
     * @param sessionContext            the session context
     * @param responseExpected          true if a response from the server is expected
     * @param payload                   the splittable payload to incorporate with the command
     * @param payloadFieldNameValidator the field name validator for the payload documents
     * @return the command result
     * @since 3.6
     */
    <T> T command(String database, BsonDocument command, FieldNameValidator commandFieldNameValidator, ReadPreference readPreference,
                  Decoder<T> commandResultDecoder, SessionContext sessionContext, boolean responseExpected,
                  SplittablePayload payload, FieldNameValidator payloadFieldNameValidator);

    /**
     * Execute the query.
     *
     * @param namespace       the namespace to query
     * @param queryDocument   the query document
     * @param fields          the field to include or exclude
     * @param numberToReturn  the number of documents to return
     * @param skip            the number of documents to skip
     * @param slaveOk         whether the query can run on a secondary
     * @param tailableCursor  whether to return a tailable cursor
     * @param awaitData       whether a tailable cursor should wait before returning if no documents are available
     * @param noCursorTimeout whether the cursor should not timeout
     * @param partial         whether partial results from sharded clusters are acceptable
     * @param oplogReplay     whether to replay the oplog
     * @param resultDecoder   the decoder for the query result documents
     * @param <T>             the query result document type
     * @return the query results
     * @deprecated Replaced by {@link #query(MongoNamespace, BsonDocument, BsonDocument, int, int, int, boolean, boolean, boolean,
     * boolean, boolean, boolean, Decoder)}
     */
    @Deprecated
    <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                             int numberToReturn, int skip,
                             boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout,
                             boolean partial, boolean oplogReplay,
                             Decoder<T> resultDecoder);

    /**
     * Execute the query.
     *
     * @param namespace       the namespace to query
     * @param queryDocument   the query document
     * @param fields          the field to include or exclude
     * @param skip            the number of documents to skip
     * @param limit           the maximum number of documents to return in all batches
     * @param batchSize       the maximum number of documents to return in this batch
     * @param slaveOk         whether the query can run on a secondary
     * @param tailableCursor  whether to return a tailable cursor
     * @param awaitData       whether a tailable cursor should wait before returning if no documents are available
     * @param noCursorTimeout whether the cursor should not timeout
     * @param partial         whether partial results from sharded clusters are acceptable
     * @param oplogReplay     whether to replay the oplog
     * @param resultDecoder   the decoder for the query result documents
     * @param <T>             the query result document type
     * @return the query results
     *
     * @since 3.1
     */
    <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                             int skip, int limit, int batchSize,
                             boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout,
                             boolean partial, boolean oplogReplay,
                             Decoder<T> resultDecoder);

    /**
     * Get more result documents from a cursor.
     *
     * @param namespace      the namespace to get more documents from
     * @param cursorId       the cursor id
     * @param numberToReturn the number of documents to return
     * @param resultDecoder  the decoder for the query results
     * @param <T>            the type of the query result documents
     * @return the query results
     */
    <T> QueryResult<T> getMore(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder);

    /**
     * Kills the given list of cursors.
     *
     * @param cursors the cursors
     * @deprecated Replaced by {@link #killCursor(MongoNamespace, List)}
     */
    @Deprecated
    void killCursor(List<Long> cursors);

    /**
     * Kills the given list of cursors.
     *
     * @param namespace the namespace to in which the cursors live
     * @param cursors   the cursors
     */
    void killCursor(MongoNamespace namespace, List<Long> cursors);
}
