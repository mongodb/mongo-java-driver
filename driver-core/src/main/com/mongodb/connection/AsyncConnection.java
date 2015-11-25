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
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.ReferenceCounted;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

/**
 * An asynchronous connection to a MongoDB server with non-blocking operations.
 *
 * <p> Implementations of this class are thread safe.  </p>
 *
 * <p> This interface is not stable. While methods will not be removed, new ones may be added. </p>
 *
 * @since 3.0
 */
@ThreadSafe
public interface AsyncConnection extends ReferenceCounted {

    @Override
    AsyncConnection retain();

    /**
     * Gets the description of the connection.
     *
     * @return the connection description
     */
    ConnectionDescription getDescription();

    /**
     * Insert the documents using the insert wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @param callback     the callback to be passed the write result
     */
    void insertAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                     List<InsertRequest> inserts, SingleResultCallback<WriteConcernResult> callback);

    /**
     * Update the documents using the update wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @param callback     the callback to be passed the write result
     */
    void updateAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates,
                     SingleResultCallback<WriteConcernResult> callback);

    /**
     * Delete the documents using the delete wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @param callback     the callback to be passed the write result
     */
    void deleteAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes,
                     SingleResultCallback<WriteConcernResult> callback);

    /**
     * Insert the documents using the insert command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @param callback     the callback to be passed the bulk write result
     * @deprecated Replaced by {@link AsyncConnection#insertCommandAsync(MongoNamespace, boolean, WriteConcern, Boolean, List,
     * SingleResultCallback)}
     */
    @Deprecated
    void insertCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts,
                            SingleResultCallback<BulkWriteResult> callback);

    /**
     * Insert the documents using the insert command asynchronously.
     *
     * @param namespace                the namespace
     * @param ordered                  whether the writes are ordered
     * @param writeConcern             the write concern
     * @param bypassDocumentValidation the bypassDocumentValidation flag
     * @param inserts                  the inserts
     * @param callback                 the callback to be passed the bulk write result
     * @since 3.2
     * @mongodb.driver.manual reference/command/insert/ Insert
     */
    void insertCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, Boolean bypassDocumentValidation,
                            List<InsertRequest> inserts, SingleResultCallback<BulkWriteResult> callback);

    /**
     * Update the documents using the update command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @param callback     the callback to be passed the BulkWriteResult
     * @deprecated Replaced by {@link AsyncConnection#updateCommandAsync(MongoNamespace, boolean, WriteConcern, Boolean, List,
     * SingleResultCallback)}
     */
    @Deprecated
    void updateCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates,
                            SingleResultCallback<BulkWriteResult> callback);

    /**
     * Update the documents using the update command asynchronously.
     *
     * @param namespace                the namespace
     * @param ordered                  whether the writes are ordered
     * @param writeConcern             the write concern
     * @param bypassDocumentValidation the bypassDocumentValidation flag
     * @param updates                  the updates
     * @param callback                 the callback to be passed the BulkWriteResult
     * @since 3.2
     * @mongodb.driver.manual reference/command/update/ Update
     */
    void updateCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, Boolean bypassDocumentValidation,
                            List<UpdateRequest> updates, SingleResultCallback<BulkWriteResult> callback);

    /**
     * Delete the documents using the delete command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @param callback     the callback to be passed the BulkWriteResult
     */
    void deleteCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes,
                            SingleResultCallback<BulkWriteResult> callback);

    /**
     * Execute the command asynchronously.
     *
     * @param database             the database to execute the command in
     * @param command              the command document
     * @param slaveOk              whether the command can run on a secondary
     * @param fieldNameValidator   the field name validator for the command document
     * @param commandResultDecoder the decoder for the result
     * @param callback             the callback to be passed the command result
     * @param <T>                  the type of the result
     */
    <T> void commandAsync(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator,
                          Decoder<T> commandResultDecoder, SingleResultCallback<T> callback);

    /**
     * Execute the query asynchronously.
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
     * @param callback        the callback to be passed the write result
     * @deprecated Replaced by {@link #queryAsync(MongoNamespace, BsonDocument, BsonDocument, int, int, int, boolean, boolean, boolean,
     * boolean, boolean, boolean, Decoder, SingleResultCallback)}
     */
    @Deprecated
    <T> void queryAsync(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                        int numberToReturn, int skip, boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout,
                        boolean partial, boolean oplogReplay, Decoder<T> resultDecoder, SingleResultCallback<QueryResult<T>> callback);

    /**
     * Execute the query asynchronously.
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
     * @param callback        the callback to be passed the write result
     * @since 3.1
     */
    <T> void queryAsync(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                        int skip, int limit, int batchSize, boolean slaveOk, boolean tailableCursor, boolean awaitData,
                        boolean noCursorTimeout, boolean partial, boolean oplogReplay, Decoder<T> resultDecoder,
                        SingleResultCallback<QueryResult<T>> callback);

    /**
     * Get more result documents from a cursor asynchronously.
     *
     * @param namespace      the namespace to get more documents from
     * @param cursorId       the cursor id
     * @param numberToReturn the number of documents to return
     * @param resultDecoder  the decoder for the query result documents
     * @param callback       the callback to be passed the query result
     * @param <T>            the type of the query result documents
     */
    <T> void getMoreAsync(MongoNamespace namespace, long cursorId, int numberToReturn, Decoder<T> resultDecoder,
                          SingleResultCallback<QueryResult<T>> callback);

    /**
     * Asynchronously Kills the given list of cursors.
     *
     * @param cursors  the cursors
     * @param callback the callback that is called once the cursors have been killed
     * @deprecated Replaced by {@link #killCursorAsync(MongoNamespace, List, SingleResultCallback)}
     */
    @Deprecated
    void killCursorAsync(List<Long> cursors, SingleResultCallback<Void> callback);

    /**
     * Asynchronously Kills the given list of cursors.
     *
     * @param namespace the namespace in which the cursors live
     * @param cursors   the cursors
     * @param callback  the callback that is called once the cursors have been killed
     */
    void killCursorAsync(MongoNamespace namespace, List<Long> cursors, SingleResultCallback<Void> callback);
}
