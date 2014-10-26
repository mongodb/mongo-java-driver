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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.ReferenceCounted;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.GetMore;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.UpdateRequest;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.List;

/**
 * A connection to a MongoDB server with blocking and non-blocking operations. <p> Implementations of this class are thread safe.  At a
 * minimum, they must support concurrent calls to sendMessage and receiveMessage, but at most one of each.  But multiple concurrent calls to
 * either sendMessage or receiveMessage may block. </p>
 *
 * @since 3.0
 */
@ThreadSafe
public interface Connection extends BufferProvider, ReferenceCounted {

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
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @return the write concern result
     */
    WriteConcernResult insert(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts);

    /**
     * Insert the documents using the insert wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @return the write concern result future
     */
    MongoFuture<WriteConcernResult> insertAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                List<InsertRequest> inserts);

    /**
     * Update the documents using the update wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @return the write concern result
     */
    WriteConcernResult update(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                              List<UpdateRequest> updates);

    /**
     * Update the documents using the update wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @return the write concern result future
     */
    MongoFuture<WriteConcernResult> updateAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                List<UpdateRequest> updates);

    /**
     * Delete the documents using the delete wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @return the write concern result
     */
    WriteConcernResult delete(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                              List<DeleteRequest> deletes);

    /**
     * Delete the documents using the delete wire protocol and apply the write concern asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @return the write concern result future
     */
    MongoFuture<WriteConcernResult> deleteAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                List<DeleteRequest> deletes);


    /**
     * Insert the documents using the insert command.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @return the bulk write result
     */
    BulkWriteResult insertCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<InsertRequest> inserts);


    /**
     * Insert the documents using the insert command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param inserts      the inserts
     * @return the bulk write result future
     */
    MongoFuture<BulkWriteResult> insertCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                    List<InsertRequest> inserts);

    /**
     * Update the documents using the update command.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @return the bulk write result
     */
    BulkWriteResult updateCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<UpdateRequest> updates);

    /**
     * Update the documents using the update command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param updates      the updates
     * @return the bulk write result future
     */
    MongoFuture<BulkWriteResult> updateCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                    List<UpdateRequest> updates);

    /**
     * Delete the documents using the delete command.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @return the bulk write result
     */
    BulkWriteResult deleteCommand(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern, List<DeleteRequest> deletes);

    /**
     * Delete the documents using the delete command asynchronously.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes      the deletes
     * @return the bulk write result future
     */
    MongoFuture<BulkWriteResult> deleteCommandAsync(MongoNamespace namespace, boolean ordered, WriteConcern writeConcern,
                                                    List<DeleteRequest> deletes);

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
     */
    <T> T command(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator,
                  Decoder<T> commandResultDecoder);

    /**
     * Execute the command asynchronously.
     *
     * @param database             the database to execute the command in
     * @param command              the command document
     * @param slaveOk              whether the command can run on a secondary
     * @param fieldNameValidator   the field name validator for the command document
     * @param commandResultDecoder the decoder for the result
     * @param <T>                  the type of the result
     * @return the command result future
     */
    <T> MongoFuture<T> commandAsync(String database, BsonDocument command, boolean slaveOk, FieldNameValidator fieldNameValidator,
                                    Decoder<T> commandResultDecoder);

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
     * @param exhaust         whether all documents should be retrieved immediately
     * @param partial         whether partial results from sharded clusters are acceptable
     * @param oplogReplay     whether to replay the oplog
     * @param resultDecoder   the decoder for the query result documents
     * @param <T>                  the type of the result
     * @return the query results
     */
    <T> QueryResult<T> query(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                             int numberToReturn, int skip,
                             boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout,
                             boolean exhaust, boolean partial, boolean oplogReplay,
                             Decoder<T> resultDecoder);

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
     * @param exhaust         whether all documents should be retrieved immediately
     * @param partial         whether partial results from sharded clusters are acceptable
     * @param oplogReplay     whether to replay the oplog
     * @param resultDecoder   the decoder for the query result documents
     * @param <T>                  the type of the result
     * @return the query results
     */
    <T> MongoFuture<QueryResult<T>> queryAsync(MongoNamespace namespace, BsonDocument queryDocument, BsonDocument fields,
                                               int numberToReturn, int skip,
                                               boolean slaveOk, boolean tailableCursor, boolean awaitData, boolean noCursorTimeout,
                                               boolean exhaust, boolean partial, boolean oplogReplay,
                                               Decoder<T> resultDecoder);

    /**
     * Get more result documents from a cursor.
     *
     * @param namespace     the namespace to get more documents from
     * @param getMore       the cursor
     * @param resultDecoder the decoder for the query result documents
     * @param <T>           the type of the query result documents
     * @return the query results
     */
    <T> QueryResult<T> getMore(MongoNamespace namespace, GetMore getMore, Decoder<T> resultDecoder);

    /**
     * Get more result documents from a cursor asynchronously.
     *
     * @param namespace     the namespace to get more documents from
     * @param getMore       the cursor
     * @param resultDecoder the decoder for the query result documents
     * @param <T>           the type of the query result documents
     * @return the query results
     */
    <T> MongoFuture<QueryResult<T>> getMoreAsync(MongoNamespace namespace, GetMore getMore, Decoder<T> resultDecoder);

    /**
     * Get more result documents from an exhaust cursor.
     *
     * @param resultDecoder the decoder for the query result documents
     * @param responseTo    the id that the next reply is a response to
     * @param <T>           the type of the query result documents
     * @return the query results
     */
    <T> QueryResult<T> getMoreReceive(Decoder<T> resultDecoder, int responseTo);

    /**
     * Get more result documents from an exhaust cursor asynchronously.
     *
     * @param resultDecoder the decoder for the query result documents
     * @param responseTo    the id that the next reply is a response to
     * @param <T>           the type of the query result documents
     * @return the query results
     */
    <T> MongoFuture<QueryResult<T>> getMoreReceiveAsync(Decoder<T> resultDecoder, int responseTo);

    /**
     * Discard all remaining results from an exhaust cursor.
     *
     * @param cursorId   the cursor
     * @param responseTo the id that the next reply is a response to
     */
    void getMoreDiscard(long cursorId, int responseTo);

    /**
     * Discard all remaining results from an exhaust cursor asynchronously.
     *
     * @param cursorId   the cursor
     * @param responseTo the id that the next reply is a response to
     * @return the future
     */
    MongoFuture<Void> getMoreDiscardAsync(long cursorId, int responseTo);

    /**
     * Kills the given list of cursors.
     *
     * @param cursors the cursors
     */
    void killCursor(List<ServerCursor> cursors);

    /**
     * Asynchronously Kills the given list of cursors.
     *
     * @param cursors the cursors
     * @return the future
     */
    MongoFuture<Void> killCursorAsync(List<ServerCursor> cursors);

    /**
     * Send a message to the server. The connection may not make any attempt to validate the integrity of the message. <p> This method
     * blocks until all bytes have been written.  This method is not thread safe: only one thread at a time can have an active call to this
     * method. </p>
     *
     * @param byteBuffers   the list of byte buffers to send.
     * @param lastRequestId the request id of the last message in byteBuffers
     */
    void sendMessage(List<ByteBuf> byteBuffers, int lastRequestId);

    /**
     * Receive a response to a sent message from the server. <p> This method blocks until the entire message has been read. This method is
     * not thread safe: only one thread at a time can have an active call to this method. </p>
     *
     * @param responseTo the expected responseTo of the received message
     * @return the response
     */
    ResponseBuffers receiveMessage(int responseTo);

    /**
     * Asynchronously send a message to the server. The connection may not make any attempt to validate the integrity of the message.
     *
     * @param byteBuffers   the list of byte buffers to send
     * @param lastRequestId the request id of the last message in byteBuffers
     * @param callback      the callback to invoke on completion
     */
    void sendMessageAsync(List<ByteBuf> byteBuffers, int lastRequestId, SingleResultCallback<Void> callback);

    /**
     * Asynchronously receive a response to a sent message from the server.
     *
     * @param responseTo the request id that this message is a response to
     * @param callback   the callback to invoke on completion
     */
    void receiveMessageAsync(int responseTo, SingleResultCallback<ResponseBuffers> callback);

    /**
     * Gets the server address of this connection
     *
     * @return a ServerAddress for this connection.
     */
    ServerAddress getServerAddress();

    /**
     * Gets the id of the connection.  If possible, this id will correlate with the connection id that the server puts in its log messages.
     *
     * @return the id
     */
    String getId();

}
