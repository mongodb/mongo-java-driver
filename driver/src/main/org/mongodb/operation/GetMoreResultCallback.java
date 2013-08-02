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

package org.mongodb.operation;

import org.mongodb.Decoder;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.QueryResult;
import org.mongodb.operation.protocol.ReplyMessage;

class GetMoreResultCallback<T> extends ResponseCallback {
    private final SingleResultCallback<QueryResult<T>> callback;
    private final Decoder<T> decoder;
    private final long cursorId;

    public GetMoreResultCallback(final SingleResultCallback<QueryResult<T>> callback, final Decoder<T> decoder,
                                 final long cursorId, final AsyncServerConnection connection,
                                 final long requestId) {
        super(connection, requestId);
        this.callback = callback;
        this.decoder = decoder;
        this.cursorId = cursorId;
    }

    @Override
    protected void callCallback(final ResponseBuffers responseBuffers, final MongoException e) {
        QueryResult<T> result = null;
        MongoException exceptionResult = null;
        try {
            if (e != null) {
                throw e;
            }
            else if (responseBuffers.getReplyHeader().isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(cursorId, getConnection().getServerAddress()));
            }
            else {
                result = new QueryResult<T>(new ReplyMessage<T>(responseBuffers, decoder, getRequestId()),
                        getConnection().getServerAddress());
            }
        } catch (MongoException me) {
            exceptionResult = me;
        } catch (Throwable t) {
            exceptionResult = new MongoInternalException("Internal exception", t);
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
        callback.onResult(result, exceptionResult);
    }
}
