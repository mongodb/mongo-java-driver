/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

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
     * Execute the command.
     *
     * @param <T>                  the type of the result
     * @param database             the database to execute the command in
     * @param command              the command document
     * @param fieldNameValidator   the field name validator for the command document
     * @param readPreference       the read preference that was applied to get this connection, or null if this is a write operation
     * @param commandResultDecoder the decoder for the result
     * @param sessionContext       the session context
     * @param serverApi            the server api, which may be null
     * @param callback             the callback to be passed the write result
     * @since 4.4
     */
    <T> void commandAsync(String database, BsonDocument command, FieldNameValidator fieldNameValidator, ReadPreference readPreference,
            Decoder<T> commandResultDecoder, SessionContext sessionContext, ServerApi serverApi, RequestContext requestContext,
            SingleResultCallback<T> callback);

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
     * @param serverApi                 the server api, which may be null
     * @param responseExpected          true if a response from the server is expected
     * @param payload                   the splittable payload to incorporate with the command
     * @param payloadFieldNameValidator the field name validator for the payload documents
     * @param callback                  the callback to be passed the write result
     * @since 3.6
     */
    <T> void commandAsync(String database, BsonDocument command, FieldNameValidator commandFieldNameValidator,
                          ReadPreference readPreference, Decoder<T> commandResultDecoder, SessionContext sessionContext,
                          ServerApi serverApi, RequestContext requestContext, boolean responseExpected, SplittablePayload payload,
                          FieldNameValidator payloadFieldNameValidator, SingleResultCallback<T> callback);

    void markAsPinned(Connection.PinningMode pinningMode);
}
