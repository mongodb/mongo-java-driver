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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.WriteConcernResult;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;


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
     * @param requestContext the request context
     * @return the write concern result
     */
    WriteConcernResult insert(MongoNamespace namespace, boolean ordered, InsertRequest insertRequest, RequestContext requestContext);

    /**
     * Update the documents using the update wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param updateRequest the update request
     * @param requestContext the request context
     * @return the write concern result
     */
    WriteConcernResult update(MongoNamespace namespace, boolean ordered, UpdateRequest updateRequest, RequestContext requestContext);

    /**
     * Delete the documents using the delete wire protocol and apply the write concern.
     *
     * @param namespace    the namespace
     * @param ordered      whether the writes are ordered
     * @param deleteRequest the delete request
     * @param requestContext the request context
     * @return the write concern result
     */
    WriteConcernResult delete(MongoNamespace namespace, boolean ordered, DeleteRequest deleteRequest, RequestContext requestContext);

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
     * @param requestContext       the request context
     * @return the command result
     * @since 3.6
     */
    <T> T command(String database, BsonDocument command, FieldNameValidator fieldNameValidator, ReadPreference readPreference,
            Decoder<T> commandResultDecoder, SessionContext sessionContext, ServerApi serverApi, RequestContext requestContext);

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
     * @param requestContext            the request context
     * @param responseExpected          true if a response from the server is expected
     * @param payload                   the splittable payload to incorporate with the command
     * @param payloadFieldNameValidator the field name validator for the payload documents
     * @return the command result
     * @since 3.6
     */
    <T> T command(String database, BsonDocument command, FieldNameValidator commandFieldNameValidator, ReadPreference readPreference,
            Decoder<T> commandResultDecoder, SessionContext sessionContext, ServerApi serverApi, RequestContext requestContext,
            boolean responseExpected, SplittablePayload payload, FieldNameValidator payloadFieldNameValidator);


    enum PinningMode {
        CURSOR,
        TRANSACTION
    }

    /**
     * Marks the connection as pinned. Used so that any pool timeout exceptions can include information about the pinned connections,
     * and what they are pinned to.
     */
     void markAsPinned(PinningMode pinningMode);
}
