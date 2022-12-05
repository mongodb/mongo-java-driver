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
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

/**
 * An asynchronous connection to a MongoDB server with non-blocking operations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    <T> void commandAsync(String database, BsonDocument command, FieldNameValidator fieldNameValidator,
            @Nullable ReadPreference readPreference, Decoder<T> commandResultDecoder, SessionContext sessionContext,
            @Nullable ServerApi serverApi, RequestContext requestContext, SingleResultCallback<T> callback);

    <T> void commandAsync(String database, BsonDocument command, FieldNameValidator commandFieldNameValidator,
                          @Nullable ReadPreference readPreference, Decoder<T> commandResultDecoder, SessionContext sessionContext,
                          @Nullable ServerApi serverApi, RequestContext requestContext, boolean responseExpected,
                          @Nullable SplittablePayload payload, @Nullable FieldNameValidator payloadFieldNameValidator,
                          SingleResultCallback<T> callback);

    void markAsPinned(Connection.PinningMode pinningMode);
}
