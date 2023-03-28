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
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.binding.BindingContext;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;


/**
 * A synchronous connection to a MongoDB server with blocking operations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    @Nullable
    <T> T command(String database, BsonDocument command, FieldNameValidator fieldNameValidator, @Nullable ReadPreference readPreference,
            Decoder<T> commandResultDecoder, BindingContext context);

    @Nullable
    <T> T command(String database, BsonDocument command, FieldNameValidator commandFieldNameValidator,
            @Nullable ReadPreference readPreference, Decoder<T> commandResultDecoder, BindingContext context,
            boolean responseExpected, @Nullable SplittablePayload payload, @Nullable FieldNameValidator payloadFieldNameValidator);


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
