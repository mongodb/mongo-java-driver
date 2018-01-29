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

package com.mongodb.connection;

import org.bson.BsonDocument;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.SplittablePayload.Type.INSERT;
import static com.mongodb.connection.SplittablePayload.Type.REPLACE;
import static com.mongodb.connection.SplittablePayload.Type.UPDATE;

/**
 * A Splittable payload for write commands.
 *
 * <p>The command will consume as much of the payload as possible. The {@link #hasAnotherSplit()} method will return true if there is
 * another split to consume, {@link #getNextSplit} method will return the next SplittablePayload.</p>
 *
 * @see com.mongodb.connection.Connection#command(String, org.bson.BsonDocument, org.bson.FieldNameValidator, com.mongodb.ReadPreference,
 * org.bson.codecs.Decoder, com.mongodb.session.SessionContext, boolean, com.mongodb.connection.SplittablePayload,
 * org.bson.FieldNameValidator)
 * @see com.mongodb.connection.AsyncConnection#commandAsync(String, org.bson.BsonDocument, org.bson.FieldNameValidator,
 * com.mongodb.ReadPreference, org.bson.codecs.Decoder, com.mongodb.session.SessionContext, boolean,
 * com.mongodb.connection.SplittablePayload, org.bson.FieldNameValidator, com.mongodb.async.SingleResultCallback)
 * @since 3.6
 */
public final class SplittablePayload {
    private final Type payloadType;
    private final List<BsonDocument> payload;
    private int position = 0;

    /**
     * The type of the payload.
     */
    public enum Type {
        /**
         * An insert.
         */
        INSERT,

        /**
         * An update that uses update operators.
         */
        UPDATE,

        /**
         * An update that replaces the existing document.
         */
        REPLACE,

        /**
         * A delete.
         */
        DELETE
    }

    /**
     * Create a new instance
     *
     * @param payloadType the payload type
     * @param payload the payload
     */
    public SplittablePayload(final Type payloadType, final List<BsonDocument> payload) {
        this.payloadType = notNull("batchType", payloadType);
        this.payload = notNull("payload", payload);
    }

    /**
     * @return the payload type
     */
    public Type getPayloadType() {
        return payloadType;
    }

    /**
     * @return the payload name
     */
    public String getPayloadName() {
        if (payloadType == INSERT) {
            return "documents";
        } else if (payloadType == UPDATE || payloadType == REPLACE) {
            return "updates";
        } else {
            return "deletes";
        }
    }

    /**
     * @return the payload
     */
    public List<BsonDocument> getPayload() {
        return payload;
    }

    /**
     * @return the current position in the payload
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the current position in the payload
     * @param position the position
     */
    public void setPosition(final int position) {
        this.position = position;
    }

    /**
     * @return true if there are more values after the current position
     */
    public boolean hasAnotherSplit() {
        return payload.size() > position;
    }

    /**
     * @return a new SplittablePayload containing only the values after the current position.
     */
    public SplittablePayload getNextSplit() {
        isTrue("hasAnotherSplit", hasAnotherSplit());
        List<BsonDocument> nextPayLoad = payload.subList(position, payload.size());
        return new SplittablePayload(payloadType, nextPayLoad);
    }

    /**
     * @return true if the payload is empty
     */
    public boolean isEmpty() {
        return payload.isEmpty();
    }
}
