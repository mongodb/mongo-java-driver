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

package org.mongodb;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.mongodb.connection.ServerAddress;

public class CommandResult<T> {
    private final ServerAddress address;
    private final BsonDocument response;
    private final long elapsedNanoseconds;
    private final Decoder<T> decoder;
    private T decodedResponse;

    public CommandResult(final ServerAddress address, final BsonDocument response, final long elapsedNanoseconds,
                         final Decoder<T> decoder) {
        this.address = address;
        this.response = response;
        this.elapsedNanoseconds = elapsedNanoseconds;
        this.decoder = decoder;
    }

    public CommandResult(final CommandResult<T> baseResult) {
        this.address = baseResult.address;
        this.response = baseResult.response;
        this.elapsedNanoseconds = baseResult.elapsedNanoseconds;
        this.decoder = baseResult.decoder;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public T getResponse() {
        if (decodedResponse == null) {
            decodedResponse = decoder.decode(new BsonDocumentReader(response), DecoderContext.builder().build());
        }
        return decodedResponse;
    }

    public BsonDocument getRawResponse() {
        return response;
    }

    /**
     * Return true if the command completed successfully.
     *
     * @return true if the command completed successfully, false otherwise.
     */
    public boolean isOk() {
        BsonValue okValue = response.get("ok");
        if (okValue instanceof BsonBoolean) {
            return ((BsonBoolean) okValue).getValue();
        } else if (okValue instanceof BsonInt32) {
            return ((BsonInt32) okValue).getValue() == 1;
        } else if (okValue instanceof BsonInt64) {
            return ((BsonInt64) okValue).getValue() == 1L;
        } else if (okValue instanceof BsonDouble) {
            return ((BsonDouble) okValue).getValue() == 1.0;
        } else {
            return false;
        }
    }

    public int getErrorCode() {
        if (response.containsKey("code")) {
            return ((BsonInt32) response.get("code")).getValue();
        } else {
            return -1;
        }
    }

    public String getErrorMessage() {
        if (response.containsKey("errmsg")) {
            return ((BsonString) response.get("errmsg")).getValue();
        } else {
            return null;
        }
    }

    public long getElapsedNanoseconds() {
        return elapsedNanoseconds;
    }
}
