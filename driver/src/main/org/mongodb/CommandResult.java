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

import com.mongodb.ServerAddress;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public class CommandResult {
    private final ServerAddress address;
    private final BsonDocument response;

    public CommandResult(final ServerAddress address, final BsonDocument response) {
        this.address = address;
        this.response = response;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public BsonDocument getResponse() {
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
        if (getResponse().containsKey("code")) {
            return ((BsonInt32) getResponse().get("code")).getValue();
        } else {
            return -1;
        }
    }

    public String getErrorMessage() {
        if (getResponse().containsKey("errmsg")) {
            return ((BsonString) getResponse().get("errmsg")).getValue();
        } else {
            return null;
        }
    }
}
