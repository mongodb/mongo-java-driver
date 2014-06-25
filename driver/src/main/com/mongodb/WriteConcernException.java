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

package com.mongodb;

import org.bson.BsonBoolean;
import org.bson.BsonInt32;
import org.mongodb.MongoWriteException;

/**
 * An exception representing an error reported due to a write failure.
 */
public class WriteConcernException extends MongoException {
    private static final long serialVersionUID = 841056799207039974L;

    private final WriteResult writeResult;

    WriteConcernException(final int code, final String message, final WriteResult writeResult) {
        super(code, message);
        this.writeResult = writeResult;
    }

    WriteConcernException(final MongoWriteException e) {
        this(e.getErrorCode(), e.getErrorMessage(), createWriteResult(e));
    }

    private static WriteResult createWriteResult(final MongoWriteException e) {
        return new WriteResult(e.getCommandResult().getResponse().getNumber("n", new BsonInt32(0)).intValue(),
                               e.getCommandResult().getResponse().getBoolean("updatedExisting", BsonBoolean.FALSE).getValue(),
                               e.getCommandResult().getResponse().get("upserted"));
    }

    /**
     * Gets the write result.
     *
     * @return the write result
     */
    public WriteResult getWriteResult() {
        return writeResult;
    }
}
