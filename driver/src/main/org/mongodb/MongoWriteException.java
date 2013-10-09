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

package org.mongodb;

import static java.lang.String.format;

/**
 * Exception indicating a failure to successfully complete a write operation according to the WriteConcern used for the operation.
 */
public class MongoWriteException extends MongoServerException {
    private static final long serialVersionUID = -1139302724723542251L;

    private final WriteResult writeResult;

    /**
     * Construct a new instance with the write result returned from checking the success of the write operation.
     *
     * @param writeResult the write result
     */
    public MongoWriteException(final WriteResult writeResult) {
        super(format("Write failed with error code %d and error message '%s'", writeResult.getErrorCode(),
                     writeResult.getErrorMessage()), writeResult.getCommandResult().getAddress());
        this.writeResult = writeResult;
    }

    @Override
    public int getErrorCode() {
        return writeResult.getCommandResult().getErrorCode();
    }

    @Override
    public String getErrorMessage() {
        return writeResult.getErrorMessage();
    }

    public WriteResult getWriteResult() {
        return writeResult;
    }
}
