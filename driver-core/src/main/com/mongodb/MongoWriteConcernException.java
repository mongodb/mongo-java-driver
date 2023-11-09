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

package com.mongodb;

import com.mongodb.bulk.WriteConcernError;
import com.mongodb.lang.Nullable;

import java.util.Collections;
import java.util.Set;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An exception indicating a failure to apply the write concern to the requested write operation
 *
 * @see com.mongodb.WriteConcern
 *
 * @since 3.0
 * @serial exclude
 */
public class MongoWriteConcernException extends MongoServerException {
    private static final long serialVersionUID = 4577579466973523211L;

    private final WriteConcernError writeConcernError;
    private final WriteConcernResult writeConcernResult;

    /**
     * Construct an instance.
     *
     * @param writeConcernError the non-null write concern error
     * @param serverAddress the non-null server address
     * @deprecated Prefer {@link MongoWriteConcernException(WriteConcernError, WriteConcernResult, ServerAddress, Set)}
     */
    @Deprecated
    public MongoWriteConcernException(final WriteConcernError writeConcernError, final ServerAddress serverAddress) {
        this(writeConcernError, null, serverAddress, Collections.emptySet());
    }

    /**
     * Construct an instance.
     *
     * @param writeConcernError the non-null write concern error
     * @param writeConcernResult the write result
     * @param serverAddress     the non-null server address
     * @since 3.2
     * @deprecated Prefer {@link MongoWriteConcernException(WriteConcernError, WriteConcernResult, ServerAddress, Set)}
     */
    @Deprecated
    public MongoWriteConcernException(final WriteConcernError writeConcernError, @Nullable final WriteConcernResult writeConcernResult,
                                      final ServerAddress serverAddress) {
        this(writeConcernError, writeConcernResult, serverAddress, Collections.emptySet());
    }

    /**
     * Construct an instance.
     *
     * @param writeConcernError the non-null write concern error
     * @param writeConcernResult the write result
     * @param serverAddress     the non-null server address
     * @param errorLabels       the server errorLabels
     * @since 5.0
     */
    public MongoWriteConcernException(final WriteConcernError writeConcernError, @Nullable final WriteConcernResult writeConcernResult,
            final ServerAddress serverAddress, final Set<String> errorLabels) {
        super(writeConcernError.getCode(), writeConcernError.getMessage(), serverAddress);
        this.writeConcernResult = writeConcernResult;
        this.writeConcernError = notNull("writeConcernError", writeConcernError);
        addLabels(errorLabels);
    }


    /**
     * Gets the write concern error.
     *
     * @return the write concern error, which may not be null
     */
    public WriteConcernError getWriteConcernError() {
        return writeConcernError;
    }

    /**
     * Gets the write result.
     *
     * @return the write result
     *
     * @since 3.2
     */
    public WriteConcernResult getWriteResult() {
        return writeConcernResult;
    }
}
