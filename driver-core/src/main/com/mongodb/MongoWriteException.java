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

import java.util.Collections;
import java.util.Set;

/**
 * An exception indicating the failure of a write operation.
 *
 * @since 3.0
 * @serial exclude
 */
public class MongoWriteException extends MongoServerException {

    private static final long serialVersionUID = -1906795074458258147L;

    private final WriteError error;

    /**
     * Construct an instance
     * @param error the error
     * @param serverAddress the server address
     * @deprecated Prefer {@link MongoWriteException(WriteError, ServerAddress, Set)}
     */
    @Deprecated
    public MongoWriteException(final WriteError error, final ServerAddress serverAddress) {
        this(error, serverAddress, Collections.emptySet());
    }

    /**
     * Construct an instance
     * @param error the error
     * @param serverAddress the server address
     * @param errorLabels the server errorLabels
     * @since 5.0
     */
    public MongoWriteException(final WriteError error, final ServerAddress serverAddress, final Set<String> errorLabels) {
        super(error.getCode(), "Write operation error on server " + serverAddress + ". Write error: " + error + ".", serverAddress);
        this.error = error;
        addLabels(errorLabels);
    }

    /**
     * Gets the error.
     *
     * @return the error
     */
    public WriteError getError() {
        return error;
    }
}
