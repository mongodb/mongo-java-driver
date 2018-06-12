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

import com.mongodb.MongoException;

public class MongoWriteConcernWithResponseException extends MongoException {
    private static final long serialVersionUID = 1707360842648550287L;
    private final MongoException cause;
    private final Object response;

    public MongoWriteConcernWithResponseException(final MongoException exception, final Object response) {
        super(exception.getCode(), exception.getMessage(), exception);
        this.cause = exception;
        this.response = response;
    }

    @Override
    public MongoException getCause() {
        return cause;
    }

    public Object getResponse() {
        return response;
    }
}
