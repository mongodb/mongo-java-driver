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

package com.mongodb.internal.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoQueryException;
import com.mongodb.ServerCursor;

final class QueryHelper {
    static MongoQueryException translateCommandException(final MongoCommandException commandException, final ServerCursor cursor) {
        if (commandException.getErrorCode() == 43) {
            return new MongoCursorNotFoundException(cursor.getId(), cursor.getAddress());
        } else {
            return new MongoQueryException(commandException);
        }
    }

    private QueryHelper() {
    }
}
