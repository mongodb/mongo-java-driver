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

package com.mongodb.bulk;

import com.mongodb.WriteError;
import org.bson.BsonDocument;

/**
 * Represents an error for an item included in a bulk write operation, e.g. a duplicate key error
 *
 * @since 3.0
 */
public class BulkWriteError extends WriteError {
    private final int index;

    /**
     * Constructs a new instance.
     *
     * @param code    the error code
     * @param message the error message
     * @param details details about the error
     * @param index   the index of the item in the bulk write operation that had this error
     */
    public BulkWriteError(final int code, final String message, final BsonDocument details, final int index) {
        super(code, message, details);
        this.index = index;
    }

    /**
     * The index of the item in the bulk write operation with this error.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BulkWriteError that = (BulkWriteError) o;

        if (index != that.index) {
            return false;
        }

        return super.equals(that);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + index;
        return result;
    }

    @Override
    public String toString() {
        return "BulkWriteError{"
               + "index=" + index
               + ", code=" + getCode()
               + ", message='" + getMessage() + '\''
               + ", details=" + getDetails()
               + '}';
    }
}
