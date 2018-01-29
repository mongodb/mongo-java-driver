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

package com.mongodb.operation;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;

import java.util.List;

final class BsonDocumentWrapperHelper {

    @SuppressWarnings("unchecked")
    static <T> List<T> toList(final BsonDocument result, final String fieldContainingWrappedArray) {
        return ((BsonArrayWrapper<T>) result.getArray(fieldContainingWrappedArray)).getWrappedArray();
    }

    @SuppressWarnings("unchecked")
    static <T> T toDocument(final BsonDocument document) {
        if (document == null) {
            return null;
        }
        return ((BsonDocumentWrapper<T>) document).getWrappedDocument();
    }

    private BsonDocumentWrapperHelper() {
    }
}
