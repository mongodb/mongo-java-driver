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

package com.mongodb.operation;

import com.mongodb.Function;
import org.bson.BsonDocument;

final class FindAndModifyHelper {

    static <T> Function<BsonDocument, T> transformer() {
        return new Function<BsonDocument, T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T apply(final BsonDocument result) {
                if (!result.isDocument("value")) {
                    return null;
                }
                return BsonDocumentWrapperHelper.toDocument(result.getDocument("value", null));
            }
        };
    }

    private FindAndModifyHelper() {
    }
}
