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
package com.mongodb.client.result.bulk;

import com.mongodb.annotations.Evolving;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.model.bulk.ClientWriteModel;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * The result of a successful {@linkplain ClientWriteModel individual insert one operation}.
 * Note that {@link WriteConcernError}s are not considered as making individuals operations unsuccessful.
 *
 * @since 5.3
 */
@Evolving
public interface ClientInsertOneResult {
    /**
     * The {@code "_id"} of the inserted document.
     *
     * @return The {@code "_id"} of the inserted document, or {@code null} if one is not available,
     * which happens when a {@link RawBsonDocument} without {@code "_id"} is inserted,
     * because the driver does not generate missing {@code "_id"} fields for {@link RawBsonDocument}s.
     */
    @Nullable
    BsonValue getInsertedId();
}
