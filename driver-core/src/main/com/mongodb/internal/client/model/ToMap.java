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
package com.mongodb.internal.client.model;

import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;

interface ToMap {
    Optional<Map<String, ?>> tryToMap();

    static Optional<Map<String, ?>> tryToMap(final Object o) {
        if (o instanceof ToMap) {
            return ((ToMap) o).tryToMap();
        } else if (o instanceof Document || o instanceof BsonDocument) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) o;
            return Optional.of(map);
        } else {
            return Optional.empty();
        }
    }
}
