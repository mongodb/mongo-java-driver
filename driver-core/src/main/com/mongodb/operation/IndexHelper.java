/*
 * Copyright 2015 MongoDB, Inc.
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
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

final class IndexHelper {

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @return a string representation of this index's fields
     */
    static String generateIndexName(final BsonDocument index) {
        StringBuilder indexName = new StringBuilder();
        for (final String keyNames : index.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            BsonValue ascOrDescValue = index.get(keyNames);
            if (ascOrDescValue instanceof BsonNumber) {
                indexName.append(((BsonNumber) ascOrDescValue).intValue());
            } else if (ascOrDescValue instanceof BsonString) {
                indexName.append(((BsonString) ascOrDescValue).getValue().replace(' ', '_'));
            }
        }
        return indexName.toString();
    }

    private IndexHelper() {
    }
}
