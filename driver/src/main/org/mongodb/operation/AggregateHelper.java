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

package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class AggregateHelper {

    static Document asCommandDocument(final MongoNamespace namespace, final List<Document> pipeline, final AggregationOptions options) {
        Document commandDocument = new Document("aggregate", namespace.getCollectionName());
        commandDocument.put("pipeline", pipeline);
        if (options.getMaxTime(MILLISECONDS) > 0) {
            commandDocument.put("maxTimeMS", options.getMaxTime(MILLISECONDS));
        }
        if (options.getOutputMode() == AggregationOptions.OutputMode.CURSOR) {
            Document cursor = new Document();
            if (options.getBatchSize() != null) {
                cursor.put("batchSize", options.getBatchSize());
            }
            commandDocument.put("cursor", cursor);
        }
        if (options.getAllowDiskUse() != null) {
            commandDocument.put("allowDiskUse", options.getAllowDiskUse());
        }
        return commandDocument;
    }

    private AggregateHelper() {
    }
}
