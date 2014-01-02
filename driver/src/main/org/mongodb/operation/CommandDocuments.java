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

import org.mongodb.Document;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.operation.DocumentHelper.putIfNotNull;
import static org.mongodb.operation.DocumentHelper.putIfNotZero;
import static org.mongodb.operation.DocumentHelper.putIfTrue;

final class CommandDocuments {
    private CommandDocuments() {
    }

    static Document createMapReduce(final String collectionName, final MapReduce mapReduce) {

        Document commandDocument = new Document("mapreduce", collectionName)
                                   .append("map", mapReduce.getMapFunction())
                                   .append("reduce", mapReduce.getReduceFunction())
                                   .append("out", mapReduce.isInline() ? new Document("inline", 1)
                                                                       : outputAsDocument(mapReduce.getOutput()))
                                   .append("query", mapReduce.getFilter())
                                   .append("sort", mapReduce.getSortCriteria())
                                   .append("finalize", mapReduce.getFinalizeFunction())
                                   .append("scope", mapReduce.getScope())
                                   .append("verbose", mapReduce.isVerbose());
        putIfNotZero(commandDocument, "limit", mapReduce.getLimit());
        putIfNotZero(commandDocument, "maxTimeMS", mapReduce.getMaxTime(MILLISECONDS));
        putIfTrue(commandDocument, "jsMode", mapReduce.isJsMode());
        return commandDocument;
    }

    private static Document outputAsDocument(final MapReduceOutputOptions output) {
        Document document = new Document(output.getAction().getValue(), output.getCollectionName());
        document.append("sharded", output.isSharded());
        document.append("nonAtomic", output.isNonAtomic());
        putIfNotNull(document, "db", output.getDatabaseName());
        return document;
    }
}
