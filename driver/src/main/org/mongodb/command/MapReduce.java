/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MapReduceOutput;

public class MapReduce extends Command {

    public MapReduce(final org.mongodb.operation.MapReduce mapReduce, final String collectionName) {
        super(asDocument(mapReduce, collectionName));
    }

    public static Document asDocument(final org.mongodb.operation.MapReduce mapReduce, final String collectionName) {

        return new Document("mapReduce", collectionName).append("map", mapReduce.getMapFunction())
                                                        .append("reduce", mapReduce.getReduceFunction())
                                                        .append("out", mapReduce.isInline() ? new Document("inline", 1)
                                                                                            : asDocument(mapReduce.getOutput()))
                                                        .append("query", mapReduce.getFilter())
                                                        .append("sort", mapReduce.getSortCriteria())
                                                        .append("limit", mapReduce.getLimit())
                                                        .append("finalize", mapReduce.getFinalizeFunction())
                                                        .append("scope", mapReduce.getScope())
                                                        .append("jsMode", mapReduce.isJsMode())
                                                        .append("verbose", mapReduce.isVerbose());
    }

    private static Document asDocument(final MapReduceOutput output) {
        Document document = new Document(output.getAction().getValue(), output.getCollectionName());
        if (output.getDatabaseName() != null) {
            document.append("db", output.getDatabaseName());
        }
        document.append("sharded", output.isSharded());
        document.append("nonAtomic", output.isNonAtomic());

        return document;
    }

}
