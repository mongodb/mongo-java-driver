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
import org.mongodb.operation.Find;

public class MapReduceCommand extends Command {
    public MapReduceCommand(final Find find, final String collectionName, final String map, final String reduce) {
        super(asDocument(find, collectionName, map, reduce));
    }

    private static Document asDocument(final Find find, final String collectionName, final String map, final String reduce) {
        Document cmd = new Document();

        cmd.put("mapreduce", collectionName);
        cmd.put("map", map);
        cmd.put("reduce", reduce);

        cmd.put("out", new Document("inline", 1));

        if (find.getFilter() != null) {
            cmd.put("query", find.getFilter());
        }

        if (find.getOrder() != null) {
            cmd.put("sort", find.getOrder());
        }

        if (find.getLimit() > 0) {
            cmd.put("limit", find.getLimit());
        }

        return cmd;
    }
}
