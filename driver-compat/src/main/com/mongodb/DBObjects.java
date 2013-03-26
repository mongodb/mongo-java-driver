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

package com.mongodb;

import org.bson.BSONObject;
import org.mongodb.Document;

import java.util.List;
import java.util.Map;

public class DBObjects {
    private DBObjects() {
    }

    public static Document toDocument(final DBObject obj) {
        final Document res = new Document();
        fill(obj, res);
        return res;
    }

    public static Document[] toDocumentArray(final DBObject[] dbObjects) {
        final Document[] res = new Document[dbObjects.length];
        for (int i = 0; i < dbObjects.length; i++) {
            res[i] = toDocument(dbObjects[i]);
        }
        return res;
    }

    public static Document toFieldSelectorDocument(final DBObject fields) {
        if (fields == null) {
            return null;
        }
        return toDocument(fields);
    }

    public static Document toUpdateOperationsDocument(final DBObject o) {
        if (o == null) {
            return null;
        }

        return toDocument(o);
    }

    public static CommandResult toCommandResult(final DBObject command, final ServerAddress serverAddress,
                                                final Document document) {
        final CommandResult res = new CommandResult(command, serverAddress);
        fill(document, res);
        return res;
    }

    public static BasicDBObject toDBObject(final Document document) {
        final BasicDBObject res = new BasicDBObject();
        fill(document, res);
        return res;
    }

    private static void fill(final DBObject obj, final Document document) {
        if (obj != null) {
            for (final String key : obj.keySet()) {
                final Object value = obj.get(key);
                if (value instanceof List) {
                    document.put(key, value);
                }
                else if (value instanceof BSONObject) {
                    final Document nestedDocument = new Document();
                    fill((DBObject) value, nestedDocument);
                    document.put(key, nestedDocument);
                }
                else {
                    document.put(key, obj.get(key));
                }
            }
        }
    }

    private static void fill(final Document document, final DBObject obj) {
        for (final Map.Entry<String, Object> cur : document.entrySet()) {
            if (cur.getValue() instanceof List) {
                obj.put(cur.getKey(), cur.getValue());
            }
            else if (cur.getValue() instanceof Document) {
                final DBObject nestedObj = new BasicDBObject();
                fill((Document) cur.getValue(), nestedObj);
                obj.put(cur.getKey(), nestedObj);
            }
            else {
                obj.put(cur.getKey(), cur.getValue());
            }
        }
    }
}
