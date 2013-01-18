/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import org.bson.BSONObject;
import org.bson.types.Document;
import org.mongodb.CommandDocument;
import org.mongodb.FieldSelectorDocument;
import org.mongodb.QueryFilterDocument;
import org.mongodb.SortCriteriaDocument;
import org.mongodb.UpdateOperationsDocument;

import java.util.List;
import java.util.Map;

// TODO: Implement these methods
public class DBObjects {
    private DBObjects() {
    }

    public static Document toDocument(final DBObject obj) {
        Document res = new Document();
        fill(obj, res);
        return res;
    }

    public static Document[] toDocumentArray(final DBObject[] dbObjects) {
        Document[] res = new Document[dbObjects.length];
        for (int i = 0; i < dbObjects.length; i++) {
            res[i] = toDocument(dbObjects[i]);
        }
        return res;
    }

    public static QueryFilterDocument toQueryFilterDocument(final DBObject obj) {
        QueryFilterDocument doc = new QueryFilterDocument();
        fill(obj, doc);
        return doc;
    }

    public static FieldSelectorDocument toFieldSelectorDocument(final DBObject fields) {
        if (fields == null) {
            return null;
        }
        FieldSelectorDocument doc = new FieldSelectorDocument();
        fill(fields, doc);
        return doc;
    }

    public static UpdateOperationsDocument toUpdateOperationsDocument(final DBObject o) {
        if (o == null) {
            return null;
        }

        UpdateOperationsDocument doc = new UpdateOperationsDocument();
        fill(o, doc);
        return doc;
    }

    public static SortCriteriaDocument toSortCriteriaDocument(final DBObject o) {
        if (o == null) {
            return null;
        }

        SortCriteriaDocument doc = new SortCriteriaDocument();
        fill(o, doc);
        return doc;
    }

    public static CommandDocument toCommandDocument(final DBObject commandObject) {
        CommandDocument doc = new CommandDocument();
        fill(commandObject, doc);
        return doc;
    }


    public static CommandResult toCommandResult(DBObject command, ServerAddress serverAddress, final Document document) {
        CommandResult res = new CommandResult(command, serverAddress);
        fill(document, res);
        return res;
    }

    public static BasicDBObject toDBObject(final Document document) {
        BasicDBObject res = new BasicDBObject();
        fill(document, res);
        return res;
    }

    // TODO: This needs to be recursive, to translate nested DBObject and DBList and arrays...
    private static void fill(final DBObject obj, final Document document) {
        for (String key : obj.keySet()) {
            Object value = obj.get(key);
            if (value instanceof List) {
               document.put(key, value);
            }
            else if (value instanceof BSONObject) {
                Document nestedDocument = new Document();
                fill((DBObject) value, nestedDocument);
                document.put(key, nestedDocument);
            }
            else {
                document.put(key, obj.get(key));
            }
        }
    }

    private static void fill(final Document document, final DBObject obj) {
        for (Map.Entry<String, Object> cur : document.entrySet()) {
            if (cur.getValue() instanceof List) {
                obj.put(cur.getKey(), cur.getValue());
            }
            else if (cur.getValue() instanceof Document) {
                DBObject nestedObj = new BasicDBObject();
                fill((Document) cur.getValue(), nestedObj);
                obj.put(cur.getKey(), nestedObj);
            }
            else {
                obj.put(cur.getKey(), cur.getValue());
            }
        }
    }
}
