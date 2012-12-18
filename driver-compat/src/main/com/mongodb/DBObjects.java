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

import org.bson.types.Document;
import org.mongodb.FieldSelectorDocument;
import org.mongodb.QueryFilterDocument;
import org.mongodb.UpdateOperationsDocument;
import org.mongodb.operation.MongoFieldSelector;

// TODO: Implement these methods
public class DBObjects {
    private DBObjects() {
    }

    public static QueryFilterDocument toQueryFilterDocument(final DBObject obj) {
        QueryFilterDocument doc = new QueryFilterDocument();
        fill(obj, doc);
        return doc;
    }

    public static MongoFieldSelector toFieldSelectorDocument(final DBObject fields) {
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

    // TODO: This needs to be recursive, to translate nested DBObject and DBList and arrays...
    private static void fill(final DBObject obj, final Document document) {
        for (String key : obj.keySet()) {
            Object value = obj.get(key);
            if (value instanceof DBObject) {
                Document nestedDocument = new Document();
                fill((DBObject) value, nestedDocument);
                document.put(key, nestedDocument);
            }
            else {
                document.put(key, obj.get(key));
            }
        }
    }

}
