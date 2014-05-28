/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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
import org.bson.BsonDocumentReader;
import org.bson.codecs.configuration.CodecSource;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.BsonDocument;
import org.bson.types.RegularExpression;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.codecs.PatternCodec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class DBObjects {
    private static final DBObjectCodec codec =
    new DBObjectCodec(null, new BasicDBObjectFactory(), new RootCodecRegistry(Arrays.<CodecSource>asList(new DBObjectCodecSource())),
                      DBObjectCodecSource.createDefaultBsonTypeClassMap());

    public static Document toDocument(final DBObject obj) {
        Document res = new Document();
        fill(obj, res);
        return res;
    }

    public static DBObject toDBObjectAllowNull(final BsonDocument document) {
        if (document == null) {
            return null;
        }
        return toDBObject(document);
    }

    public static DBObject toDBObject(final BsonDocument document) {
        return codec.decode(new BsonDocumentReader(document));
    }

    public static BasicDBObject toDBObject(final Document document) {
        BasicDBObject res = new BasicDBObject();
        fill(document, res);
        return res;
    }

    private static void fill(final DBObject obj, final Document document) {
        if (obj != null) {
            Container container = new Container() {
                public void addToCollection(final String key, final Object value) {
                    document.put(key, value);
                }
            };
            for (final String key : obj.keySet()) {
                Object value = obj.get(key);
                convertType(container, key, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void convertType(final Container document, final String key, final Object value) {
        if (value instanceof List) {
            final List<Object> newList = new ArrayList<Object>();
            Container listContainer = new Container() {
                public void addToCollection(final String key, final Object value) {
                    newList.add(value);
                }
            };
            for (final Object item : (List) value) {
                convertType(listContainer, null, item);
            }
            document.addToCollection(key, newList);
        } else if (value instanceof DBRef) {
            DBRef ref = (DBRef) value;
            document.addToCollection(key, new Document("$ref", ref.getRef()).append("$id", ref.getId()));
        } else if (value instanceof Pattern) {
            Pattern pattern = (Pattern) value;
            document.addToCollection(key, new RegularExpression(pattern.pattern(), PatternCodec.getOptionsAsString(pattern)));
        } else if (value instanceof BSONObject) {
            Document nestedDocument = new Document();
            fill((DBObject) value, nestedDocument);
            document.addToCollection(key, nestedDocument);
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map) value;
            final HashMap<String, Object> newMap = new HashMap<String, Object>();
            Container mapContainer = new Container() {
                public void addToCollection(final String key, final Object value) {
                    newMap.put(key, value);
                }
            };
            for (final Object mapKey : map.keySet()) {
                convertType(mapContainer, mapKey.toString(), map.get(mapKey));
            }
            document.addToCollection(key, newMap);
        } else {
            document.addToCollection(key, value);
        }
    }

    private static void fill(final Document document, final DBObject obj) {
        for (final Map.Entry<String, Object> cur : document.entrySet()) {
            if (cur.getValue() instanceof List<?>) {
                obj.put(cur.getKey(), toDBList((List) cur.getValue()));
            } else if (cur.getValue() instanceof Document) {
                DBObject nestedObj = new BasicDBObject();
                fill((Document) cur.getValue(), nestedObj);
                obj.put(cur.getKey(), nestedObj);
            } else {
                obj.put(cur.getKey(), cur.getValue());
            }
        }
    }

    public static BasicDBList toDBList(final List<?> source) {
        BasicDBList dbList = new BasicDBList();
        for (final Object o : source) {
            if (o instanceof Document) {
                dbList.add(toDBObject((Document) o));
            } else {
                dbList.add(o);
            }
        }
        return dbList;
    }

    public static BasicDBList toDBList(final MongoCursor<DBObject> source) {
        BasicDBList dbList = new BasicDBList();
        while (source.hasNext()) {
            dbList.add(source.next());
        }
        return dbList;
    }

    private DBObjects() {
    }

    private interface Container {
        void addToCollection(final String key, final Object value);
    }
}
