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

import org.bson.BSONBinaryReader;
import org.bson.BSONObject;
import org.bson.BSONReader;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.MongoExceptions.mapException;
import static java.nio.ByteBuffer.wrap;

final class DBObjects {
    public static Document toDocument(final DBObject obj) {
        Document res = new Document();
        fill(obj, res);
        return res;
    }

    public static Document toDocument(final DBObject obj, final DBEncoder encoder, final Decoder<Document> documentDecoder) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        encoder.writeObject(buffer, obj);
        BSONReader bsonReader = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(wrap(buffer.toByteArray()))), true);
        try {
            return documentDecoder.decode(bsonReader);
        } catch (final MongoException e) {
            throw mapException(e);
        } finally {
            bsonReader.close();
        }
    }

    public static Document toFieldSelectorDocument(final DBObject fields) {
        if (fields == null) {
            return null;
        }
        return toDocument(fields);
    }

    public static Document toNullableDocument(final DBObject obj) {
        if (obj == null) {
            return null;
        }
        return toDocument(obj);
    }

    public static Document toUpdateOperationsDocument(final DBObject o) {
        if (o == null) {
            return null;
        }

        return toDocument(o);
    }

    public static BasicDBObject toDBObject(final Document document) {
        BasicDBObject res = new BasicDBObject();
        fill(document, res);
        return res;
    }

    public static BasicDBObject toNullableDBObject(final Document document) {
        if (document == null) {
            return null;
        }
        final BasicDBObject res = new BasicDBObject();
        fill(document, res);
        return res;
    }

    private static void fill(final DBObject obj, final Document document) {
        if (obj != null) {
            for (final String key : obj.keySet()) {
                Object value = obj.get(key);
                convertType(obj, document, key, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void convertType(final DBObject obj, final Map<String, Object> document, final String key, final Object value) {
        if (value instanceof List) {
            document.put(key, value);
        } else if (value instanceof DBRef) {
            DBRef ref = (DBRef) value;
            document.put(key, ref.toNew());
        } else if (value instanceof BSONObject) {
            Document nestedDocument = new Document();
            fill((DBObject) value, nestedDocument);
            document.put(key, nestedDocument);
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map) obj.get(key);
            HashMap<String, Object> newMap = new HashMap<String, Object>();
            for (final Object mapKey : map.keySet()) {
                convertType(obj, newMap, mapKey.toString(), map.get(mapKey));
            }
            document.put(key, newMap);
        } else {
            document.put(key, value);
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

    public static BasicDBList toDBList(final MongoCursor<?> source) {
        BasicDBList dbList = new BasicDBList();
        while (source.hasNext()) {
            Object o = source.next();
            if (o instanceof Document) {
                dbList.add(toDBObject((Document) o));
            } else {
                dbList.add(o);
            }
        }
        return dbList;
    }

    private DBObjects() { }
}
