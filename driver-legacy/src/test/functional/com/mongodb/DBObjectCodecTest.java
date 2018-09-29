/*
 * Copyright 2008-present MongoDB, Inc.
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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.LazyBSONCallback;
import org.bson.Transformer;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class DBObjectCodecTest extends DatabaseTestCase {

    @Test
    public void testTransformers() {
        try {
            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(Double.class, collection.findOne().get("x").getClass());

            org.bson.BSON.addEncodingHook(Double.class, new Transformer() {
                public Object transform(final Object o) {
                    return o.toString();
                }
            });

            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(String.class, collection.findOne().get("x").getClass());

            org.bson.BSON.clearAllHooks();
            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(Double.class, collection.findOne().get("x").getClass());

            org.bson.BSON.addDecodingHook(Double.class, new Transformer() {
                public Object transform(final Object o) {
                    return o.toString();
                }
            });
            assertEquals(String.class, collection.findOne().get("x").getClass());
            org.bson.BSON.clearAllHooks();
            assertEquals(Double.class, collection.findOne().get("x").getClass());
        } finally {
            org.bson.BSON.clearAllHooks();
        }
    }

    @Test
    public void testDBListEncoding() {
        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("a", 1).append("b", true));
        list.add(new BasicDBObject("c", "string").append("d", 0.1));
        collection.save(new BasicDBObject("l", list));
        assertEquals(list, collection.findOne().get("l"));
    }

    @Test
    public void shouldNotGenerateIdIfPresent() {
        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));
        DBObject document = new BasicDBObject("_id", 1);
        assertTrue(dbObjectCodec.documentHasId(document));
        document = dbObjectCodec.generateIdIfAbsentFromDocument(document);
        assertTrue(dbObjectCodec.documentHasId(document));
        assertEquals(new BsonInt32(1), dbObjectCodec.getDocumentId(document));
    }

    @Test
    public void shouldGenerateIdIfAbsent() {
        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));
        DBObject document = new BasicDBObject();
        assertFalse(dbObjectCodec.documentHasId(document));
        document = dbObjectCodec.generateIdIfAbsentFromDocument(document);
        assertTrue(dbObjectCodec.documentHasId(document));
        assertEquals(BsonObjectId.class, dbObjectCodec.getDocumentId(document).getClass());
    }

    @Test
    public void shouldRespectEncodeIdFirstPropertyInEncoderContext() {
        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));
        // given
        DBObject doc = new BasicDBObject("x", 2).append("_id", 2);

        // when
        BsonDocument encodedDocument = new BsonDocument();
        dbObjectCodec.encode(new BsonDocumentWriter(encodedDocument),
                             doc,
                             EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        // then
        assertEquals(new ArrayList<String>(encodedDocument.keySet()), asList("_id", "x"));

        // when
        encodedDocument.clear();
        dbObjectCodec.encode(new BsonDocumentWriter(encodedDocument),
                             doc,
                             EncoderContext.builder().isEncodingCollectibleDocument(false).build());

        // then
        assertEquals(new ArrayList<String>(encodedDocument.keySet()), asList("x", "_id"));
    }

    @Test
    public void shouldEncodeNull() {
        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));

        DBObject doc = new BasicDBObject("null", null);

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        dbObjectCodec.encode(writer, doc, EncoderContext.builder().build());

        assertEquals(new BsonDocument("null", BsonNull.VALUE), writer.getDocument());
    }

    @Test
    public void shouldEncodedNestedMapsListsAndDocuments() {
        byte[] zeroOneDocumentBytes = new byte[]{19, 0, 0, 0, 16, 48, 0, 0, 0, 0, 0, 16, 49, 0, 1, 0, 0, 0, 0}; //  {"0" : 0, "1", 1}
        Map<String, Object> zeroOneMap = new HashMap<String, Object>();
        zeroOneMap.put("0", 0);
        zeroOneMap.put("1", 1);
        DBObject zeroOneDBObject = new BasicDBObject();
        zeroOneDBObject.putAll(zeroOneMap);
        DBObject zeroOneDBList = new BasicDBList();
        zeroOneDBList.putAll(zeroOneMap);
        List<Integer> zeroOneList = asList(0, 1);

        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));

        DBObject doc = new BasicDBObject()
                       .append("map", zeroOneMap)
                       .append("dbDocument", zeroOneDBObject)
                       .append("dbList", zeroOneDBList)
                       .append("list", zeroOneList)
                       .append("array", new int[] {0, 1})
                       .append("lazyDoc", new LazyDBObject(zeroOneDocumentBytes, new LazyBSONCallback()))
                       .append("lazyArray", new LazyDBList(zeroOneDocumentBytes, new LazyBSONCallback()));

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        dbObjectCodec.encode(writer, doc, EncoderContext.builder().build());

        BsonDocument zeroOneBsonDocument = new BsonDocument().append("0", new BsonInt32(0)).append("1", new BsonInt32(1));
        BsonArray zeroOneBsonArray = new BsonArray(asList(new BsonInt32(0), new BsonInt32(1)));

        assertEquals(new BsonDocument("map", zeroOneBsonDocument)
                     .append("dbDocument", zeroOneBsonDocument)
                     .append("dbList", zeroOneBsonArray)
                     .append("list", zeroOneBsonArray)
                     .append("array", zeroOneBsonArray)
                     .append("lazyDoc", zeroOneBsonDocument)
                     .append("lazyArray", zeroOneBsonArray), writer.getDocument());
    }

    @Test
    public void shouldEncodeIterableMapAsMap() {
        IterableMap iterableMap = new IterableMap();
        iterableMap.put("first", 1);

        DBObjectCodec dbObjectCodec = new DBObjectCodec(fromProviders(asList(new ValueCodecProvider(), new DBObjectCodecProvider(),
                new BsonValueCodecProvider())));

        DBObject doc = new BasicDBObject("map", iterableMap);

        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
        dbObjectCodec.encode(writer, doc, EncoderContext.builder().build());

        assertEquals(new BsonDocument("map", new BsonDocument("first", new BsonInt32(1))), writer.getDocument());
    }

    static class IterableMap extends HashMap<String, Integer> implements Iterable<Integer> {
        private static final long serialVersionUID = -5090421898469363392L;

        @Override
        public Iterator<Integer> iterator() {
            return values().iterator();
        }
    }
}
