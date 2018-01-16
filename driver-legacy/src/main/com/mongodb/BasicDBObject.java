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

package com.mongodb;

import org.bson.BSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;
import java.util.Map;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;

/**
 * A basic implementation of BSON object that is MongoDB specific. A {@code DBObject} can be created as follows, using this class:
 * <pre>
 * DBObject obj = new BasicDBObject();
 * obj.put( "foo", "bar" );
 * </pre>
 *
 * @mongodb.driver.manual core/document/ MongoDB Documents
 */
@SuppressWarnings({"rawtypes"})
public class BasicDBObject extends BasicBSONObject implements DBObject, Bson {
    private static final long serialVersionUID = -4415279469780082174L;

    private boolean isPartialObject;

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code BasicDBObject}.
     *
     * @param json the JSON string
     * @return a corresponding {@code BasicDBObject} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static BasicDBObject parse(final String json) {
        return parse(json, getDefaultCodecRegistry().get(BasicDBObject.class));
    }

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code BasicDBObject}.
     *
     * @param json the JSON string
     * @param decoder the decoder to use to decode the BasicDBObject instance
     * @return a corresponding {@code BasicDBObject} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static BasicDBObject parse(final String json, final Decoder<BasicDBObject> decoder) {
        return decoder.decode(new JsonReader(json), DecoderContext.builder().build());
    }

    /**
     * Creates an empty object.
     */
    public BasicDBObject() {
        super();
    }

    /**
     * Creates an empty object
     *
     * @param size an estimate of number of fields that will be inserted
     */
    public BasicDBObject(final int size) {
        super(size);
    }

    /**
     * Creates an object with the given key/value
     *
     * @param key   key under which to store
     * @param value value to store
     */
    public BasicDBObject(final String key, final Object value) {
        super(key, value);
    }

    /**
     * Creates an object from a map.
     *
     * @param map map to convert
     */
    public BasicDBObject(final Map map) {
        super(map);
    }

    /**
     * Add a key/value pair to this object
     *
     * @param key the field name
     * @param val the field value
     * @return this BasicDBObject with the new values added
     */
    @Override
    public BasicDBObject append(final String key, final Object val) {
        put(key, val);
        return this;
    }

    @Override
    protected BSONEncoder getEncoder() {
        return new DefaultDBEncoder();
    }

    /**
     * Whether {@link #markAsPartialObject} was ever called only matters if you are going to upsert and do not want to risk losing fields.
     *
     * @return true if this has been marked as a partial object
     */
    @Override
    public boolean isPartialObject() {
        return isPartialObject;
    }

    /**
     * Gets a JSON representation of this document using the {@link org.bson.json.JsonMode#STRICT} output mode, and otherwise the default
     * settings of {@link JsonWriterSettings.Builder} and {@link DBObjectCodec}.
     *
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     * @see #toJson(JsonWriterSettings)
     * @see JsonWriterSettings
     */
    @SuppressWarnings("deprecation")
    public String toJson() {
        return toJson(new JsonWriterSettings());
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link DBObjectCodec}.</p>
     *
     * @param writerSettings the json writer settings to use when encoding
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     */
    public String toJson(final JsonWriterSettings writerSettings) {
        return toJson(writerSettings, getDefaultCodecRegistry().get(BasicDBObject.class));
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link JsonWriterSettings}.</p>
     *
     * @param encoder the BasicDBObject codec instance to encode the document with
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    @SuppressWarnings("deprecation")
    public String toJson(final Encoder<BasicDBObject> encoder) {
        return toJson(new JsonWriterSettings(), encoder);
    }

    /**
     * Gets a JSON representation of this document
     *
     * @param writerSettings the json writer settings to use when encoding
     * @param encoder the BasicDBObject codec instance to encode the document with
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    public String toJson(final JsonWriterSettings writerSettings, final Encoder<BasicDBObject> encoder) {
        JsonWriter writer = new JsonWriter(new StringWriter(), writerSettings);
        encoder.encode(writer, this, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }

    /**
     * <p>Returns a JSON serialization of this object</p>
     *
     * <p>The output will look like: {@code  {"a":1, "b":["x","y","z"]} }</p>
     *
     * @return JSON serialization
     */
    @SuppressWarnings("deprecation")
    public String toString() {
        return com.mongodb.util.JSON.serialize(this);
    }

    /**
     * If this object was retrieved with only some fields (using a field filter) this method will be called to mark it as such.
     */
    @Override
    public void markAsPartialObject() {
        isPartialObject = true;
    }

    /**
     * Creates a new instance which is a copy of this BasicDBObject.
     *
     * @return a BasicDBObject with exactly the same values as this instance.
     */
    public Object copy() {
        // copy field values into new object
        BasicDBObject newCopy = new BasicDBObject(this.toMap());
        // need to clone the sub obj
        for (final String field : keySet()) {
            Object val = get(field);
            if (val instanceof BasicDBObject) {
                newCopy.put(field, ((BasicDBObject) val).copy());
            } else if (val instanceof BasicDBList) {
                newCopy.put(field, ((BasicDBList) val).copy());
            }
        }
        return newCopy;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<BasicDBObject>(this, codecRegistry.get(BasicDBObject.class));
    }
}
