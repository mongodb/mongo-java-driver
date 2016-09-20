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

// BasicDBList.java

package com.mongodb;

import com.mongodb.util.JSON;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;

import java.io.StringWriter;

import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.bson.types.BasicBSONList;

/**
 * An implementation of List that reflects the way BSON lists work.
 */
public class BasicDBList extends BasicBSONList implements DBObject {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code BasicDBList}.
     *
     * @param json the JSON string
     * @return a corresponding {@code BasicDBList} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static BasicDBList parse(final String json) {
        return parse(json, getDefaultCodecRegistry().get(BasicDBList.class));
    }

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code BasicDBList}.
     *
     * @param json the JSON string
     * @param decoder the decoder to use to decode the BasicDBList instance
     * @return a corresponding {@code BasicDBList} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static BasicDBList parse(final String json, final Decoder<BasicDBList> decoder) {
        return decoder.decode(new JsonReader(json), DecoderContext.builder().build());
    }

    /**
     * Gets a JSON representation of this object
     *
     * <p>With the default {@link JsonWriterSettings} and {@link DBObjectCodec}.</p>
     *
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     */
    public String toJson() {
        return toJson(new JsonWriterSettings());
    }

    /**
     * Gets a JSON representation of this object
     *
     * <p>With the default {@link DBObjectCodec}.</p>
     *
     * @param writerSettings the json writer settings to use when encoding
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     */
    public String toJson(final JsonWriterSettings writerSettings) {
        return toJson(writerSettings, getDefaultCodecRegistry().get(BasicDBList.class));
    }

    /**
     * Gets a JSON representation of this object
     *
     * <p>With the default {@link JsonWriterSettings}.</p>
     *
     * @param encoder the BasicDBList codec instance to encode the document with
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    public String toJson(final Encoder<BasicDBList> encoder) {
        return toJson(new JsonWriterSettings(), encoder);
    }

    /**
     * Gets a JSON representation of this object
     *
     * @param writerSettings the json writer settings to use when encoding
     * @param encoder the BasicDBList codec instance to encode the document with
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    public String toJson(final JsonWriterSettings writerSettings, final Encoder<BasicDBList> encoder) {
        JsonWriter writer = new JsonWriter(new StringWriter(), writerSettings);
        encoder.encode(writer, this, EncoderContext.builder().isEncodingCollectibleDocument(false).build());
        return writer.getWriter().toString();
    }

    /**
     * Returns a JSON serialization of this object
     *
     * @return JSON serialization
     */
    @Override
    public String toString() {
        return JSON.serialize(this);
    }

    @Override
    public boolean isPartialObject() {
        return _isPartialObject;
    }

    @Override
    public void markAsPartialObject() {
        _isPartialObject = true;
    }

    /**
     * Copies this instance into a new Object.
     *
     * @return a new BasicDBList with the same values as this instance
     */
    public Object copy() {
        // copy field values into new object
        BasicDBList newobj = new BasicDBList();
        // need to clone the sub obj
        for (int i = 0; i < size(); ++i) {
            Object val = get(i);
            if (val instanceof BasicDBObject) {
                val = ((BasicDBObject) val).copy();
            } else if (val instanceof BasicDBList) {
                val = ((BasicDBList) val).copy();
            }
            newobj.add(val);
        }
        return newobj;
    }

    private boolean _isPartialObject;
}
