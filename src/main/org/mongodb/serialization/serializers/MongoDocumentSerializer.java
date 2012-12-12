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

package org.mongodb.serialization.serializers;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.MongoDocument;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.Map;

//TODO: handle array type
public class MongoDocumentSerializer implements Serializer<MongoDocument> {
    private final PrimitiveSerializers primitiveSerializers;

    public MongoDocumentSerializer(PrimitiveSerializers primitiveSerializers) {
        this.primitiveSerializers = primitiveSerializers;
    }

    // TODO: deal with options.  C# driver sends different options.  For one, to write _id field first
    @Override
    public void serialize(final BSONWriter bsonWriter, final MongoDocument document, final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            if (entry.getValue() instanceof MongoDocument) {
                serialize(bsonWriter, (MongoDocument) entry.getValue(), options);
            } else {
                primitiveSerializers.serialize(bsonWriter, entry.getValue(), options);
            }
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public MongoDocument deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        MongoDocument document = new MongoDocument();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            BsonType bsonType = reader.getNextBsonType();
            if (bsonType.equals(BsonType.DOCUMENT)) {
                document.put(fieldName, getDocumentDeserializerForField(fieldName).deserialize(reader, options));
            } else {
                Object value = primitiveSerializers.deserialize(reader, options);
                document.put(fieldName, value);
            }
        }

        reader.readEndDocument();

        return document;
    }

    @Override
    public Class<MongoDocument> getSerializationClass() {
        return MongoDocument.class;
    }

    protected PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    protected Serializer getDocumentDeserializerForField(final String fieldName) {
        return this;
    }

}
