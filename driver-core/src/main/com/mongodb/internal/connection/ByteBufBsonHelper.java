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

package com.mongodb.internal.connection;

import org.bson.BsonBinaryReader;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;

import java.io.Closeable;
import java.util.List;

final class ByteBufBsonHelper {
    static BsonValue readBsonValue(final ByteBuf byteBuf, final BsonBinaryReader bsonReader, final List<Closeable> trackedResources) {
        BsonValue value;
        switch (bsonReader.getCurrentBsonType()) {
            case DOCUMENT:
                ByteBuf documentByteBuf = byteBuf.duplicate();
                ByteBufBsonDocument document = new ByteBufBsonDocument(documentByteBuf);
                trackedResources.add(document);
                value = document;
                bsonReader.skipValue();
                break;
            case ARRAY:
                ByteBuf arrayByteBuf = byteBuf.duplicate();
                ByteBufBsonArray array = new ByteBufBsonArray(arrayByteBuf);
                trackedResources.add(array);
                value = array;
                bsonReader.skipValue();
                break;
            case INT32:
                value = new BsonInt32(bsonReader.readInt32());
                break;
            case INT64:
                value = new BsonInt64(bsonReader.readInt64());
                break;
            case DOUBLE:
                value = new BsonDouble(bsonReader.readDouble());
                break;
            case DECIMAL128:
                value = new BsonDecimal128(bsonReader.readDecimal128());
                break;
            case DATE_TIME:
                value = new BsonDateTime(bsonReader.readDateTime());
                break;
            case TIMESTAMP:
                value = bsonReader.readTimestamp();
                break;
            case BOOLEAN:
                value = new BsonBoolean(bsonReader.readBoolean());
                break;
            case OBJECT_ID:
                value = new BsonObjectId(bsonReader.readObjectId());
                break;
            case STRING:
                value = new BsonString(bsonReader.readString());
                break;
            case BINARY:
                value = bsonReader.readBinaryData();
                break;
            case SYMBOL:
                value = new BsonSymbol(bsonReader.readSymbol());
                break;
            case UNDEFINED:
                bsonReader.readUndefined();
                value = new BsonUndefined();
                break;
            case REGULAR_EXPRESSION:
                value = bsonReader.readRegularExpression();
                break;
            case DB_POINTER:
                value = bsonReader.readDBPointer();
                break;
            case JAVASCRIPT:
                value = new BsonJavaScript(bsonReader.readJavaScript());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                String code = bsonReader.readJavaScriptWithScope();
                BsonDocument scope = new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
                value = new BsonJavaScriptWithScope(code, scope);
                break;
            case MIN_KEY:
                bsonReader.readMinKey();
                value = new BsonMinKey();
                break;
            case MAX_KEY:
                bsonReader.readMaxKey();
                value = new BsonMaxKey();
                break;
            case NULL:
                bsonReader.readNull();
                value = new BsonNull();
                break;
            default:
                throw new UnsupportedOperationException("Unexpected BSON type: " + bsonReader.getCurrentBsonType());
        }
        return value;
    }

    private ByteBufBsonHelper() {
    }
}
