/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class BsonHelper {
    private static final List<BsonValue> BSON_VALUES = asList(
            new BsonNull(),
            new BsonInt32(42),
            new BsonInt64(52L),
            new BsonDecimal128(Decimal128.parse("4.00")),
            new BsonBoolean(true),
            new BsonDateTime(new Date().getTime()),
            new BsonDouble(62.0),
            new BsonString("the fox ..."),
            new BsonMinKey(),
            new BsonMaxKey(),
            new BsonDbPointer("test.test", new ObjectId()),
            new BsonJavaScript("int i = 0;"),
            new BsonJavaScriptWithScope("x", new BsonDocument("x", new BsonInt32(1))),
            new BsonObjectId(new ObjectId()),
            new BsonRegularExpression("^test.*regex.*xyz$", "i"),
            new BsonSymbol("ruby stuff"),
            new BsonTimestamp(0x12345678, 5),
            new BsonUndefined(),
            new BsonBinary((byte) 80, new byte[]{5, 4, 3, 2, 1}),
            new BsonArray(asList(new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                    new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3), new BsonDocument("a", new BsonInt64(2L)))))),
            new BsonDocument("a", new BsonInt32(1)));

    private static final BsonDocument BSON_DOCUMENT = new BsonDocument();

    static {
        for (int i = 0; i < BSON_VALUES.size(); i++) {
            BSON_DOCUMENT.append(Integer.toString(i), BSON_VALUES.get(i));
        }
    }

    // fail class loading if any BSON types are not represented in BSON_VALUES.
    static {
        for (BsonType curBsonType : BsonType.values()) {
            if (curBsonType == BsonType.END_OF_DOCUMENT) {
                continue;
            }

            boolean found = false;
            for (BsonValue curBsonValue : BSON_VALUES) {
                if (curBsonValue.getBsonType() == curBsonType) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalStateException(format("Missing BsonValue type %s in BSON_VALUES. Please add a BsonValue with that type!",
                        curBsonType));
            }
        }
    }

    public static List<BsonValue> valuesOfEveryType() {
        return BSON_VALUES;
    }

    public static BsonDocument documentWithValuesOfEveryType() {
        return BSON_DOCUMENT;
    }

    public static ByteBuffer toBson(final BsonDocument document) {
        BasicOutputBuffer bsonOutput = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(bsonOutput), document, EncoderContext.builder().build());
        return ByteBuffer.wrap(bsonOutput.toByteArray());
    }

    private BsonHelper() {
    }
}
