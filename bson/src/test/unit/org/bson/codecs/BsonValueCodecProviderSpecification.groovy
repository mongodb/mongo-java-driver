/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs

import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDbPointer
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonJavaScriptWithScope
import org.bson.BsonMaxKey
import org.bson.BsonMinKey
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonUndefined
import org.bson.RawBsonDocument
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class BsonValueCodecProviderSpecification extends Specification {

    def provider = new BsonValueCodecProvider()
    def codecRegistry = fromProviders(provider)

    def 'should get correct codec'() {
        expect:
        provider.get(String, codecRegistry) == null

        provider.get(BsonInt32, codecRegistry).class == BsonInt32Codec
        provider.get(BsonInt64, codecRegistry).class == BsonInt64Codec
        provider.get(BsonDouble, codecRegistry).class == BsonDoubleCodec
        provider.get(BsonString, codecRegistry).class == BsonStringCodec
        provider.get(BsonBoolean, codecRegistry).class == BsonBooleanCodec
        provider.get(BsonDecimal128, codecRegistry).class == BsonDecimal128Codec

        provider.get(BsonNull, codecRegistry).class == BsonNullCodec
        provider.get(BsonDateTime, codecRegistry).class == BsonDateTimeCodec
        provider.get(BsonMinKey, codecRegistry).class == BsonMinKeyCodec
        provider.get(BsonMaxKey, codecRegistry).class == BsonMaxKeyCodec
        provider.get(BsonJavaScript, codecRegistry).class == BsonJavaScriptCodec
        provider.get(BsonObjectId, codecRegistry).class == BsonObjectIdCodec
        provider.get(BsonRegularExpression, codecRegistry).class == BsonRegularExpressionCodec
        provider.get(BsonSymbol, codecRegistry).class == BsonSymbolCodec
        provider.get(BsonTimestamp, codecRegistry).class == BsonTimestampCodec
        provider.get(BsonUndefined, codecRegistry).class == BsonUndefinedCodec
        provider.get(BsonDbPointer, codecRegistry).class == BsonDBPointerCodec

        provider.get(BsonJavaScriptWithScope, codecRegistry).class == BsonJavaScriptWithScopeCodec

        provider.get(BsonArray, codecRegistry).class == BsonArrayCodec

        provider.get(BsonDocument, codecRegistry).class == BsonDocumentCodec
        provider.get(BsonDocumentWrapper, codecRegistry).class == BsonDocumentWrapperCodec
        provider.get(RawBsonDocument, codecRegistry).class == RawBsonDocumentCodec
        provider.get(BsonDocumentSubclass, codecRegistry).class == BsonDocumentCodec
    }
}
