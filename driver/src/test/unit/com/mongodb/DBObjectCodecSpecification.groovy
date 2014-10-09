/*
 *
 *  * Copyright (c) 2008-2014 MongoDB, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.mongodb

import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 */
class DBObjectCodecSpecification extends Specification {

    @Shared BsonDocument bsonDoc = new BsonDocument()

    def 'should encode and decode UUIDs'() {
        given:
        UUID uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')
        DBObjectCodec dbObjectCodec = new DBObjectCodec(null, new BasicDBObjectFactory(),
                new RootCodecRegistry(Arrays.<CodecProvider>asList(new DBObjectCodecProvider())),
                DBObjectCodecProvider.createDefaultBsonTypeClassMap());
        BasicDBObject uuidObj = new BasicDBObject('uuid', uuid)
        BsonDocumentWriter writer = new BsonDocumentWriter(bsonDoc)
        dbObjectCodec.encode(writer, uuidObj, EncoderContext.builder().build())
        BsonDocumentReader reader = new BsonDocumentReader(bsonDoc)
        DBObject decodedUuid = dbObjectCodec.decode(reader, DecoderContext.builder().build())

        expect:
        decodedUuid.get('uuid') == uuid
    }
}