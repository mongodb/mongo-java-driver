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

package com.mongodb

import org.bson.BSONDecoder
import org.bson.BasicBSONDecoder
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import spock.lang.Specification

class DBEncoderDecoderDBRefSpecification extends Specification {

    def 'should encode and decode DBRefs'() {
        given:
        DBRef reference = new DBRef('coll', 'hello world')
        DBObject document = new BasicDBObject('!', reference)
        OutputBuffer buffer = new BasicOutputBuffer()

        when:
        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document)
        DefaultDBCallback callback = new DefaultDBCallback(null)
        BSONDecoder decoder = new BasicBSONDecoder()
        decoder.decode(buffer.toByteArray(), callback)
        DBRef decoded = ((DBObject) callback.get()).get('!')

        then:
        decoded.databaseName == null
        decoded.collectionName == 'coll'
        decoded.id == 'hello world'
    }

    def 'should encode and decode DBRefs with a database name'() {
        given:
        DBRef reference = new DBRef('db', 'coll', 'hello world')
        DBObject document = new BasicDBObject('!', reference)
        OutputBuffer buffer = new BasicOutputBuffer()

        when:
        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document)
        DefaultDBCallback callback = new DefaultDBCallback(null)
        BSONDecoder decoder = new BasicBSONDecoder()
        decoder.decode(buffer.toByteArray(), callback)
        DBRef decoded = ((DBObject) callback.get()).get('!')

        then:
        decoded.databaseName == 'db'
        decoded.collectionName == 'coll'
        decoded.id == 'hello world'
    }
}
