/*
 * Copyright (c) 2008 - 2013 MongoDB Inc. <http://10gen.com>
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

import org.bson.BSONReader
import org.bson.BSONWriter
import org.mongodb.codecs.EncodingException
import spock.lang.Specification
import spock.lang.Subject

class DBObjectCodecSpecification extends Specification {

    @Subject
    DBObjectCodec dbObjectCodec = new DBObjectCodec();

    def 'should convert EncodingException for a missing codec into old com.mongodb.Exceptions for writeValue'() {
        BSONWriter bsonWriter = Mock();

        when:
        dbObjectCodec.writeValue(bsonWriter, new Object())

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should convert EncodingException for a missing codec into old com.mongodb.MongoException when reading a value'() {
        BSONReader bsonReader = Mock();
        bsonReader._() >> { throw new EncodingException('New layer Exception that should not leak') }

        when:
        dbObjectCodec.readValue(bsonReader, 'fieldName', [])

        then:
        thrown(com.mongodb.MongoException)
    }
}
